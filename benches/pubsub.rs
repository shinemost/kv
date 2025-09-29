use anyhow::Result;
use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use kv::{
    ClientConfig, CommandRequest, ServerConfig, StorageConfig, YamuxCtrl, start_client_with_config,
    start_server_with_config,
};
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::trace::BatchConfig;
use opentelemetry::sdk::{Resource, trace};
use opentelemetry::trace::Tracer;
use opentelemetry::{KeyValue, global, runtime};
use opentelemetry_otlp::WithExportConfig;
use rand::prelude::SliceRandom;
use std::sync::OnceLock;
use std::time::Duration;
use std::vec;
use tokio::net::TcpStream;
use tokio::runtime::{Builder, Runtime};
use tokio::time;
use tokio_rustls::client::TlsStream;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// 将硬编码值提取为常量或配置
const SERVER_ADDR: &str = "127.0.0.1:9999";
const OTLP_ENDPOINT: &str = "http://localhost:4317";
const SUBSCRIBER_COUNT: usize = 100;
const TOPIC: &str = "lobby";
const MESSAGE_VALUES: &[&str] = &["Hello", "Tyr", "Goodbye", "World"];

async fn start_server() -> Result<()> {
    let mut config: ServerConfig = toml::from_str(include_str!("../fixtures/server.conf"))?;
    config.general.addr = SERVER_ADDR.into();
    config.storage = StorageConfig::MemTable;

    tokio::spawn(async move {
        start_server_with_config(&config).await.unwrap();
    });

    Ok(())
}

async fn connect() -> Result<YamuxCtrl<TlsStream<TcpStream>>> {
    let mut config: ClientConfig = toml::from_str(include_str!("../fixtures/client.conf"))?;
    config.general.addr = SERVER_ADDR.into();

    Ok(start_client_with_config(&config).await?)
}

async fn start_subscribers(topic: &'static str) -> Result<()> {
    let mut ctrl = connect().await?;
    let stream = ctrl.open_stream().await?;
    info!("C(subscriber): stream opened");
    let cmd = CommandRequest::new_subscribe(topic.to_string());
    tokio::spawn(async move {
        let mut stream = stream.execute_stream(&cmd).await.unwrap();
        while let Some(Ok(data)) = stream.next().await {
            drop(data);
        }
    });

    Ok(())
}

async fn start_publishers(topic: &'static str, values: &'static [&'static str]) -> Result<()> {
    let mut rng = rand::thread_rng();
    let v = values.choose(&mut rng).unwrap();

    let mut ctrl = connect().await?;
    let mut stream = ctrl.open_stream().await?;
    info!("C(publisher): stream opened");

    let cmd = CommandRequest::new_publish(topic.to_string(), vec![(*v).into()]);
    stream.execute_unary(cmd).await?;

    Ok(())
}

// 全局运行时存储
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn pubsub() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("pubsub")
            .enable_all()
            .build()
            .unwrap();

        let tracer = runtime.block_on(async {
            opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(OTLP_ENDPOINT),
                )
                .with_batch_config(BatchConfig::default().with_max_queue_size(9999999))
                .with_trace_config(
                    trace::config()
                        .with_sampler(trace::Sampler::AlwaysOn)
                        .with_resource(Resource::new(vec![KeyValue::new(
                            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                            "kv_pubsub_benchmark",
                        )])),
                )
                .install_batch(runtime::Tokio)
                .unwrap()
        });

        global::set_text_map_propagator(TraceContextPropagator::new());

        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        tracing_subscriber::registry().with(telemetry).init();

        let tracer_box = global::tracer("benchmark");

        tracer_box.in_span("start subscriber", |_ctx| {
            runtime.block_on(async {
                eprint!("preparing server and subscribers");
                start_server().await.unwrap();
                time::sleep(Duration::from_millis(100)).await;
                for _ in 0..SUBSCRIBER_COUNT {
                    start_subscribers(TOPIC).await.unwrap();
                    eprint!(".");
                }
                eprintln!("Done!");
            });
        });

        runtime
    })
}

fn start_benchmark(c: &mut Criterion) {
    let runtime = pubsub();

    c.bench_function("publishing", move |b| {
        b.to_async(runtime)
            .iter(|| async { start_publishers(TOPIC, MESSAGE_VALUES).await });
    });
    global::shutdown_tracer_provider();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(10)).warm_up_time(Duration::from_secs(10));
    targets = start_benchmark
}
criterion_main!(benches);
