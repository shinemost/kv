use anyhow::Result;
use kv::{LogLevel, RotationConfig, ServerConfig, start_server_with_config};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::BatchConfig;
use opentelemetry_sdk::{Resource, runtime, trace};
use std::env;
use tokio::fs;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::time::LocalTime;
use tracing_subscriber::fmt::{format, time};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

const OTLP_ENDPOINT: &str = "http://localhost:4317";

#[tokio::main]
async fn main() -> Result<()> {
    let config = match env::var("KV_SERVER_CONFIG") {
        Ok(path) => fs::read_to_string(&path).await?,
        Err(_) => include_str!("../fixtures/server.conf").to_string(),
    };

    let config: ServerConfig = toml::from_str(&config)?;

    let tracer = opentelemetry_otlp::new_pipeline()
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
                    "kv_server",
                )])),
        )
        .install_batch(runtime::Tokio)?;

    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer).boxed();

    global::set_text_map_propagator(TraceContextPropagator::new());
    global::tracer("kv_server");

    // 添加
    let log = &config.log;
    let file_appender = match log.rotation {
        RotationConfig::Hourly => tracing_appender::rolling::hourly(&log.path, "server.log"),
        RotationConfig::Daily => tracing_appender::rolling::daily(&log.path, "server.log"),
        RotationConfig::Never => tracing_appender::rolling::never(&log.path, "server.log"),
    };

    let (non_blocking, _guard1) = tracing_appender::non_blocking(file_appender);

    let base_env_filter = match &log.log_level {
        LogLevel::Trace => LevelFilter::TRACE,
        LogLevel::Debug => LevelFilter::DEBUG,
        LogLevel::Info => LevelFilter::INFO,
        LogLevel::Warn => LevelFilter::WARN,
        LogLevel::Error => LevelFilter::ERROR,
        LogLevel::Fatal => LevelFilter::ERROR, // fatal 通常映射为 error
    };

    let env_filter = EnvFilter::from_default_env().add_directive(base_env_filter.into());

    // 针对文件日志的 level 过滤
    let fmt_layer = fmt::layer()
        .event_format(format().compact())
        .with_writer(non_blocking)
        .and_then(env_filter.clone());

    // 日志格式 format
    let format = tracing_subscriber::fmt::format()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_level(true)
        .with_source_location(true)
        .with_target(true)
        // .with_timer(time::ChronoLocal::new("%Y-%m-%d %H:%M:%S %Z".to_string()))
        .with_timer(time::LocalTime::rfc_3339())
        .compact();

    // 针对控制台日志的 level 过滤
    let std_layer = fmt::layer()
        .event_format(format.clone())
        .with_writer(std::io::stdout)
        .and_then(env_filter.clone());

    // 判断是否启用了日志文件输出
    if log.enable_log_file {
        tracing_subscriber::registry()
            .with(opentelemetry)
            .with(std_layer)
            .with(fmt_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(opentelemetry)
            .with(std_layer)
            .init();
    }

    start_server_with_config(&config).await?;

    Ok(())
}
