use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::runtime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 OpenTelemetry tracer
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint("http://localhost:4317"), // Jaeger 的 OTLP gRPC 端口
        )
        .install_batch(runtime::Tokio)?;

    // 将 OpenTelemetry 集成到 tracing 系统
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(telemetry_layer)
        .try_init()?;

    // 现在可以使用 tracing 宏，数据会自动发送到 Jaeger
    tracing::info_span!("main_operation").in_scope(|| {
        tracing::info!("Starting application");
        do_work();
        tracing::info!("Application finished");
    });

    // 确保所有数据都发送完成
    opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}

fn do_work() {
    tracing::info_span!("do_work").in_scope(async || {
        tracing::info!("Doing some work");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        tracing::info!("Work completed");
    });
}
