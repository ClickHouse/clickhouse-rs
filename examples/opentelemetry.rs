use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[tokio::main]
async fn main() {
    // The `TraceContextPropagator` is responsible for generating a trace ID
    // that will then be forwarded to ClickHouse server for distributed tracing:
    // https://clickhouse.com/docs/operations/opentelemetry
    //
    // Without this, OpenTelemetry spans will still be generated, but the `clickhouse` crate
    // will not be able to add the `traceparent` and `tracestate` headers to requests
    // to the ClickHouse HTTP interface.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // Configure export of OTLP spans over HTTP.
    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        // The endpoint to listen on for the collector (default).
        .with_endpoint("http://localhost:4318/")
        .build()
        .unwrap();

    let provider = SdkTracerProvider::builder()
        // `.with_batch_exporter()` spawns a background thread to collect traces
        // so application threads don't get blocked.
        .with_batch_exporter(otlp_exporter)
        .build();

    // Creates a `tracing` subscriber stack that obeys the `RUST_LOG` variable, writes JSON logs to
    // stdout, and emits span traces to our OTLP endpoint using a different variable.
    tracing_subscriber::registry()
        .with(
            // Create a layer that automatically links `tracing::Span`s to OpenTelemetry spans.
            tracing_opentelemetry::layer()
                .with_tracer(
                    // Set the application name in emitted spans.
                    provider.tracer("clickhouse_rs_example_opentelemetry"),
                )
                // Filter what modules emit traces to OTLP by setting `RUST_SPAN`,
                // using the same syntax as the `RUST_LOG` variable.
                .with_filter(
                    EnvFilter::builder()
                        .with_env_var("RUST_SPAN")
                        // By default, emit all spans at `INFO` level and above.
                        .with_default_directive(LevelFilter::INFO.into())
                        .from_env_lossy(),
                ),
        )
        .with(
            // Write JSON logs to stdout.
            tracing_subscriber::fmt::layer()
                .json()
                // Filter logs using the conventional `RUST_LOG` variable:
                // https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/index.html#filtering-events-with-environment-variables
                // By placing the filter here, we can filter spans going to OTLP separately.
                .with_filter(EnvFilter::from_default_env()),
        )
        .init();

    // TODO: basic query and insert to show traces being emitted
}
