use crate::get_client;
use opentelemetry::global::BoxedTracer;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::sync::Once;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;

#[tokio::test]
async fn query_with_opentelemetry() {
    let tracer = get_tracer();

    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer)),
    );

    let span = tracing::info_span!("query_with_opentelemetry", "foo=bar");

    let context = span.context();

    let client = get_client();

    let query_id = format!("clickhouse_test_{}", rand::random::<u64>());

    let _numbers = client
        .query("SELECT * FROM system.numbers LIMIT 10")
        .with_option("query_id", query_id.clone())
        .fetch_all::<u64>()
        .instrument(span)
        .await
        .unwrap();

    let trace_id = context.span().span_context().trace_id();

    crate::flush_query_log(&client).await;

    let span_query_id = client
        .query(
            "SELECT attribute['clickhouse.query_id'] AS query_id \
         FROM system.opentelemetry_span_log \
         WHERE trace_id = {trace_id:String} AND query_id != ''",
        )
        .param("trace_id", trace_id.to_string())
        // ClickHouse parses and stores the hex span ID as an integer
        .fetch_one::<String>()
        .await
        .unwrap();

    assert_eq!(query_id, span_query_id);
}

fn get_tracer() -> BoxedTracer {
    static ONCE: Once = Once::new();

    ONCE.call_once(|| {
        // These set global statics so we want to ensure this is done,
        // but it only needs to be done once.
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
            .build();

        opentelemetry::global::set_tracer_provider(provider);
    });

    opentelemetry::global::tracer("clickhouse_test_opentelemetry")
}
