use clickhouse::Client;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn bind(c: &mut Criterion) {
    let client = Client::default();
    let mut group = c.benchmark_group("bind");
    group.sample_size(10);

    for n in [1_000, 10_000, 50_000] {
        let sql = format!("SELECT * FROM t WHERE x IN ({})", vec!["?"; n].join(","));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut query = client.query(&sql);
                for i in 0..n {
                    query = query.bind(i);
                }
                black_box(query.sql_display().to_string())
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bind);
criterion_main!(benches);
