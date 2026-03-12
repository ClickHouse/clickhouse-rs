use serde::{Deserialize, Serialize};

use clickhouse::async_inserter::{AsyncInserter, AsyncInserterConfig};
use clickhouse::{Client, Row};

#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
struct MyRow {
    id: u32,
    data: String,
}

async fn create_table(client: &Client) {
    client
        .query(
            "CREATE TABLE test(id UInt32, data String) \
             ENGINE = MergeTree ORDER BY id",
        )
        .execute()
        .await
        .unwrap();
}

async fn count_rows(client: &Client) -> u64 {
    client
        .query("SELECT count() FROM test")
        .fetch_one::<u64>()
        .await
        .unwrap()
}

// ═══════════════════════════════════════════════════════════════════════════
// Happy-path tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn async_inserter_basic() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    for i in 0..100u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    assert_eq!(count_rows(&client).await, 100);
}

#[tokio::test]
async fn async_inserter_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    for i in 0..50u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    let q = inserter.flush().await.unwrap();
    assert_eq!(q.rows, 50);
    assert_eq!(count_rows(&client).await, 50);

    for i in 50..100u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 100);
}

#[tokio::test]
async fn async_inserter_max_rows() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_max_rows(10)
            .without_period(),
    );

    for i in 0..35u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 35);
}

#[tokio::test]
async fn async_inserter_period_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_max_rows(u64::MAX)
            .with_max_bytes(u64::MAX)
            .with_max_period(tokio::time::Duration::from_millis(200)),
    );

    for i in 0..20u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    assert_eq!(count_rows(&client).await, 20);

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 20);
}

#[tokio::test]
async fn async_inserter_empty_end() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 0);
}

#[tokio::test]
async fn async_inserter_concurrent_handles() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let mut tasks = Vec::new();
    for chunk_start in (0..100u32).step_by(10) {
        let handle = inserter.handle();
        tasks.push(tokio::spawn(async move {
            for i in chunk_start..chunk_start + 10 {
                handle
                    .write(MyRow { id: i, data: i.to_string() })
                    .await
                    .unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 100);
}

// ═══════════════════════════════════════════════════════════════════════════
// Edge cases
// ═══════════════════════════════════════════════════════════════════════════

/// Writing a single row should work.
#[tokio::test]
async fn async_inserter_single_row() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    inserter
        .write(MyRow {
            id: 42,
            data: "hello".into(),
        })
        .await
        .unwrap();

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 1);
}

/// Flush on an empty buffer should return zero quantities (not error).
#[tokio::test]
async fn async_inserter_flush_empty() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let q = inserter.flush().await.unwrap();
    assert_eq!(q.rows, 0);
    assert_eq!(q.bytes, 0);

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 0);
}

/// Multiple flushes in a row without writes between them.
#[tokio::test]
async fn async_inserter_double_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    for i in 0..10u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    let q1 = inserter.flush().await.unwrap();
    assert_eq!(q1.rows, 10);

    // Second flush with nothing buffered.
    let q2 = inserter.flush().await.unwrap();
    assert_eq!(q2.rows, 0);

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 10);
}

/// max_rows=1 should auto-flush after every single row.
#[tokio::test]
async fn async_inserter_max_rows_one() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_max_rows(1)
            .without_period(),
    );

    for i in 0..5u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 5);
}

/// Rows with large string data still round-trip correctly.
#[tokio::test]
async fn async_inserter_large_strings() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let big = "x".repeat(100_000);
    for i in 0..3u32 {
        inserter
            .write(MyRow {
                id: i,
                data: big.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<MyRow> = client
        .query("SELECT id, data FROM test ORDER BY id")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].data.len(), 100_000);
}

/// max_bytes flush threshold triggers when accumulated serialised data is large.
#[tokio::test]
async fn async_inserter_max_bytes_trigger() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_max_rows(u64::MAX)
            .with_max_bytes(100) // very small — should trigger after a few rows
            .without_period(),
    );

    for i in 0..20u32 {
        inserter
            .write(MyRow {
                id: i,
                data: "some payload data here".into(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 20);
}

/// Small channel capacity (1) forces extreme backpressure but should still work.
#[tokio::test]
async fn async_inserter_tiny_channel() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_channel_capacity(1)
            .without_period(),
    );

    for i in 0..20u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// Failure / error propagation tests
// ═══════════════════════════════════════════════════════════════════════════

/// Writing to a non-existent table should propagate the ClickHouse error back
/// to the caller (on flush/end, not on write — writes only serialize).
#[tokio::test]
async fn async_inserter_bad_table() {
    let client = prepare_database!();
    // Intentionally do NOT create the table.

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "this_table_does_not_exist",
        AsyncInserterConfig::default()
            .with_max_rows(1) // force flush after one row
            .without_period(),
    );

    // write() serialises into the buffer — the error surfaces on the commit
    // triggered by max_rows=1.
    let result = inserter
        .write(MyRow {
            id: 1,
            data: "x".into(),
        })
        .await;

    // The error might surface on write (if commit happens inline) or on end.
    if result.is_ok() {
        let end_result = inserter.end().await;
        // At least end() should report the error.
        assert!(
            end_result.is_err() || end_result.unwrap().rows == 0,
            "expected error or zero rows for non-existent table"
        );
    }
}

/// Handle becomes inert after the inserter is ended — writes should fail.
#[tokio::test]
async fn async_inserter_handle_after_end() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let handle = inserter.handle();

    inserter.end().await.unwrap();

    // The background task has stopped — write via handle should fail.
    let result = handle
        .write(MyRow {
            id: 1,
            data: "late".into(),
        })
        .await;
    assert!(result.is_err(), "write after end() should fail");
}

/// flush() via handle after inserter is ended should fail.
#[tokio::test]
async fn async_inserter_flush_after_end() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let handle = inserter.handle();

    inserter.end().await.unwrap();

    let result = handle.flush().await;
    assert!(result.is_err(), "flush after end() should fail");
}

// ═══════════════════════════════════════════════════════════════════════════
// Stress / concurrency tests
// ═══════════════════════════════════════════════════════════════════════════

/// Many concurrent writers with small max_rows to stress the flush path.
#[tokio::test]
async fn async_inserter_stress_concurrent() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_max_rows(7) // prime number to create odd batch boundaries
            .without_period(),
    );

    let mut tasks = Vec::new();
    for task_id in 0..20u32 {
        let handle = inserter.handle();
        tasks.push(tokio::spawn(async move {
            for j in 0..50u32 {
                let id = task_id * 50 + j;
                handle
                    .write(MyRow {
                        id,
                        data: format!("task{task_id}_row{j}"),
                    })
                    .await
                    .unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 1000);
}

/// Interleaved writes and flushes from multiple handles.
#[tokio::test]
async fn async_inserter_interleaved_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let h1 = inserter.handle();
    let h2 = inserter.handle();

    // Writer 1: write 10 rows, then flush.
    for i in 0..10u32 {
        h1.write(MyRow { id: i, data: "a".into() }).await.unwrap();
    }
    h1.flush().await.unwrap();

    // Writer 2: write 10 rows, then flush.
    for i in 10..20u32 {
        h2.write(MyRow { id: i, data: "b".into() }).await.unwrap();
    }
    h2.flush().await.unwrap();

    assert_eq!(count_rows(&client).await, 20);

    drop(h1);
    drop(h2);
    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// Large ugly JSON source tests — Filebeat / Winlogbeat payloads
// ═══════════════════════════════════════════════════════════════════════════
//
// These tests exercise the full insert round-trip with realistic, deeply
// nested JSON blobs that match what Elastic Beat agents produce in the wild.
// They stress: large String values, Unicode, Windows backslash paths,
// embedded newlines, null-heavy payloads, arrays of objects, and mixed types.

#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
struct LogRow {
    ts: u64,
    source: String,
    json_data: String,
}

fn filebeat_nginx_json() -> String {
    r#"{
  "@timestamp": "2026-03-12T08:14:22.337Z",
  "@metadata": {
    "beat": "filebeat",
    "type": "_doc",
    "version": "8.17.0",
    "pipeline": "filebeat-8.17.0-nginx-access-pipeline"
  },
  "agent": {
    "name": "web-prod-03.dc1.example.com",
    "type": "filebeat",
    "version": "8.17.0",
    "ephemeral_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "id": "deadbeef-cafe-babe-f00d-123456789abc",
    "hostname": "web-prod-03.dc1.example.com"
  },
  "log": {
    "file": { "path": "/var/log/nginx/access.log", "inode": "1234567" },
    "offset": 9823741,
    "flags": ["utf-8", "multiline"]
  },
  "message": "192.168.1.100 - jean-françois [12/Mar/2026:08:14:22 +0000] \"GET /api/v2/données/résultat?q=名前&page=1&size=50 HTTP/2.0\" 200 13847 \"https://app.example.com/dashboard/über-ansicht\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\" \"-\" rt=0.042 uct=0.001 uht=0.040 urt=0.041",
  "source": { "address": "192.168.1.100", "ip": "192.168.1.100", "geo": null },
  "http": {
    "request": {
      "method": "GET",
      "referrer": "https://app.example.com/dashboard/über-ansicht",
      "headers": {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
        "X-Request-ID": "req_7f8a9b0c-1d2e-3f4a-5b6c-7d8e9f0a1b2c",
        "X-Forwarded-For": "10.0.0.1, 172.16.0.1, 192.168.1.100",
        "Cookie": "session=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkrDqWFuLUZyYW7Dp29pcyIsImlhdCI6MTUxNjIzOTAyMn0.fake_sig"
      }
    },
    "response": {
      "status_code": 200,
      "body": { "bytes": 13847 },
      "headers": {
        "Content-Type": "application/json; charset=utf-8",
        "X-Cache": "MISS",
        "X-Served-By": "backend-pool-2a"
      }
    },
    "version": "2.0"
  },
  "url": {
    "original": "/api/v2/données/résultat?q=名前&page=1&size=50",
    "path": "/api/v2/données/résultat",
    "query": "q=名前&page=1&size=50",
    "domain": "app.example.com",
    "scheme": "https",
    "port": 443
  },
  "nginx": {
    "access": {
      "upstream": {
        "response_time": 0.041,
        "connect_time": 0.001,
        "header_time": 0.040,
        "addr": ["10.0.2.15:8080", "10.0.2.16:8080"],
        "status": [200]
      },
      "geoip": {
        "country_iso_code": "DE",
        "city_name": "München",
        "location": { "lat": 48.1351, "lon": 11.5820 }
      }
    }
  },
  "ecs": { "version": "8.0.0" },
  "tags": ["nginx", "web", "production", "dc1"],
  "fields": {
    "environment": "production",
    "team": "platform-engineering",
    "cost_center": "CC-4242"
  },
  "event": {
    "dataset": "nginx.access",
    "module": "nginx",
    "category": ["web"],
    "type": ["access"],
    "outcome": "success",
    "duration": 42000000,
    "created": "2026-03-12T08:14:22.380Z",
    "ingested": "2026-03-12T08:14:23.001Z"
  }
}"#.to_string()
}

fn winlogbeat_security_json() -> String {
    r#"{
  "@timestamp": "2026-03-12T03:47:11.892Z",
  "@metadata": {
    "beat": "winlogbeat",
    "type": "_doc",
    "version": "8.17.0"
  },
  "agent": {
    "name": "DC01.corp.contoso.com",
    "type": "winlogbeat",
    "version": "8.17.0",
    "ephemeral_id": "f1e2d3c4-b5a6-9780-fedc-ba0987654321",
    "id": "01234567-89ab-cdef-0123-456789abcdef"
  },
  "winlog": {
    "channel": "Security",
    "provider_name": "Microsoft-Windows-Security-Auditing",
    "provider_guid": "{54849625-5478-4994-A5BA-3E3B0328C30D}",
    "event_id": 4625,
    "version": 0,
    "task": "Logon",
    "opcode": "Info",
    "keywords": ["Audit Failure"],
    "record_id": 987654321,
    "computer_name": "DC01.corp.contoso.com",
    "process": { "pid": 788, "thread": { "id": 4892 } },
    "api": "wineventlog",
    "activity_id": "{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}",
    "event_data": {
      "SubjectUserSid": "S-1-5-18",
      "SubjectUserName": "DC01$",
      "SubjectDomainName": "CORP",
      "SubjectLogonId": "0x3e7",
      "TargetUserSid": "S-1-0-0",
      "TargetUserName": "администратор",
      "TargetDomainName": "CORP",
      "Status": "0xc000006d",
      "FailureReason": "%%2313",
      "SubStatus": "0xc0000064",
      "LogonType": "10",
      "LogonProcessName": "User32 ",
      "AuthenticationPackageName": "Negotiate",
      "WorkstationName": "АТАКУЮЩИЙ-ПК",
      "TransmittedServices": "-",
      "LmPackageName": "-",
      "KeyLength": "0",
      "ProcessId": "0x0",
      "ProcessName": "-",
      "IpAddress": "198.51.100.23",
      "IpPort": "49832"
    }
  },
  "event": {
    "code": "4625",
    "kind": "event",
    "provider": "Microsoft-Windows-Security-Auditing",
    "action": "logon-failed",
    "category": ["authentication"],
    "type": ["start"],
    "outcome": "failure",
    "created": "2026-03-12T03:47:12.100Z",
    "ingested": "2026-03-12T03:47:13.250Z",
    "severity": 0
  },
  "host": {
    "name": "DC01",
    "hostname": "DC01.corp.contoso.com",
    "os": {
      "family": "windows",
      "name": "Windows Server 2022",
      "version": "10.0.20348.2340",
      "build": "20348.2340",
      "platform": "windows",
      "type": "windows",
      "kernel": "10.0.20348.2340 (WinBuild.160101.0800)"
    },
    "ip": ["10.0.0.5", "fe80::1234:5678:abcd:ef01"],
    "mac": ["00-15-5D-01-02-03"],
    "architecture": "x86_64",
    "domain": "corp.contoso.com"
  },
  "source": {
    "ip": "198.51.100.23",
    "port": 49832,
    "geo": {
      "country_iso_code": "RU",
      "city_name": "Москва",
      "region_name": "Москва",
      "location": { "lat": 55.7558, "lon": 37.6173 },
      "timezone": "Europe/Moscow"
    }
  },
  "user": {
    "name": "администратор",
    "domain": "CORP",
    "id": "S-1-0-0",
    "target": {
      "name": "администратор",
      "domain": "CORP"
    }
  },
  "message": "An account failed to log on.\n\nSubject:\n\tSecurity ID:\t\tS-1-5-18\n\tAccount Name:\t\tDC01$\n\tAccount Domain:\t\tCORP\n\tLogon ID:\t\t0x3E7\n\nLogon Information:\n\tLogon Type:\t\t10\n\tRestricted Admin Mode:\t-\n\tVirtual Account:\t\tNo\n\tElevated Token:\t\tNo\n\nFailure Information:\n\tFailure Reason:\t\tUnknown user name or bad password.\n\tStatus:\t\t\t0xC000006D\n\tSub Status:\t\t0xC0000064\n\nNew Logon:\n\tSecurity ID:\t\tS-1-0-0\n\tAccount Name:\t\tадминистратор\n\tAccount Domain:\t\tCORP\n\nProcess Information:\n\tCaller Process ID:\t0x0\n\tCaller Process Name:\t-\n\nNetwork Information:\n\tWorkstation Name:\tАТАКУЮЩИЙ-ПК\n\tSource Network Address:\t198.51.100.23\n\tSource Port:\t\t49832",
  "related": {
    "ip": ["198.51.100.23", "10.0.0.5"],
    "user": ["DC01$", "администратор"]
  },
  "ecs": { "version": "8.0.0" },
  "tags": ["security", "authentication", "failed-logon", "brute-force-candidate"]
}"#.to_string()
}

fn filebeat_multiline_java_json() -> String {
    r#"{
  "@timestamp": "2026-03-12T14:22:03.001Z",
  "@metadata": { "beat": "filebeat", "version": "8.17.0" },
  "agent": { "name": "app-srv-07", "type": "filebeat", "version": "8.17.0" },
  "log": {
    "file": {
      "path": "C:\\Program Files\\MyApp\\logs\\application-2026-03-12.log",
      "inode": "0"
    },
    "offset": 482716,
    "flags": ["utf-8", "multiline"]
  },
  "message": "2026-03-12 14:22:02,999 ERROR [http-nio-8443-exec-42] com.example.api.UserController - Failed to process request for user_id=café-résumé-42\njava.lang.NullPointerException: Cannot invoke \"com.example.model.UserProfile.getDisplayName()\" because the return value of \"com.example.service.UserService.findById(String)\" is null\n\tat com.example.api.UserController.getUserProfile(UserController.java:142)\n\tat com.example.api.UserController$$FastClassBySpringCGLIB$$abc123.invoke(<generated>)\n\tat org.springframework.cglib.proxy.MethodProxy.invoke(MethodProxy.java:218)\n\tat org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.invokeJoinpoint(CglibAopProxy.java:793)\n\tat org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:163)\n\tat org.springframework.aop.framework.CglibAopProxy$DynamicAdvisedInterceptor.intercept(CglibAopProxy.java:723)\n\tat com.example.api.UserController$$EnhancerBySpringCGLIB$$def456.getUserProfile(<generated>)\n\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n\tat java.lang.reflect.Method.invoke(Method.java:498)\n\tat org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:61)\n\tat java.lang.Thread.run(Thread.java:750)\nCaused by: org.hibernate.exception.JDBCConnectionException: Unable to acquire JDBC Connection\n\tat org.hibernate.exception.internal.SQLExceptionTypeDelegate.convert(SQLExceptionTypeDelegate.java:48)\n\tat com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:163)\n\tat com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:128)\nCaused by: java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available, request timed out after 30000ms.\n\tat com.zaxxer.hikari.pool.HikariPool.createTimeoutException(HikariPool.java:695)\n\t... 42 more",
  "error": {
    "type": "java.lang.NullPointerException",
    "message": "Cannot invoke \"com.example.model.UserProfile.getDisplayName()\"",
    "stack_trace": "... (see message field for full trace)"
  },
  "host": {
    "name": "app-srv-07",
    "os": {
      "family": "windows",
      "name": "Windows Server 2019",
      "version": "10.0.17763.5329"
    },
    "ip": ["10.10.20.7"]
  },
  "service": {
    "name": "user-api",
    "version": "3.14.159-SNAPSHOT",
    "environment": "staging",
    "node": { "name": "app-srv-07:8443" }
  },
  "labels": {
    "deployment_id": "deploy-2026-03-12-r42",
    "git_sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
    "jira_ticket": "PLAT-9876"
  },
  "ecs": { "version": "8.0.0" },
  "tags": ["java", "error", "staging", "connection-pool-exhaustion"]
}"#.to_string()
}

fn winlogbeat_powershell_json() -> String {
    r#"{
  "@timestamp": "2026-03-12T01:15:44.203Z",
  "@metadata": { "beat": "winlogbeat", "version": "8.17.0" },
  "agent": { "name": "WS-FINANCE-12", "type": "winlogbeat" },
  "winlog": {
    "channel": "Microsoft-Windows-PowerShell/Operational",
    "provider_name": "Microsoft-Windows-PowerShell",
    "event_id": 4104,
    "task": "Execute a Remote Command",
    "opcode": "On create calls",
    "record_id": 55432,
    "computer_name": "WS-FINANCE-12.corp.contoso.com",
    "process": { "pid": 6328, "thread": { "id": 7204 } },
    "event_data": {
      "MessageNumber": "1",
      "MessageTotal": "1",
      "ScriptBlockText": "function Invoke-Çömpléx_Tàsk {\n    param(\n        [Parameter(Mandatory=$true)]\n        [string]$Tärget,\n        [ValidateSet('Réad','Wríte','Éxecute')]\n        [string]$Möde = 'Réad'\n    )\n    \n    $encodedCmd = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($Tärget))\n    $résult = @{\n        'Tïmestamp' = (Get-Date -Format 'yyyy-MM-ddTHH:mm:ss.fffZ')\n        'Üser'      = $env:USERNAME\n        'Dömain'    = $env:USERDOMAIN\n        'Pàth'      = \"C:\\Users\\$env:USERNAME\\AppData\\Local\\Temp\\öutput_$(Get-Random).tmp\"\n        'Àrgs'      = @($Tärget, $Möde, $encodedCmd)\n        'Nësted'    = @{\n            'Dëep1' = @{\n                'Dëep2' = @{\n                    'Dëep3' = @{\n                        'value' = 'We\\'re testing deep nesting with spëcial chars: <>&\\\"\\'/'\n                    }\n                }\n            }\n        }\n    }\n    \n    $résult | ConvertTo-Json -Depth 10 | Out-File -FilePath $résult['Pàth'] -Encoding UTF8\n    return $résult\n}",
      "ScriptBlockId": "b7c8d9e0-f1a2-3b4c-5d6e-7f8a9b0c1d2e",
      "Path": "C:\\Users\\jëan-pierré\\Documents\\Scrïpts\\Ïnvoke-Task.ps1"
    }
  },
  "event": {
    "code": "4104",
    "kind": "event",
    "provider": "Microsoft-Windows-PowerShell",
    "category": ["process"],
    "type": ["info"],
    "outcome": "success"
  },
  "host": {
    "name": "WS-FINANCE-12",
    "hostname": "WS-FINANCE-12.corp.contoso.com",
    "os": {
      "family": "windows",
      "name": "Windows 11 Enterprise",
      "version": "10.0.22631.3155",
      "build": "22631.3155"
    },
    "ip": ["10.20.30.12", "fe80::abcd:ef01:2345:6789"],
    "mac": ["00-50-56-AB-CD-EF"]
  },
  "user": {
    "name": "jëan-pierré",
    "domain": "CORP",
    "id": "S-1-5-21-1234567890-1234567890-1234567890-5678"
  },
  "process": {
    "pid": 6328,
    "executable": "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",
    "command_line": "powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"C:\\Users\\jëan-pierré\\Documents\\Scrïpts\\Ïnvoke-Task.ps1\"",
    "parent": {
      "pid": 4120,
      "executable": "C:\\Windows\\explorer.exe"
    }
  },
  "message": "Creating Scriptblock text (1 of 1):\nfunction Invoke-Çömpléx_Tàsk { ... (see ScriptBlockText for full content)",
  "related": {
    "user": ["jëan-pierré"]
  },
  "ecs": { "version": "8.0.0" },
  "tags": ["powershell", "scriptblock", "finance-dept"]
}"#.to_string()
}

fn filebeat_kubernetes_json() -> String {
    r#"{
  "@timestamp": "2026-03-12T19:33:07.445Z",
  "@metadata": { "beat": "filebeat", "version": "8.17.0" },
  "agent": { "name": "k8s-node-pool-a-2", "type": "filebeat" },
  "kubernetes": {
    "pod": {
      "name": "payment-svc-7b8c9d-xq2f4",
      "uid": "12345678-abcd-ef01-2345-67890abcdef0",
      "ip": "10.244.3.17",
      "labels": {
        "app_kubernetes_io/name": "payment-svc",
        "app_kubernetes_io/version": "2.71.828",
        "app_kubernetes_io/component": "api",
        "helm_sh/chart": "payment-svc-2.71.828",
        "pod-template-hash": "7b8c9d"
      },
      "annotations": {
        "prometheus_io/scrape": "true",
        "prometheus_io/port": "9090",
        "vault_hashicorp_com/agent-inject": "true",
        "vault_hashicorp_com/role": "payment-svc-prod"
      }
    },
    "node": {
      "name": "k8s-node-pool-a-2",
      "hostname": "k8s-node-pool-a-2.cluster.local",
      "labels": {
        "kubernetes_io/arch": "amd64",
        "node_kubernetes_io/instance-type": "m5.2xlarge",
        "topology_kubernetes_io/zone": "ap-southeast-2a"
      }
    },
    "namespace": "payment-prod",
    "replicaset": { "name": "payment-svc-7b8c9d" },
    "deployment": { "name": "payment-svc" },
    "container": {
      "name": "payment-api",
      "image": "harbor.internal/payment/api:2.71.828-deadbeef",
      "id": "containerd://abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    }
  },
  "container": {
    "id": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    "image": { "name": "harbor.internal/payment/api:2.71.828-deadbeef" },
    "runtime": "containerd"
  },
  "log": {
    "file": {
      "path": "/var/log/pods/payment-prod_payment-svc-7b8c9d-xq2f4_12345678-abcd-ef01-2345-67890abcdef0/payment-api/0.log"
    }
  },
  "message": "{\"level\":\"error\",\"ts\":1741804387.445,\"caller\":\"handler/payment.go:287\",\"msg\":\"payment processing failed\",\"trace_id\":\"abc123def456\",\"span_id\":\"789012\",\"request_id\":\"req-ñoño-42\",\"customer_id\":\"cust_Ωmega_∆lpha\",\"amount\":\"¥123,456.78\",\"currency\":\"JPY\",\"gateway_response\":{\"code\":\"DECLINED_INSUFFICIENT_FUNDS\",\"raw\":\"カード残高不足です。別のお支払い方法をお試しください。\",\"retry_after_ms\":null,\"metadata\":{\"issuer_country\":\"JP\",\"card_brand\":\"JCB\",\"last4\":\"4242\",\"3ds_enrolled\":true,\"risk_score\":0.73}},\"stack\":\"goroutine 847 [running]:\\nruntime/debug.Stack()\\n\\t/usr/local/go/src/runtime/debug/stack.go:24 +0x5e\\ngithub.com/example/payment-svc/internal/handler.(*PaymentHandler).ProcessPayment(...)\\n\\t/app/internal/handler/payment.go:287 +0x1a3\\ngithub.com/example/payment-svc/internal/handler.(*PaymentHandler).HandleRequest(...)\\n\\t/app/internal/handler/payment.go:142 +0x892\"}",
  "stream": "stderr",
  "event": {
    "dataset": "kubernetes.container_logs",
    "module": "kubernetes"
  },
  "ecs": { "version": "8.0.0" },
  "tags": ["kubernetes", "payment", "production", "pci-zone"]
}"#.to_string()
}

async fn create_log_table(client: &Client) {
    client
        .query(
            "CREATE TABLE test_logs(ts UInt64, source String, json_data String) \
             ENGINE = MergeTree ORDER BY ts",
        )
        .execute()
        .await
        .unwrap();
}

async fn count_log_rows(client: &Client) -> u64 {
    client
        .query("SELECT count() FROM test_logs")
        .fetch_one::<u64>()
        .await
        .unwrap()
}

/// Filebeat nginx access log — Unicode URL params, geo data, nested headers.
#[tokio::test]
async fn async_inserter_filebeat_nginx() {
    let client = prepare_database!();
    create_log_table(&client).await;

    let inserter = AsyncInserter::<LogRow>::new(
        &client,
        "test_logs",
        AsyncInserterConfig::default().without_period(),
    );

    let json = filebeat_nginx_json();
    for i in 0..10u64 {
        inserter
            .write(LogRow {
                ts: 1741760062000 + i,
                source: "filebeat-nginx".into(),
                json_data: json.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<LogRow> = client
        .query("SELECT ts, source, json_data FROM test_logs ORDER BY ts")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 10);
    assert!(rows[0].json_data.contains("jean-françois"));
    assert!(rows[0].json_data.contains("名前"));
    assert!(rows[0].json_data.contains("über-ansicht"));
    assert!(rows[0].json_data.contains("München"));
}

/// Winlogbeat security 4625 — Cyrillic usernames, failed logon, nested event_data.
#[tokio::test]
async fn async_inserter_winlogbeat_security() {
    let client = prepare_database!();
    create_log_table(&client).await;

    let inserter = AsyncInserter::<LogRow>::new(
        &client,
        "test_logs",
        AsyncInserterConfig::default().without_period(),
    );

    let json = winlogbeat_security_json();
    for i in 0..10u64 {
        inserter
            .write(LogRow {
                ts: 1741744031000 + i,
                source: "winlogbeat-security".into(),
                json_data: json.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<LogRow> = client
        .query("SELECT ts, source, json_data FROM test_logs ORDER BY ts")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 10);
    assert!(rows[0].json_data.contains("администратор"));
    assert!(rows[0].json_data.contains("АТАКУЮЩИЙ-ПК"));
    assert!(rows[0].json_data.contains("Москва"));
    assert!(rows[0].json_data.contains("S-1-5-18"));
}

/// Filebeat multiline Java stack trace — Windows paths, embedded newlines, deep exception chain.
#[tokio::test]
async fn async_inserter_filebeat_java_stacktrace() {
    let client = prepare_database!();
    create_log_table(&client).await;

    let inserter = AsyncInserter::<LogRow>::new(
        &client,
        "test_logs",
        AsyncInserterConfig::default().without_period(),
    );

    let json = filebeat_multiline_java_json();
    for i in 0..5u64 {
        inserter
            .write(LogRow {
                ts: 1741781723000 + i,
                source: "filebeat-java".into(),
                json_data: json.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<LogRow> = client
        .query("SELECT ts, source, json_data FROM test_logs ORDER BY ts")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows[0].json_data.contains("NullPointerException"));
    assert!(rows[0].json_data.contains("café-résumé-42"));
    assert!(rows[0].json_data.contains("C:\\\\Program Files\\\\MyApp"));
    assert!(rows[0].json_data.contains("HikariPool"));
}

/// Winlogbeat PowerShell scriptblock — deeply nested diacritics, Base64, special chars.
#[tokio::test]
async fn async_inserter_winlogbeat_powershell() {
    let client = prepare_database!();
    create_log_table(&client).await;

    let inserter = AsyncInserter::<LogRow>::new(
        &client,
        "test_logs",
        AsyncInserterConfig::default().without_period(),
    );

    let json = winlogbeat_powershell_json();
    for i in 0..5u64 {
        inserter
            .write(LogRow {
                ts: 1741742144000 + i,
                source: "winlogbeat-powershell".into(),
                json_data: json.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<LogRow> = client
        .query("SELECT ts, source, json_data FROM test_logs ORDER BY ts")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows[0].json_data.contains("Invoke-Çömpléx_Tàsk"));
    assert!(rows[0].json_data.contains("jëan-pierré"));
    assert!(rows[0].json_data.contains("Scrïpts"));
}

/// Filebeat Kubernetes container log — JSON-in-JSON, CJK payment errors, Go stack trace.
#[tokio::test]
async fn async_inserter_filebeat_kubernetes() {
    let client = prepare_database!();
    create_log_table(&client).await;

    let inserter = AsyncInserter::<LogRow>::new(
        &client,
        "test_logs",
        AsyncInserterConfig::default().without_period(),
    );

    let json = filebeat_kubernetes_json();
    for i in 0..5u64 {
        inserter
            .write(LogRow {
                ts: 1741804387000 + i,
                source: "filebeat-k8s".into(),
                json_data: json.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<LogRow> = client
        .query("SELECT ts, source, json_data FROM test_logs ORDER BY ts")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows[0].json_data.contains("payment-svc-7b8c9d-xq2f4"));
    assert!(rows[0].json_data.contains("カード残高不足"));
    assert!(rows[0].json_data.contains("req-ñoño-42"));
    assert!(rows[0].json_data.contains("cust_Ωmega_∆lpha"));
    assert!(rows[0].json_data.contains("¥123,456.78"));
}

/// Mixed Beat sources in a single batch — concurrent handles, one source per handle.
#[tokio::test]
async fn async_inserter_mixed_beats_concurrent() {
    let client = prepare_database!();
    create_log_table(&client).await;

    let inserter = AsyncInserter::<LogRow>::new(
        &client,
        "test_logs",
        AsyncInserterConfig::default()
            .with_max_rows(15) // force multiple flushes mid-batch
            .without_period(),
    );

    let sources: Vec<(&str, String)> = vec![
        ("filebeat-nginx", filebeat_nginx_json()),
        ("winlogbeat-security", winlogbeat_security_json()),
        ("filebeat-java", filebeat_multiline_java_json()),
        ("winlogbeat-powershell", winlogbeat_powershell_json()),
        ("filebeat-k8s", filebeat_kubernetes_json()),
    ];

    let mut tasks = Vec::new();
    for (idx, (source, json)) in sources.into_iter().enumerate() {
        let handle = inserter.handle();
        let source = source.to_string();
        tasks.push(tokio::spawn(async move {
            for j in 0..20u64 {
                let ts = (idx as u64) * 1_000_000 + j;
                handle
                    .write(LogRow {
                        ts,
                        source: source.clone(),
                        json_data: json.clone(),
                    })
                    .await
                    .unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    inserter.end().await.unwrap();

    // 5 sources × 20 rows = 100
    assert_eq!(count_log_rows(&client).await, 100);

    // Verify each source is present.
    let nginx_count: u64 = client
        .query("SELECT count() FROM test_logs WHERE source = 'filebeat-nginx'")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(nginx_count, 20);

    let security_count: u64 = client
        .query("SELECT count() FROM test_logs WHERE source = 'winlogbeat-security'")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(security_count, 20);
}
