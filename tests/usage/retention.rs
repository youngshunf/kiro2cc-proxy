use super::*;
use std::path::PathBuf;

fn usage_path() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kiro-retention-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&dir).unwrap();
    dir.join("api_key_usage.json")
}

fn record_at(created_at: DateTime<Utc>) -> UsageRecord {
    serde_json::from_value(serde_json::json!({
        "apiKeyId": 7, "credentialId": 31, "model": "claude-opus-4.6",
        "inputTokens": 1000, "outputTokens": 100, "estimatedCost": 0.02,
        "creditsUsed": 0.5, "createdAt": created_at,
    }))
    .unwrap()
}

#[tokio::test]
async fn retention_load_prunes_old_details_without_reducing_totals() {
    let path = usage_path();
    let now = Utc::now();
    let records = vec![record_at(now - chrono::Duration::days(31)), record_at(now)];
    fs::write(&path, serde_json::to_vec(&records).unwrap()).unwrap();
    let tracker = UsageTracker::load(&path).unwrap();
    assert_eq!(tracker.records.read().len(), 1);
    let summary = tracker.get_summary(7);
    assert_eq!(summary.total_requests, 2);
    assert_eq!(summary.total_input_tokens, 2000);
    assert_eq!(summary.total_output_tokens, 200);
    assert!((tracker.get_total_cost(7) - 0.04).abs() < 1e-9);
    assert!((tracker.get_total_credits(7) - 1.0).abs() < 1e-9);
    let credential = tracker.get_summary_for_credential(31);
    assert_eq!(credential.total_requests, 2);
    assert_eq!(credential.total_input_tokens, 2000);
}

#[test]
fn retention_cutoff_keeps_boundary_and_future_records() {
    let now = Utc::now();
    let cutoff = now - chrono::Duration::days(30);
    let records = Arc::new(RwLock::new(vec![
        record_at(cutoff - chrono::Duration::nanoseconds(1)),
        record_at(cutoff),
        record_at(now + chrono::Duration::days(1)),
    ]));
    let keys = Arc::new(RwLock::new(HashMap::new()));
    let credentials = Arc::new(RwLock::new(HashMap::new()));
    assert_eq!(
        UsageTracker::prune_expired(&records, &keys, &credentials, now),
        1
    );
    assert_eq!(records.read().len(), 2);
    assert_eq!(keys.read()[&7].pruned_requests, 1);
    assert_eq!(credentials.read()[&31].pruned_input_tokens, 1000);
    assert_eq!(
        UsageTracker::prune_expired(&records, &keys, &credentials, now),
        0
    );
    assert_eq!(keys.read()[&7].pruned_requests, 1);
}

#[tokio::test]
async fn retention_credential_totals_keep_per_record_negative_token_clamping() {
    let path = usage_path();
    let mut negative = record_at(Utc::now() - chrono::Duration::days(31));
    negative.output_tokens = -100;
    let positive = record_at(Utc::now() - chrono::Duration::days(31));
    fs::write(
        &path,
        serde_json::to_vec(&vec![negative, positive]).unwrap(),
    )
    .unwrap();
    let tracker = UsageTracker::load(&path).unwrap();
    assert!(tracker.records.read().is_empty());
    assert_eq!(
        tracker.get_summary_for_credential(31).total_output_tokens,
        100
    );
}

#[tokio::test(start_paused = true)]
async fn retention_hourly_maintenance_runs_without_new_requests() {
    let path = usage_path();
    let tracker = UsageTracker::load(&path).unwrap();
    // 先消费启动时的立即 tick，再注入无需发送 dirty 通知的过期明细。
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tracker
        .records
        .write()
        .push(record_at(Utc::now() - chrono::Duration::days(31)));
    tokio::time::advance(std::time::Duration::from_secs(3600)).await;
    for _ in 0..100 {
        if tracker.records.read().is_empty() {
            break;
        }
        // 刷盘在阻塞线程完成，虚拟时钟推进后也须等待真实文件操作结束。
        tokio::task::spawn_blocking(|| std::thread::sleep(std::time::Duration::from_millis(1)))
            .await
            .unwrap();
    }
    assert!(tracker.records.read().is_empty());
    assert_eq!(tracker.get_summary(7).total_requests, 1);
    assert!((tracker.get_total_credits(7) - 0.5).abs() < 1e-9);
}

#[tokio::test]
async fn retention_snapshot_roundtrip_ignores_obsolete_lifetime_file() {
    let path = usage_path();
    let records = Arc::new(RwLock::new(vec![record_at(
        Utc::now() - chrono::Duration::days(31),
    )]));
    let keys = Arc::new(RwLock::new(HashMap::new()));
    let credentials = Arc::new(RwLock::new(HashMap::new()));
    UsageTracker::prune_expired(&records, &keys, &credentials, Utc::now());
    UsageTracker::save_internal(&records, &keys, &credentials, &path)
        .await
        .unwrap();
    fs::write(
        path.with_file_name("api_key_lifetime.json"),
        "已废弃的旧文件",
    )
    .unwrap();
    // 模拟写新快照中途退出留下的临时文件，加载必须继续使用完整旧快照。
    fs::write(
        path.with_extension(format!("tmp.{}", std::process::id())),
        "{半截",
    )
    .unwrap();
    let tracker = UsageTracker::load(&path).unwrap();
    assert!(tracker.records.read().is_empty());
    assert_eq!(tracker.get_summary(7).total_requests, 1);
    assert_eq!(tracker.get_summary(7).total_input_tokens, 1000);
    assert_eq!(tracker.max_api_key_id(), 7);
    assert!((tracker.get_total_cost(7) - 0.02).abs() < 1e-9);
    assert!((tracker.get_total_credits(7) - 0.5).abs() < 1e-9);
    assert_eq!(tracker.get_summary_for_credential(31).total_requests, 1);
    assert_eq!(tracker.get_summary(7).by_model[0].requests, 1);
    tracker.reset(7).unwrap();
    assert_eq!(tracker.get_summary(7).total_requests, 0);
    assert_eq!(tracker.get_total_cost(7), 0.0);
}

#[cfg(unix)]
#[tokio::test]
async fn retention_snapshot_save_replaces_inode_and_preserves_old_reader() {
    use std::io::Read;
    use std::os::unix::fs::MetadataExt;
    let path = usage_path();
    fs::write(&path, "[]").unwrap();
    let mut old = fs::File::open(&path).unwrap();
    let inode = old.metadata().unwrap().ino();
    let records = Arc::new(RwLock::new(vec![record_at(Utc::now())]));
    let keys = Arc::new(RwLock::new(HashMap::new()));
    let credentials = Arc::new(RwLock::new(HashMap::new()));
    UsageTracker::save_internal(&records, &keys, &credentials, &path)
        .await
        .unwrap();
    assert_ne!(fs::metadata(&path).unwrap().ino(), inode);
    let mut previous = String::new();
    old.read_to_string(&mut previous).unwrap();
    assert_eq!(previous, "[]");
    let snapshot: UsageSnapshot = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(snapshot.records.len(), 1);
}

#[tokio::test]
async fn retention_snapshot_failed_save_leaves_committed_data_untouched() {
    let path = usage_path();
    fs::write(&path, "[]").unwrap();
    // 临时路径不能创建文件，模拟刷盘前失败，旧快照应完全不变。
    fs::create_dir(path.with_extension(format!("tmp.{}", std::process::id()))).unwrap();
    let records = Arc::new(RwLock::new(vec![record_at(Utc::now())]));
    let keys = Arc::new(RwLock::new(HashMap::new()));
    let credentials = Arc::new(RwLock::new(HashMap::new()));
    assert!(
        UsageTracker::save_internal(&records, &keys, &credentials, &path)
            .await
            .is_err()
    );
    assert_eq!(fs::read_to_string(&path).unwrap(), "[]");
}

#[tokio::test]
async fn retention_rejects_corruption_unknown_versions_and_invalid_legacy_totals() {
    for content in [
        "{截断",
        r#"{"formatVersion":2,"records":[],"lifetimeKeys":{},"lifetimeCredentials":{}}"#,
    ] {
        let path = usage_path();
        fs::write(&path, content).unwrap();
        assert!(UsageTracker::load(&path).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), content);
    }
    let path = usage_path();
    fs::write(&path, "[]").unwrap();
    fs::write(path.with_file_name("api_key_lifetime.json"), "{截断").unwrap();
    assert!(UsageTracker::load(&path).is_err());
}

#[tokio::test]
async fn retention_legacy_migration_keeps_existing_cumulative_base() {
    let path = usage_path();
    fs::write(
        &path,
        serde_json::to_vec(&vec![record_at(Utc::now() - chrono::Duration::days(31))]).unwrap(),
    )
    .unwrap();
    fs::write(
        path.with_file_name("api_key_lifetime.json"),
        r#"{"7":{"prunedRequests":100,"prunedInputTokens":9000,"prunedOutputTokens":500}}"#,
    )
    .unwrap();
    let tracker = UsageTracker::load(&path).unwrap();
    assert!(tracker.records.read().is_empty());
    let summary = tracker.get_summary(7);
    assert_eq!(summary.total_requests, 101);
    assert_eq!(summary.total_input_tokens, 10000);
    assert_eq!(summary.total_output_tokens, 600);
}

#[tokio::test]
async fn retention_count_limit_keeps_costs_and_credential_totals() {
    let path = usage_path();
    let tracker = UsageTracker::load(&path).unwrap();
    tracker
        .records
        .write()
        .extend((0..MAX_RECORDS_PER_KEY).map(|_| record_at(Utc::now())));
    let before = tracker.get_total_cost(7);
    // 不同 Key 共用同一账号，账号条数上限裁剪的旧记录仍应补偿原 Key。
    tracker.record(
        8,
        "claude-opus-4.6".into(),
        1000,
        100,
        RecordOptions {
            credential_id: Some(31),
            credits_used: Some(0.5),
            ..Default::default()
        },
    );
    assert_eq!(tracker.records.read().len(), MAX_RECORDS_PER_KEY);
    assert_eq!(
        tracker.get_summary(7).total_requests,
        MAX_RECORDS_PER_KEY as u64
    );
    assert!((tracker.get_total_cost(7) - before).abs() < 1e-8);
    assert_eq!(
        tracker.get_summary_for_credential(31).total_requests,
        MAX_RECORDS_PER_KEY as u64 + 1
    );
}
