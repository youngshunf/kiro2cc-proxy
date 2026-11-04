//! 验证实时模型能力与订阅名称冲突时的真实调度行为，不发起网络请求。

use super::super::types::{MODEL_SUPPORT_TTL, ModelSupport, MultiTokenManager};
use super::sticky::tests::make_valid_cred;
use crate::model::config::Config;
use std::collections::HashSet;
use std::time::{Duration, Instant};

const OPUS: &str = "claude-opus-5.5";

fn manager(titles: &[&str], balanced: bool) -> MultiTokenManager {
    let mut config = Config::default();
    config.load_balancing_mode = if balanced { "balanced" } else { "priority" }.to_string();
    let credentials = titles
        .iter()
        .enumerate()
        .map(|(index, title)| {
            let mut credential = make_valid_cred(&format!("unit-token-{}", index + 1));
            credential.subscription_title = Some((*title).to_string());
            credential.priority = index as u32;
            credential
        })
        .collect();
    MultiTokenManager::new(config, credentials, None, None, false).unwrap()
}

fn models(manager: &MultiTokenManager, id: u64, ids: &[&str], checked_at: Instant) {
    manager.model_support.lock().insert(
        id,
        ModelSupport {
            models: Some(
                ids.iter()
                    .map(|id| id.to_ascii_lowercase())
                    .collect::<HashSet<_>>(),
            ),
            checked_at,
        },
    );
}

#[tokio::test]
async fn free_account_with_live_opus_support_is_selected() {
    let manager = manager(&["KIRO FREE"], false);
    models(&manager, 1, &[OPUS], Instant::now());
    assert_eq!(manager.acquire_context(Some(OPUS)).await.unwrap().id, 1);
}

#[tokio::test]
async fn live_support_respects_whitelist_and_preserves_sticky_binding() {
    let manager = manager(&["KIRO FREE", "KIRO PRO"], false);
    models(&manager, 1, &[OPUS], Instant::now());
    models(&manager, 2, &["claude-sonnet-4.6"], Instant::now());
    for _ in 0..2 {
        let context = manager
            .acquire_context_sticky(Some(OPUS), &[1], Some("opus-live-session"), &[])
            .await
            .unwrap();
        assert_eq!(context.id, 1);
    }
    assert!(
        manager
            .acquire_context_filtered(Some(OPUS), &[2])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn balanced_routing_uses_live_supported_free_accounts() {
    let manager = manager(&["KIRO FREE", "KIRO FREE"], true);
    models(&manager, 1, &[OPUS], Instant::now());
    models(&manager, 2, &["claude-sonnet-4.6"], Instant::now());
    for _ in 0..4 {
        assert_eq!(manager.acquire_context(Some(OPUS)).await.unwrap().id, 1);
    }
}

#[tokio::test]
async fn unknown_and_expired_support_do_not_unlock_free_accounts() {
    let manager = manager(&["KIRO FREE"], false);
    assert!(manager.acquire_context(Some(OPUS)).await.is_err());
    let stale = Instant::now()
        .checked_sub(MODEL_SUPPORT_TTL + Duration::from_secs(1))
        .unwrap();
    models(&manager, 1, &[OPUS], stale);
    assert!(manager.acquire_context(Some(OPUS)).await.is_err());
}

#[tokio::test]
async fn paid_account_with_unknown_support_keeps_existing_behavior() {
    let manager = manager(&["KIRO PRO"], false);
    assert_eq!(manager.acquire_context(Some(OPUS)).await.unwrap().id, 1);
}

#[tokio::test]
async fn explicit_live_unsupported_result_overrides_paid_subscription() {
    let manager = manager(&["KIRO PRO"], false);
    models(&manager, 1, &["claude-sonnet-4.6"], Instant::now());
    assert!(manager.acquire_context(Some(OPUS)).await.is_err());
}

#[tokio::test]
async fn live_support_does_not_bypass_disabled_account() {
    let manager = manager(&["KIRO FREE"], false);
    models(&manager, 1, &[OPUS], Instant::now());
    manager.entries.lock()[0].disabled = true;
    assert!(manager.acquire_context(Some(OPUS)).await.is_err());
}

#[test]
fn single_account_pool_is_probed_and_fresh_capabilities_are_reused() {
    let manager = manager(&["KIRO FREE"], false);
    assert_eq!(manager.model_support_probe_targets(&[]), vec![1]);
    models(&manager, 1, &[OPUS], Instant::now());
    assert!(manager.model_support_probe_targets(&[]).is_empty());
    assert!(manager.model_support_probe_targets(&[2]).is_empty());
}

#[test]
fn unavailable_reason_uses_the_same_live_capability_rule() {
    let manager = manager(&["KIRO FREE"], false);
    models(&manager, 1, &[OPUS], Instant::now());
    let message = manager.describe_unavailable(Some(OPUS), &[]);
    assert!(!message.contains("没有支持该模型"), "{message}");
}
