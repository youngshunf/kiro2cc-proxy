// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! API Key 用量追踪模块
//!
//! 记录每个 API Key 的请求用量（input/output tokens），并根据模型定价估算费用。
//! 数据持久化到 `api_key_usage.json`。

use crate::common::fs::atomic_write;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;

/// 单条用量记录
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageRecord {
    /// API Key ID（0 = 主密钥）
    pub api_key_id: u32,
    /// 账号 ID（None 表示旧数据或未知）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<u64>,
    /// 模型名称
    pub model: String,
    /// 输入 tokens
    pub input_tokens: i32,
    /// 输出 tokens
    pub output_tokens: i32,
    /// 估算费用（美元）
    pub estimated_cost: f64,
    /// 真实 credits 消耗（来自 meteringEvent，None 表示旧数据）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credits_used: Option<f64>,
    /// 缓存命中的输入 token 数（来自 meteringEvent 或反推，None 表示旧数据）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<i32>,
    /// 缓存创建的输入 token 数（来自 meteringEvent，None 表示旧数据）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<i32>,
    /// 5m ephemeral tier 的 cache_creation 拆分（默认 0，向后兼容）
    #[serde(default)]
    pub cache_creation_5m_input_tokens: i32,
    /// 1h ephemeral tier 的 cache_creation 拆分（默认 0，向后兼容）
    #[serde(default)]
    pub cache_creation_1h_input_tokens: i32,
    /// 记录时间
    pub created_at: DateTime<Utc>,
    /// 客户端 IP（None 表示旧数据或未知）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
    /// 请求的 effort 级别（low/medium/high/xhigh/max；None 表示旧数据或客户端未传 output_config）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    /// 首 token 延迟（请求开始到首个上游业务数据事件，毫秒；None 表示旧数据或非流式请求）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_token_ms: Option<u64>,
    /// 服务端处理总耗时（请求开始到落库前，毫秒；None 表示旧数据）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// 深度思考文案 token 数（客户端实际收到的 thinking 内容；None 表示旧数据
    /// 或该请求未产生 thinking 内容）。流式与非流式均为 BPE 真值（count_tokens），
    /// 与客户端「Thought for Ns (N tokens)」显示同源同口径
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_token_count: Option<i32>,
    /// 请求类型（true = 流式 SSE，false = 非流式；None 表示旧数据）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_stream: Option<bool>,
    /// 客户端 User-Agent（截断至 256 字符、最坏 1024 字节；None 表示旧数据或客户端未携带）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
}

/// `record()` 的可选参数集合：位置参数已达 14 个且多次追加（thinking/is_stream），
/// 收拢为具名结构体后调用点尾部不再出现裸 None/Some 字面量，新增字段也不会
/// 引发全量调用点改动（此前 thinking 字段追加时已三次被迫改动全部调用点）。
///
/// 生产调用点必须显式设置 is_stream——Default 的 None 仅表示「旧数据」语义，
/// 漏设会与新数据混淆；thinking_token_count 的 None 容忍度更高（未产生思考）。
#[derive(Debug, Clone, Default)]
pub struct RecordOptions {
    pub credential_id: Option<u64>,
    pub client_ip: Option<String>,
    pub credits_used: Option<f64>,
    pub cache_read_input_tokens: Option<i32>,
    pub cache_creation_input_tokens: Option<i32>,
    pub effort: Option<String>,
    pub first_token_ms: Option<u64>,
    pub duration_ms: Option<u64>,
    pub thinking_token_count: Option<i32>,
    pub is_stream: Option<bool>,
    pub user_agent: Option<String>,
}

/// 单个 API Key 的用量汇总
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageSummary {
    /// API Key ID
    pub api_key_id: u32,
    /// 总请求次数
    pub total_requests: u64,
    /// 总输入 tokens
    pub total_input_tokens: i64,
    /// 总输出 tokens
    pub total_output_tokens: i64,
    /// 总估算费用（美元）
    pub total_cost: f64,
    /// 累计真实 credits 消耗（旧记录按 estimated_cost * k_ref 回退估算）
    pub total_credits: f64,
    /// 节省的 credits 总量（仅含有 credits_used 的记录）
    pub total_credits_saved: f64,
    /// 按模型分组的用量
    pub by_model: Vec<ModelUsage>,
}

/// 按模型分组的用量
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelUsage {
    pub model: String,
    pub requests: u64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cost: f64,
    /// 累计真实 credits 消耗（旧记录按 estimated_cost * k_ref 回退估算）
    pub credits: f64,
    /// 节省的 credits 总量（仅含有 credits_used 的记录）
    pub credits_saved: f64,
}
/// 每个 API Key / 账号的最大日志条数，超出时删除最老的记录
const MAX_RECORDS_PER_KEY: usize = 10_000;
/// 明细保留最近 30 天，累计用量不随清理减少。
const RETENTION_DAYS: i64 = 30;
/// 测试专用再导出（tests.rs 为独立文件，需经模块路径访问私有常量）
#[cfg(test)]
pub(crate) const MAX_RECORDS_PER_KEY_FOR_TEST: usize = MAX_RECORDS_PER_KEY;

/// 生命周期累计计数的持久化结构（按 api_key_id 与 credential_id 分组）
///
/// 明细记录受 `MAX_RECORDS_PER_KEY` 裁剪，直接数记录条数会在超过 1 万次后
/// 封顶不再增长（历史缺陷：列表页请求数永远显示 10,000）。裁剪发生时把被
/// 删记录的贡献累加进此处的「裁剪前基数」，使
/// `裁剪前基数 + 现存记录求和` 恒等于真实累计值。
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct LifetimeTotals {
    /// 裁剪掉的记录贡献的请求数（已从明细缓冲删除，只能从这里取）
    pruned_requests: u64,
    /// 裁剪掉的记录贡献的输入 tokens
    pruned_input_tokens: i64,
    /// 裁剪掉的记录贡献的输出 tokens
    pruned_output_tokens: i64,
    pruned_cost: f64,
    pruned_credits: f64,
    pruned_credits_saved: f64,
    pruned_cache_read_tokens: u64,
    pruned_positive_input_tokens: u64,
    pruned_positive_output_tokens: u64,
    by_model: HashMap<String, ModelUsage>,
}

impl LifetimeTotals {
    fn add_record(&mut self, record: &UsageRecord) {
        let credits = record
            .credits_used
            .unwrap_or_else(|| record.estimated_cost * get_k_ref(&record.model));
        let saved = record.credits_used.map_or(0.0, |cu| {
            record.estimated_cost * get_k_ref(&record.model) - cu
        });
        self.pruned_requests += 1;
        self.pruned_input_tokens += i64::from(record.input_tokens);
        self.pruned_output_tokens += i64::from(record.output_tokens);
        self.pruned_positive_input_tokens += record.input_tokens.max(0) as u64;
        self.pruned_positive_output_tokens += record.output_tokens.max(0) as u64;
        self.pruned_cost += record.estimated_cost;
        self.pruned_credits += credits;
        self.pruned_credits_saved += saved;
        self.pruned_cache_read_tokens += record.cache_read_input_tokens.unwrap_or(0).max(0) as u64;
        let model = self
            .by_model
            .entry(record.model.clone())
            .or_insert_with(|| ModelUsage {
                model: record.model.clone(),
                requests: 0,
                input_tokens: 0,
                output_tokens: 0,
                cost: 0.0,
                credits: 0.0,
                credits_saved: 0.0,
            });
        model.requests += 1;
        model.input_tokens += i64::from(record.input_tokens);
        model.output_tokens += i64::from(record.output_tokens);
        model.cost += record.estimated_cost;
        model.credits += credits;
        model.credits_saved += saved;
    }
}

/// 明细和累计基数必须来自同一次快照，避免两份文件分别成功导致重复或漏计。
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UsageSnapshot {
    format_version: u32,
    records: Vec<UsageRecord>,
    lifetime_keys: HashMap<u32, LifetimeTotals>,
    lifetime_credentials: HashMap<u64, LifetimeTotals>,
}

enum UsageData {
    Legacy(Vec<UsageRecord>),
    Snapshot(UsageSnapshot),
}

type LifetimeMap<T> = Arc<RwLock<HashMap<T, LifetimeTotals>>>;

/// 用量追踪器（线程安全）
pub struct UsageTracker {
    pub(crate) records: Arc<RwLock<Vec<UsageRecord>>>,
    pub(crate) dirty_tx: mpsc::UnboundedSender<()>,
    lifetime_keys: LifetimeMap<u32>,
    lifetime_credentials: LifetimeMap<u64>,
}

impl UsageTracker {
    /// 读取新快照或旧数组，旧累计文件只在迁移时读取，损坏数据显式报错。
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let data = match fs::read_to_string(&path) {
            Ok(content) if !content.trim().is_empty() => {
                if content.trim_start().starts_with('[') {
                    UsageData::Legacy(serde_json::from_str(&content)?)
                } else {
                    UsageData::Snapshot(serde_json::from_str(&content)?)
                }
            }
            Ok(_) => UsageData::Legacy(Vec::new()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => UsageData::Legacy(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let (records, lifetime_keys, lifetime_credentials, migrating) = match data {
            UsageData::Snapshot(snapshot) => {
                anyhow::ensure!(
                    snapshot.format_version == 1,
                    "不支持的用量快照版本: {}",
                    snapshot.format_version
                );
                (
                    snapshot.records,
                    snapshot.lifetime_keys,
                    snapshot.lifetime_credentials,
                    false,
                )
            }
            UsageData::Legacy(records) => {
                let lifetime_path = path.with_file_name("api_key_lifetime.json");
                let keys = match fs::read_to_string(&lifetime_path) {
                    Ok(content) => serde_json::from_str(&content)?,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
                    Err(e) => return Err(e.into()),
                };
                (records, keys, HashMap::new(), true)
            }
        };
        let records = Arc::new(RwLock::new(records));
        let lifetime_keys = Arc::new(RwLock::new(lifetime_keys));
        let lifetime_credentials = Arc::new(RwLock::new(lifetime_credentials));
        let removed =
            Self::prune_expired(&records, &lifetime_keys, &lifetime_credentials, Utc::now());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let records_task = records.clone();
        let keys_task = lifetime_keys.clone();
        let credentials_task = lifetime_credentials.clone();
        tokio::spawn(async move {
            let mut dirty = migrating || removed > 0;
            let mut flush = tokio::time::interval(std::time::Duration::from_secs(5));
            let mut maintenance = tokio::time::interval(std::time::Duration::from_secs(3600));
            loop {
                tokio::select! {
                    res = rx.recv() => match res {
                        Some(()) => dirty = true,
                        None => {
                            if dirty && let Err(e) = Self::save_internal(&records_task, &keys_task, &credentials_task, &path).await {
                                tracing::error!("退出时保存用量快照失败: {}", e);
                            }
                            break;
                        }
                    },
                    _ = maintenance.tick() => {
                        dirty |= Self::prune_expired(&records_task, &keys_task, &credentials_task, Utc::now()) > 0;
                    }
                    _ = flush.tick() => {
                        if dirty {
                            match Self::save_internal(&records_task, &keys_task, &credentials_task, &path).await {
                                Ok(()) => dirty = false,
                                Err(e) => tracing::warn!("保存用量快照失败，下一周期重试: {}", e),
                            }
                        }
                    }
                }
            }
        });
        Ok(Self {
            records,
            dirty_tx: tx,
            lifetime_keys,
            lifetime_credentials,
        })
    }

    /// 顺序固定为明细、Key 基数、账号基数，清理和保存共用这一锁序。
    fn retain_records(
        records: &mut Vec<UsageRecord>,
        keys: &mut HashMap<u32, LifetimeTotals>,
        credentials: &mut HashMap<u64, LifetimeTotals>,
        mut keep: impl FnMut(&UsageRecord) -> bool,
    ) -> usize {
        let before = records.len();
        records.retain(|record| {
            if keep(record) {
                return true;
            }
            keys.entry(record.api_key_id)
                .or_default()
                .add_record(record);
            if let Some(id) = record.credential_id {
                credentials.entry(id).or_default().add_record(record);
            }
            false
        });
        before - records.len()
    }

    fn prune_expired(
        records: &Arc<RwLock<Vec<UsageRecord>>>,
        keys: &LifetimeMap<u32>,
        credentials: &LifetimeMap<u64>,
        now: DateTime<Utc>,
    ) -> usize {
        let cutoff = now - chrono::Duration::days(RETENTION_DAYS);
        let removed = Self::retain_records(
            &mut records.write(),
            &mut keys.write(),
            &mut credentials.write(),
            |r| r.created_at >= cutoff,
        );
        if removed > 0 {
            tracing::info!(
                removed,
                retention_days = RETENTION_DAYS,
                "已清理过期用量明细，累计用量保留"
            );
        }
        removed
    }

    /// 整体原子替换快照；被中断时旧快照仍然完整，未完成临时文件不参与加载。
    async fn save_internal(
        records: &Arc<RwLock<Vec<UsageRecord>>>,
        keys: &LifetimeMap<u32>,
        credentials: &LifetimeMap<u64>,
        file_path: &Path,
    ) -> anyhow::Result<()> {
        let content = {
            let records = records.read();
            let keys = keys.read();
            let credentials = credentials.read();
            serde_json::to_vec(&UsageSnapshot {
                format_version: 1,
                records: records.clone(),
                lifetime_keys: keys.clone(),
                lifetime_credentials: credentials.clone(),
            })?
        };
        let path = file_path.to_path_buf();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            atomic_write(&path, &content)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// 记录一次请求用量
    pub fn record(
        &self,
        api_key_id: u32,
        model: String,
        input_tokens: i32,
        output_tokens: i32,
        options: RecordOptions,
    ) {
        let RecordOptions {
            credential_id,
            client_ip,
            credits_used,
            cache_read_input_tokens,
            cache_creation_input_tokens,
            effort,
            first_token_ms,
            duration_ms,
            thinking_token_count,
            is_stream,
            user_agent,
        } = options;
        let cost = calculate_cost(&model, input_tokens, output_tokens);
        let record = UsageRecord {
            api_key_id,
            credential_id,
            model,
            input_tokens,
            output_tokens,
            estimated_cost: cost,
            credits_used,
            cache_read_input_tokens,
            cache_creation_input_tokens,
            cache_creation_5m_input_tokens: 0,
            cache_creation_1h_input_tokens: 0,
            created_at: Utc::now(),
            client_ip,
            effort,
            first_token_ms,
            duration_ms,
            thinking_token_count,
            is_stream,
            user_agent,
        };
        {
            let mut records = self.records.write();
            records.push(record);

            let mut keys = self.lifetime_keys.write();
            let mut credentials = self.lifetime_credentials.write();
            // 数量上限与时间上限共用补偿逻辑，任何维度裁剪都保留累计与额度。
            let mut excess = records
                .iter()
                .filter(|r| r.api_key_id == api_key_id)
                .count()
                .saturating_sub(MAX_RECORDS_PER_KEY);
            Self::retain_records(&mut records, &mut keys, &mut credentials, |r| {
                if r.api_key_id == api_key_id && excess > 0 {
                    excess -= 1;
                    false
                } else {
                    true
                }
            });
            if let Some(cid) = credential_id {
                let mut excess = records
                    .iter()
                    .filter(|r| r.credential_id == Some(cid))
                    .count()
                    .saturating_sub(MAX_RECORDS_PER_KEY);
                Self::retain_records(&mut records, &mut keys, &mut credentials, |r| {
                    if r.credential_id == Some(cid) && excess > 0 {
                        excess -= 1;
                        false
                    } else {
                        true
                    }
                });
            }
        }
        let _ = self.dirty_tx.send(());
    }
    /// 获取单个 API Key 的用量汇总
    pub fn get_summary(&self, api_key_id: u32) -> UsageSummary {
        let records = self.records.read();
        let filtered: Vec<&UsageRecord> = records
            .iter()
            .filter(|r| r.api_key_id == api_key_id)
            .collect();

        let mut by_model: HashMap<String, (u64, i64, i64, f64, f64, f64)> = HashMap::new();
        for r in &filtered {
            let credits = r
                .credits_used
                .unwrap_or_else(|| r.estimated_cost * get_k_ref(&r.model));
            let credits_saved = r
                .credits_used
                .map(|cu| r.estimated_cost * get_k_ref(&r.model) - cu)
                .unwrap_or(0.0);
            let entry = by_model.entry(r.model.clone()).or_default();
            entry.0 += 1;
            entry.1 += r.input_tokens as i64;
            entry.2 += r.output_tokens as i64;
            entry.3 += r.estimated_cost;
            entry.4 += credits;
            entry.5 += credits_saved;
        }

        let total_credits_saved: f64 = filtered
            .iter()
            .filter_map(|r| {
                r.credits_used
                    .map(|cu| r.estimated_cost * get_k_ref(&r.model) - cu)
            })
            .sum();

        let total_credits: f64 = filtered
            .iter()
            .map(|r| {
                r.credits_used
                    .unwrap_or_else(|| r.estimated_cost * get_k_ref(&r.model))
            })
            .sum();

        let lifetime = self
            .lifetime_keys
            .read()
            .get(&api_key_id)
            .cloned()
            .unwrap_or_default();
        for model in lifetime.by_model.values() {
            let entry = by_model.entry(model.model.clone()).or_default();
            entry.0 += model.requests;
            entry.1 += model.input_tokens;
            entry.2 += model.output_tokens;
            entry.3 += model.cost;
            entry.4 += model.credits;
            entry.5 += model.credits_saved;
        }
        UsageSummary {
            api_key_id,
            total_requests: lifetime.pruned_requests + filtered.len() as u64,
            total_input_tokens: lifetime.pruned_input_tokens
                + filtered
                    .iter()
                    .map(|r| i64::from(r.input_tokens))
                    .sum::<i64>(),
            total_output_tokens: lifetime.pruned_output_tokens
                + filtered
                    .iter()
                    .map(|r| i64::from(r.output_tokens))
                    .sum::<i64>(),
            total_cost: lifetime.pruned_cost
                + filtered.iter().map(|r| r.estimated_cost).sum::<f64>(),
            total_credits: lifetime.pruned_credits + total_credits,
            total_credits_saved: lifetime.pruned_credits_saved + total_credits_saved,
            by_model: by_model
                .into_iter()
                .map(
                    |(model, (requests, input, output, cost, credits, credits_saved))| ModelUsage {
                        model,
                        requests,
                        input_tokens: input,
                        output_tokens: output,
                        cost,
                        credits,
                        credits_saved,
                    },
                )
                .collect(),
        }
    }

    /// 获取所有 API Key 的用量概览
    pub fn get_all_summaries(&self) -> Vec<UsageSummary> {
        // 枚举明细与累计基数的 ID 并集：仅剩基数（明细被全部裁剪）的 Key
        // 也要出现在概览中，否则管理 UI 会把该 Key 显示为 0
        let mut key_ids: Vec<u32> = {
            let records = self.records.read();
            let lifetime = self.lifetime_keys.read();
            let mut ids: Vec<u32> = records.iter().map(|r| r.api_key_id).collect();
            ids.extend(lifetime.keys().copied());
            ids
        };
        key_ids.sort();
        key_ids.dedup();

        key_ids.iter().map(|&id| self.get_summary(id)).collect()
    }

    /// 历史用量记录中出现过的最大 API Key ID
    ///
    /// 用于 API Key ID 计数器的种子：计数器文件首次不存在时（所有存量部署），
    /// `ApiKeyManager::load` 只能按当前 key 列表推算，会漏掉已删除 key 曾用过的
    /// 高位 id，导致新 key 复用后继承其用量与累计消费额。
    pub fn max_api_key_id(&self) -> u32 {
        self.records
            .read()
            .iter()
            .map(|r| r.api_key_id)
            .chain(self.lifetime_keys.read().keys().copied())
            .max()
            .unwrap_or(0)
    }

    /// 重置指定 API Key 的用量记录
    pub fn reset(&self, api_key_id: u32) -> anyhow::Result<()> {
        // 与 record 一致的锁顺序：保持明细写锁直至基数清零，避免并发查询
        // 在两段之间看到「仅剩基数」的不一致中间态（如 10,007 → 7 → 0）
        let mut records = self.records.write();
        records.retain(|r| r.api_key_id != api_key_id);
        // 基数同步清零，否则重置后请求数会直接回到重置前的累计值
        self.lifetime_keys.write().remove(&api_key_id);
        drop(records);
        let _ = self.dirty_tx.send(());
        Ok(())
    }

    /// 获取指定 API Key 的累计费用（轻量版，仅算总费用）
    pub fn get_total_cost(&self, api_key_id: u32) -> f64 {
        let records = self.records.read();
        records
            .iter()
            .filter(|r| r.api_key_id == api_key_id)
            .map(|r| r.estimated_cost)
            .sum::<f64>()
            + self
                .lifetime_keys
                .read()
                .get(&api_key_id)
                .map_or(0.0, |t| t.pruned_cost)
    }

    /// 获取指定 API Key 的累计真实 credits 消耗（轻量版）
    /// 旧记录无 credits_used 时按 estimated_cost * k_ref 回退估算（与日报汇总口径一致）
    pub fn get_total_credits(&self, api_key_id: u32) -> f64 {
        let records = self.records.read();
        records
            .iter()
            .filter(|r| r.api_key_id == api_key_id)
            .map(|r| {
                r.credits_used
                    .unwrap_or_else(|| r.estimated_cost * get_k_ref(&r.model))
            })
            .sum::<f64>()
            + self
                .lifetime_keys
                .read()
                .get(&api_key_id)
                .map_or(0.0, |t| t.pruned_credits)
    }
}

mod daily;
mod pagination;
mod pricing;

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "../../../tests/usage/retention.rs"]
mod retention_tests;

#[allow(unused_imports)] // 类型归属 daily.rs，经 mod 再导出保持原模块路径
pub(crate) use daily::{CredentialDaySummary, CredentialRangeSummary, DailySummary};
#[allow(unused_imports)]
pub(crate) use pagination::{UsageRecordItem, UsageRecordsPage};
#[allow(unused_imports)]
pub(crate) use pricing::{
    MIN_INFER_INPUT_TOKENS, ModelPricing, get_model_pricing, infer_cache_read_from_credits,
};
pub(crate) use pricing::{calculate_cost, get_k_ref, refine_prefix_cache_read};
#[cfg(test)]
pub(crate) use pricing::{credits_for_cache_read, get_claude_credit_rates};
