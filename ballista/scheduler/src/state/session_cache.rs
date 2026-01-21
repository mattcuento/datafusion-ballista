// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Session context caching for the Ballista scheduler.
//!
//! This module provides a bounded LRU + TTL cache for `SessionContext` instances
//! to improve performance by avoiding repeated context creation for the same session.

use datafusion::prelude::{SessionConfig, SessionContext};
use moka::future::Cache;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the session cache.
#[derive(Debug, Clone)]
pub struct SessionCacheConfig {
    /// Maximum number of sessions to cache.
    pub max_entries: u64,
    /// Time-to-live for cached sessions.
    pub ttl: Duration,
    /// Time-to-idle - sessions not accessed within this time are evicted.
    pub tti: Duration,
}

impl Default for SessionCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            ttl: Duration::from_secs(3600),
            tti: Duration::from_secs(1800),
        }
    }
}

impl SessionCacheConfig {
    /// Creates a new `SessionCacheConfig` with the given parameters.
    pub fn new(max_entries: u64, ttl_seconds: u64, tti_seconds: u64) -> Self {
        Self {
            max_entries,
            ttl: Duration::from_secs(ttl_seconds),
            tti: Duration::from_secs(tti_seconds),
        }
    }
}

/// A cached session context with its configuration hash.
///
/// The config hash is used to detect when the configuration has changed,
/// which requires invalidating and recreating the session.
#[derive(Clone)]
struct CachedSession {
    context: Arc<SessionContext>,
    config_hash: u64,
}

/// Computes a hash of the session configuration for change detection.
fn compute_config_hash(config: &SessionConfig) -> u64 {
    let mut hasher = DefaultHasher::new();
    // Hash the debug representation of the config options
    // ConfigOptions doesn't implement Hash, so we use its string representation
    let config_str = format!("{:?}", config.options());
    config_str.hash(&mut hasher);
    hasher.finish()
}

/// A bounded cache for `SessionContext` instances with LRU + TTL eviction.
///
/// This cache helps avoid the cost of creating new `SessionContext` instances
/// for every query while ensuring bounded memory usage through:
/// - Maximum entry limit (LRU eviction when full)
/// - Time-to-live (TTL) - sessions expire after a fixed time
/// - Time-to-idle (TTI) - sessions expire after being idle
#[derive(Clone)]
pub struct SessionCache {
    cache: Cache<String, CachedSession>,
}

impl SessionCache {
    /// Creates a new `SessionCache` with the given configuration.
    pub fn new(config: SessionCacheConfig) -> Self {
        let cache = Cache::builder()
            .max_capacity(config.max_entries)
            .time_to_live(config.ttl)
            .time_to_idle(config.tti)
            .build();

        Self { cache }
    }

    /// Gets a cached session if it exists and its configuration matches.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The unique session identifier
    /// * `config` - The expected session configuration
    ///
    /// # Returns
    ///
    /// `Some((context, true))` if cache hit with matching config,
    /// `Some((context, false))` if cached but config mismatch (invalidates entry),
    /// `None` if not in cache.
    pub async fn get_if_valid(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> Option<(Arc<SessionContext>, bool)> {
        let config_hash = compute_config_hash(config);

        if let Some(cached) = self.cache.get(session_id).await {
            if cached.config_hash == config_hash {
                return Some((cached.context, true));
            }
            // Config changed, invalidate the old entry
            log::debug!(
                "Session config changed for session_id={}, invalidating cache",
                session_id
            );
            self.cache.invalidate(session_id).await;
        }

        None
    }

    /// Inserts a session context into the cache.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The unique session identifier
    /// * `config` - The session configuration (used for hash computation)
    /// * `context` - The session context to cache
    pub async fn insert(
        &self,
        session_id: &str,
        config: &SessionConfig,
        context: Arc<SessionContext>,
    ) {
        let config_hash = compute_config_hash(config);
        let cached = CachedSession {
            context,
            config_hash,
        };
        self.cache.insert(session_id.to_string(), cached).await;
    }

    /// Invalidates (removes) a session from the cache.
    pub async fn invalidate(&self, session_id: &str) {
        log::debug!("Invalidating cached session: {}", session_id);
        self.cache.invalidate(session_id).await;
    }

    /// Returns the current number of entries in the cache.
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Runs pending maintenance tasks (eviction, etc.).
    ///
    /// This is normally handled automatically, but can be called explicitly
    /// for testing purposes.
    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionConfig;
    use std::time::Duration;

    fn create_test_context() -> Arc<SessionContext> {
        Arc::new(SessionContext::new())
    }

    #[tokio::test]
    async fn test_cache_hit() {
        let config = SessionCacheConfig::new(100, 3600, 1800);
        let cache = SessionCache::new(config);
        let session_config = SessionConfig::new();

        // First call should be a miss
        let result1 = cache.get_if_valid("session1", &session_config).await;
        assert!(result1.is_none(), "First call should be a cache miss");

        // Insert a context
        let ctx1 = create_test_context();
        cache
            .insert("session1", &session_config, ctx1.clone())
            .await;

        // Second call with same config should be a hit
        let result2 = cache.get_if_valid("session1", &session_config).await;
        assert!(result2.is_some(), "Second call should be a cache hit");
        let (ctx2, valid) = result2.unwrap();
        assert!(valid);

        // Should return the same context
        assert!(Arc::ptr_eq(&ctx1, &ctx2));
    }

    #[tokio::test]
    async fn test_cache_miss_different_session() {
        let config = SessionCacheConfig::new(100, 3600, 1800);
        let cache = SessionCache::new(config);
        let session_config = SessionConfig::new();

        // Insert session1
        cache
            .insert("session1", &session_config, create_test_context())
            .await;

        // Different session should be a miss
        let result = cache.get_if_valid("session2", &session_config).await;
        assert!(result.is_none(), "Different session should be a cache miss");

        // Insert session2
        cache
            .insert("session2", &session_config, create_test_context())
            .await;

        cache.run_pending_tasks().await;
        assert_eq!(cache.entry_count(), 2);
    }

    #[tokio::test]
    async fn test_config_change_invalidation() {
        let config = SessionCacheConfig::new(100, 3600, 1800);
        let cache = SessionCache::new(config);

        let config1 = SessionConfig::new().with_target_partitions(4);
        let config2 = SessionConfig::new().with_target_partitions(8);

        // Insert with config1
        let ctx1 = create_test_context();
        cache.insert("session1", &config1, ctx1.clone()).await;

        // Same config should hit
        let result = cache.get_if_valid("session1", &config1).await;
        assert!(result.is_some());
        let (ctx2, valid) = result.unwrap();
        assert!(valid);
        assert!(Arc::ptr_eq(&ctx1, &ctx2));

        // Different config should miss (and invalidate)
        let result = cache.get_if_valid("session1", &config2).await;
        assert!(result.is_none(), "Changed config should invalidate cache");

        // Insert with new config
        let ctx3 = create_test_context();
        cache.insert("session1", &config2, ctx3.clone()).await;

        // Should now hit with new config
        let result = cache.get_if_valid("session1", &config2).await;
        assert!(result.is_some());
        assert!(!Arc::ptr_eq(&ctx1, &result.unwrap().0));
    }

    #[tokio::test]
    async fn test_explicit_invalidation() {
        let config = SessionCacheConfig::new(100, 3600, 1800);
        let cache = SessionCache::new(config);
        let session_config = SessionConfig::new();

        // Insert session
        let ctx1 = create_test_context();
        cache
            .insert("session1", &session_config, ctx1.clone())
            .await;

        // Verify it's cached
        let result = cache.get_if_valid("session1", &session_config).await;
        assert!(result.is_some());

        // Invalidate
        cache.invalidate("session1").await;
        cache.run_pending_tasks().await;

        // Should be a miss now
        let result = cache.get_if_valid("session1", &session_config).await;
        assert!(
            result.is_none(),
            "After invalidation should be a cache miss"
        );
    }

    #[tokio::test]
    async fn test_lru_eviction() {
        // Small cache with max 2 entries
        let config = SessionCacheConfig::new(2, 3600, 1800);
        let cache = SessionCache::new(config);
        let session_config = SessionConfig::new();

        // Fill cache with 2 entries
        cache
            .insert("session1", &session_config, create_test_context())
            .await;
        cache
            .insert("session2", &session_config, create_test_context())
            .await;

        // Run maintenance to ensure counts are updated
        cache.run_pending_tasks().await;
        assert_eq!(cache.entry_count(), 2);

        // Add third entry, should trigger eviction
        cache
            .insert("session3", &session_config, create_test_context())
            .await;

        // Run maintenance to process eviction
        cache.run_pending_tasks().await;

        // Cache should still have max 2 entries
        assert!(cache.entry_count() <= 2);
    }

    #[tokio::test]
    async fn test_tti_eviction() {
        // Cache with 100ms TTI for testing
        let config = SessionCacheConfig {
            max_entries: 100,
            ttl: Duration::from_secs(3600),
            tti: Duration::from_millis(100),
        };
        let cache = SessionCache::new(config);
        let session_config = SessionConfig::new();

        // Insert session
        cache
            .insert("session1", &session_config, create_test_context())
            .await;

        // Wait for TTI to expire
        tokio::time::sleep(Duration::from_millis(150)).await;
        cache.run_pending_tasks().await;

        // Should be evicted due to TTI
        let result = cache.get_if_valid("session1", &session_config).await;
        assert!(result.is_none(), "Session should be evicted after TTI");
    }

    #[tokio::test]
    async fn test_ttl_eviction() {
        // Cache with 100ms TTL for testing
        let config = SessionCacheConfig {
            max_entries: 100,
            ttl: Duration::from_millis(100),
            tti: Duration::from_secs(3600),
        };
        let cache = SessionCache::new(config);
        let session_config = SessionConfig::new();

        // Insert session
        cache
            .insert("session1", &session_config, create_test_context())
            .await;

        // Access it to reset TTI but not TTL
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = cache.get_if_valid("session1", &session_config).await;
        assert!(
            result.is_some(),
            "Should still be cached before TTL expires"
        );

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(100)).await;
        cache.run_pending_tasks().await;

        // Should be evicted due to TTL
        let result = cache.get_if_valid("session1", &session_config).await;
        assert!(result.is_none(), "Session should be evicted after TTL");
    }
}
