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

use crate::scheduler_server::SessionBuilder;
use crate::state::session_cache::{SessionCache, SessionCacheConfig};
use ballista_core::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::cluster::JobState;
use std::sync::Arc;

/// Manages DataFusion session contexts for the Ballista scheduler.
///
/// Sessions hold configuration and state for query execution.
/// Includes a bounded LRU + TTL cache to avoid repeated context creation.
#[derive(Clone)]
pub struct SessionManager {
    /// Job state storage for persisting session information.
    state: Arc<dyn JobState>,
    /// Cache for session contexts with LRU + TTL eviction.
    cache: SessionCache,
}

impl SessionManager {
    /// Creates a new `SessionManager` with the given job state backend and default cache config.
    pub fn new(state: Arc<dyn JobState>) -> Self {
        Self::with_cache_config(state, SessionCacheConfig::default())
    }

    /// Creates a new `SessionManager` with the given job state backend and cache configuration.
    pub fn with_cache_config(
        state: Arc<dyn JobState>,
        cache_config: SessionCacheConfig,
    ) -> Self {
        Self {
            state,
            cache: SessionCache::new(cache_config),
        }
    }

    /// Removes a session from the state store and cache.
    pub async fn remove_session(&self, session_id: &str) -> Result<()> {
        // Invalidate from cache first
        self.cache.invalidate(session_id).await;
        // Then remove from state
        self.state.remove_session(session_id).await
    }

    /// Creates a new session or returns a cached one with the given configuration.
    ///
    /// This method uses a cache-first approach:
    /// 1. Check if a session with matching config exists in cache
    /// 2. If cache hit and config matches, return cached context
    /// 3. If cache miss or config changed, create new context via state and cache it
    ///
    /// Returns the session context that can be used for query execution.
    pub async fn create_or_update_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> Result<Arc<SessionContext>> {
        // Try to get from cache first
        if let Some((context, true)) = self.cache.get_if_valid(session_id, config).await {
            log::debug!("Session cache hit for session_id={}", session_id);
            return Ok(context);
        }

        // Cache miss or config changed - create via state (which handles the actual creation)
        log::debug!(
            "Session cache miss for session_id={}, creating via state",
            session_id
        );
        let context = self
            .state
            .create_or_update_session(session_id, config)
            .await?;

        // Cache the newly created context
        self.cache.insert(session_id, config, context.clone()).await;

        Ok(context)
    }

    pub(crate) fn produce_config(&self) -> SessionConfig {
        self.state.produce_config()
    }

    /// Returns the current number of cached sessions.
    #[cfg(test)]
    pub fn cache_entry_count(&self) -> u64 {
        self.cache.entry_count()
    }
}

/// Creates a DataFusion session context that is compatible with Ballista configuration.
///
/// This function disables round-robin repartitioning if it was enabled, as Ballista
/// handles partitioning differently.
pub fn create_datafusion_context(
    session_config: &SessionConfig,
    session_builder: SessionBuilder,
) -> datafusion::common::Result<Arc<SessionContext>> {
    let session_state = if session_config.round_robin_repartition() {
        let session_config = session_config
            .clone()
            // should we disable catalog on the scheduler side
            .with_round_robin_repartition(false);

        log::warn!(
            "session manager will override `datafusion.optimizer.enable_round_robin_repartition` to `false` "
        );
        session_builder(session_config)?
    } else {
        session_builder(session_config.clone())?
    };

    Ok(Arc::new(SessionContext::new_with_state(session_state)))
}
