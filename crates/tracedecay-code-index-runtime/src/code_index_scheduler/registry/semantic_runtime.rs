//! Semantic-runtime installation and lookup for mounted scopes.

use std::{path::Path, sync::Arc};

use super::{CodeIndexSchedulerRegistryV1, unique_mounted_for_scope};
use crate::code_index_scheduler::CodeIndexSchedulerErrorV1;

impl CodeIndexSchedulerRegistryV1 {
    /// Attach one semantic lifecycle/query owner to its exact mounted
    /// checkout. A second distinct owner cannot replace a live route.
    pub async fn mount_semantic_runtime(
        &self,
        project_root: &Path,
        scope: &tracedecay_contracts::ResolvedScope,
        runtime: Arc<tracedecay_application::semantic_runtime::ProjectSemanticRuntimeV1>,
    ) -> Result<(), CodeIndexSchedulerErrorV1> {
        scope
            .validate()
            .map_err(|error| CodeIndexSchedulerErrorV1::Identity(error.to_string()))?;
        let project_root = project_root.canonicalize()?;
        let mut mounted = self.mounted.lock().await;
        let worktree = mounted.get_mut(&project_root).ok_or_else(|| {
            CodeIndexSchedulerErrorV1::Identity(
                "cannot mount semantic runtime before its worktree".to_owned(),
            )
        })?;
        if worktree.project_id != scope.project_id
            || worktree.repository_id != scope.repository_id
            || worktree.worktree_id != scope.worktree_id
        {
            return Err(CodeIndexSchedulerErrorV1::Identity(
                "semantic runtime scope does not match the mounted worktree".to_owned(),
            ));
        }
        if let Some(incumbent) = worktree.semantic_runtime.as_ref() {
            if Arc::ptr_eq(incumbent, &runtime) {
                return Ok(());
            }
            return Err(CodeIndexSchedulerErrorV1::Identity(
                "a different semantic runtime already owns this worktree".to_owned(),
            ));
        }
        worktree.semantic_runtime = Some(runtime);
        Ok(())
    }

    /// Return the semantic owner mounted for one exact physical checkout.
    /// Reference labels may move without changing checkout identity.
    pub async fn semantic_runtime_for_scope(
        &self,
        scope: &tracedecay_contracts::ResolvedScope,
    ) -> Option<Arc<tracedecay_application::semantic_runtime::ProjectSemanticRuntimeV1>> {
        self.activate_for_scope(scope);
        let mounted = self.mounted.lock().await;
        unique_mounted_for_scope(&mounted, scope)
            .unique()
            .and_then(|(_, worktree)| worktree.semantic_runtime.as_ref().map(Arc::clone))
    }

    /// Remove a route's semantic owner only when it is still the installed
    /// instance. A remount that raced retirement remains untouched.
    pub async fn unmount_semantic_runtime_if_current(
        &self,
        project_root: &Path,
        scope: &tracedecay_contracts::ResolvedScope,
        expected: &Arc<tracedecay_application::semantic_runtime::ProjectSemanticRuntimeV1>,
    ) -> bool {
        let Ok(project_root) = project_root.canonicalize() else {
            return false;
        };
        let mut mounted = self.mounted.lock().await;
        let Some(worktree) = mounted.get_mut(&project_root) else {
            return false;
        };
        if worktree.project_id != scope.project_id
            || worktree.repository_id != scope.repository_id
            || worktree.worktree_id != scope.worktree_id
            || worktree
                .semantic_runtime
                .as_ref()
                .is_none_or(|runtime| !Arc::ptr_eq(runtime, expected))
        {
            return false;
        }
        worktree.semantic_runtime = None;
        true
    }
}
