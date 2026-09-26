//! The app-supplied chain of schema versions.

use std::collections::HashSet;

use super::{error::MigrationError, snapshot::StoreSnapshot};

/// One forward step. `from_version` is the schema version the step reads.
///
/// Steps edit [`StoreSnapshot`] with literal storage names. They do not call
/// live `T::name()` helpers, because a rename is the thing the step is doing.
pub trait MigrationStep: Send + Sync {
    fn from_version(&self) -> u32;
    fn id(&self) -> &'static str;
    fn apply(&self, store: &mut StoreSnapshot) -> Result<(), MigrationError>;
}

/// Versions an app knows how to migrate, plus the embedded schema lock.
///
/// The current version is [`Self::starting_at`] plus the number of steps.
/// An app that never builds a history keeps today's behavior: nothing in the
/// library reads a schema document unless [`crate::migrate_store`] is called.
pub struct SchemaHistory {
    starting_at: u32,
    steps: Vec<Box<dyn MigrationStep>>,
    lock_source: Option<String>,
}

impl SchemaHistory {
    /// Oldest schema version this binary still accepts.
    ///
    /// `0` is the unstamped version: a non-empty store with no schema document.
    pub fn starting_at(version: u32) -> Self {
        Self {
            starting_at: version,
            steps: Vec::new(),
            lock_source: None,
        }
    }

    pub fn step(mut self, step: impl MigrationStep + 'static) -> Self {
        self.steps.push(Box::new(step));
        self
    }

    /// Embed the committed schema lock (`include_str!` of the lock file).
    pub fn lock(mut self, source: impl Into<String>) -> Self {
        self.lock_source = Some(source.into());
        self
    }

    pub fn starting_version(&self) -> u32 {
        self.starting_at
    }

    pub fn current_version(&self) -> u32 {
        self.starting_at + self.steps.len() as u32
    }

    pub fn steps(&self) -> &[Box<dyn MigrationStep>] {
        &self.steps
    }

    pub fn lock_source(&self) -> Option<&str> {
        self.lock_source.as_deref()
    }

    /// Steps are contiguous from [`Self::starting_version`], with unique ids.
    pub fn validate_chain(&self) -> Result<(), MigrationError> {
        let mut ids = HashSet::new();
        for (index, step) in self.steps.iter().enumerate() {
            let expected = self
                .starting_at
                .checked_add(index as u32)
                .ok_or_else(|| MigrationError::Chain("schema version overflow".to_string()))?;
            if step.from_version() != expected {
                return Err(MigrationError::Chain(format!(
                    "step `{}` migrates from version {}, but version {expected} is required",
                    step.id(),
                    step.from_version()
                )));
            }
            if !ids.insert(step.id()) {
                return Err(MigrationError::Chain(format!(
                    "duplicate migration id `{}`",
                    step.id()
                )));
            }
        }
        Ok(())
    }
}

pub(crate) fn apply_pending(
    history: &SchemaHistory,
    store: &mut StoreSnapshot,
    stored_version: u32,
) -> Result<Vec<&'static str>, MigrationError> {
    let mut applied = Vec::new();
    for step in &history.steps {
        if step.from_version() < stored_version {
            continue;
        }
        step.apply(store).map_err(|error| match error {
            MigrationError::Step { .. } => error,
            other => MigrationError::step(step.id(), other.to_string()),
        })?;
        applied.push(step.id());
    }
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rename;

    impl MigrationStep for Rename {
        fn from_version(&self) -> u32 {
            1
        }
        fn id(&self) -> &'static str {
            "rename-speed"
        }
        fn apply(&self, _store: &mut StoreSnapshot) -> Result<(), MigrationError> {
            Ok(())
        }
    }

    struct AlsoFromOne;

    impl MigrationStep for AlsoFromOne {
        fn from_version(&self) -> u32 {
            1
        }
        fn id(&self) -> &'static str {
            "again"
        }
        fn apply(&self, _store: &mut StoreSnapshot) -> Result<(), MigrationError> {
            Ok(())
        }
    }

    // GIVEN a history whose second step does not continue from the previous version
    // WHEN the chain is validated
    // THEN the gap is rejected
    #[test]
    fn rejects_a_gap_in_the_chain() {
        let history = SchemaHistory::starting_at(1).step(Rename).step(AlsoFromOne);
        let error = history.validate_chain().unwrap_err();
        assert!(error.to_string().contains("version 2 is required"), "{error}");
    }

    // GIVEN two steps with the same id
    // WHEN the chain is validated
    // THEN the duplicate id is rejected
    #[test]
    fn rejects_a_duplicate_step_id() {
        struct SameId;
        impl MigrationStep for SameId {
            fn from_version(&self) -> u32 {
                2
            }
            fn id(&self) -> &'static str {
                "rename-speed"
            }
            fn apply(&self, _store: &mut StoreSnapshot) -> Result<(), MigrationError> {
                Ok(())
            }
        }
        let history = SchemaHistory::starting_at(1).step(Rename).step(SameId);
        let error = history.validate_chain().unwrap_err();
        assert!(error.to_string().contains("duplicate migration id"), "{error}");
    }
}
