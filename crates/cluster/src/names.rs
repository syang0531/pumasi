//! Node display names an admin set (`pumasi node rename`, 1.3).
//!
//! An agent reports a name when it connects (`--name`, by default the
//! machine's host name). An admin can give a node another one from the
//! CLI, the dashboard or the API; it is kept in `meta` and replaces the
//! reported name wherever the control plane keeps the node's info, so the
//! status, the dashboard, events, metrics and the built-in `name` label
//! all show it. The reported name is remembered next to it, so clearing
//! the override goes back to it even while the node is offline.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use pumasi_proto::{monitor::EventLevel, msg::NodeInfo};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::state::State;

const NAMES_META: &str = "node_names";
/// Longest display name, in characters.
pub const MAX_NAME_CHARS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameOverride {
    /// The name the admin gave.
    pub name: String,
    /// The name the node's agent reports.
    pub reported: String,
}

/// Checks a display name: 1 to 64 characters, no control characters, no
/// spaces at either end.
pub fn validate_name(name: &str) -> Result<()> {
    ensure!(!name.trim().is_empty(), "a node name cannot be empty");
    ensure!(
        name.chars().count() <= MAX_NAME_CHARS,
        "a node name has at most {MAX_NAME_CHARS} characters"
    );
    ensure!(
        name.trim() == name,
        "a node name cannot start or end with a space"
    );
    ensure!(
        !name.chars().any(char::is_control),
        "a node name cannot contain control characters"
    );
    Ok(())
}

impl State {
    pub(crate) fn load_node_names(&self) -> Result<BTreeMap<String, NameOverride>> {
        match self.store.get_meta(NAMES_META)? {
            Some(b) => Ok(postcard::from_bytes::<Vec<(String, NameOverride)>>(&b)
                .context("corrupt node names")?
                .into_iter()
                .collect()),
            None => Ok(BTreeMap::new()),
        }
    }

    fn save_node_names(&self) -> Result<()> {
        if self.names.is_empty() {
            return self.store.delete_meta(NAMES_META);
        }
        let rows: Vec<(&String, &NameOverride)> = self.names.iter().collect();
        self.store.put_meta(
            NAMES_META,
            &postcard::to_stdvec(&rows).expect("node names encode"),
        )
    }

    /// Puts the admin's name for node `id` into info its agent just sent,
    /// remembering the name the agent reported.
    pub(crate) fn apply_node_name(&mut self, id: &str, info: &mut NodeInfo) {
        let Some(o) = self.names.get_mut(id) else {
            return;
        };
        if o.reported != info.name {
            o.reported = info.name.clone();
            if let Err(e) = self.save_node_names() {
                tracing::warn!(node = %id, "could not save node names: {e:#}");
            }
        }
        info.name = self.names[id].name.clone();
    }

    /// Gives a node a display name, or (`None`, or the name its agent
    /// reports) goes back to the reported one. Answers with the node's id
    /// and its name now.
    pub fn rename_node(&mut self, node: &str, name: Option<&str>) -> Result<(String, String)> {
        let id = self.resolve_node(node)?;
        let Some(member) = self.members.get(&id) else {
            bail!("no such node");
        };
        let old = member.info.name.clone();
        let reported = self
            .names
            .get(&id)
            .map(|o| o.reported.clone())
            .unwrap_or_else(|| old.clone());
        let name = name.map(str::trim).filter(|n| !n.is_empty());
        let new = match name {
            Some(n) => {
                validate_name(n)?;
                n.to_string()
            }
            None => reported.clone(),
        };
        if new == old {
            return Ok((id, new));
        }
        if let Some(other) = self
            .members
            .values()
            .find(|m| m.id != id && m.info.name == new)
        {
            bail!(
                "node {} is already named {new:?}; names must be unique so commands can find nodes by name",
                other.id.chars().take(10).collect::<String>()
            );
        }
        if new == reported {
            self.names.remove(&id);
        } else {
            self.names.insert(
                id.clone(),
                NameOverride {
                    name: new.clone(),
                    reported,
                },
            );
        }
        self.save_node_names()?;
        let m = self.members.get_mut(&id).expect("member checked above");
        m.info.name = new.clone();
        let m = m.clone();
        self.store.put_member(&m)?;
        if let Some(n) = self.nodes.get_mut(&id) {
            n.info.name = new.clone();
        }
        info!(node = %id, from = %old, to = %new, "node renamed");
        self.event(
            EventLevel::Info,
            "node.renamed",
            Some(&id),
            None,
            format!("node {old} renamed to {new}"),
        );
        // The built-in `name` label changed: selectors may match others.
        self.schedule();
        Ok((id, new))
    }

    /// Forgets a removed node's display name.
    pub(crate) fn forget_node_name(&mut self, id: &str) -> Result<()> {
        if self.names.remove(id).is_some() {
            self.save_node_names()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_checked() {
        assert!(validate_name("거실 데스크톱").is_ok());
        assert!(validate_name("gpu-box-1").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("   ").is_err());
        assert!(validate_name(" padded").is_err());
        assert!(validate_name("tab\there").is_err());
        assert!(validate_name(&"x".repeat(65)).is_err());
        assert!(validate_name(&"가".repeat(64)).is_ok());
    }
}
