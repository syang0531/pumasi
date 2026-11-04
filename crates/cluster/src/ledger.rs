//! Accounts and settlement: prepaid credit, charges from receipts, held
//! provider earnings, release after the hold period, forfeits.
//!
//! Every money movement is a pair of ledger entries written in the same
//! redb transaction as the balances (and the receipt that caused it).

use anyhow::{Result, bail, ensure};
use pumasi_proto::{
    economy::{Micros, format_credits, validate_account},
    msg::{AccountView, LedgerEntry, LedgerKind, TaskRef},
    receipt::Receipt,
};
use tracing::{info, warn};

use crate::{
    state::{Hold, State, now_unix},
    store::LedgerWrite,
};

/// Ledger changes collected before one atomic write.
#[derive(Default)]
struct Batch {
    entries: Vec<LedgerEntry>,
    touched: Vec<String>,
    put_holds: Vec<Hold>,
    del_holds: Vec<TaskRef>,
}

impl State {
    pub(crate) fn ensure_account(&mut self, name: &str) -> Result<()> {
        if self.accounts.contains_key(name) {
            return Ok(());
        }
        validate_account(name)?;
        let a = AccountView {
            name: name.to_string(),
            balance: 0,
            held: 0,
            spent: 0,
            earned: 0,
        };
        self.store.write_ledger(&LedgerWrite {
            accounts: vec![&a],
            ..Default::default()
        })?;
        self.accounts.insert(name.to_string(), a);
        Ok(())
    }

    fn account(&mut self, name: &str) -> &mut AccountView {
        self.accounts
            .entry(name.to_string())
            .or_insert_with(|| AccountView {
                name: name.to_string(),
                balance: 0,
                held: 0,
                spent: 0,
                earned: 0,
            })
    }

    fn entry(
        &mut self,
        b: &mut Batch,
        account: &str,
        kind: LedgerKind,
        amount: Micros,
        task: Option<TaskRef>,
        memo: String,
    ) {
        let seq = self.next_ledger_seq;
        self.next_ledger_seq += 1;
        b.entries.push(LedgerEntry {
            seq,
            unix: now_unix(),
            account: account.to_string(),
            kind,
            amount,
            task,
            memo,
        });
        if !b.touched.iter().any(|a| a == account) {
            b.touched.push(account.to_string());
        }
    }

    fn commit(&mut self, b: Batch, receipt: Option<&Receipt>) {
        let accounts: Vec<&AccountView> = b
            .touched
            .iter()
            .filter_map(|n| self.accounts.get(n))
            .collect();
        let res = self.store.write_ledger(&LedgerWrite {
            accounts,
            entries: &b.entries,
            put_holds: &b.put_holds,
            del_holds: &b.del_holds,
            receipt,
        });
        if let Err(e) = res {
            warn!("persist ledger: {e:#}");
        }
    }

    /// Stores a countersigned receipt and, when it carries a charge, debits
    /// the consumer and puts the provider's earning on hold.
    pub(crate) fn charge(&mut self, receipt: &Receipt) {
        let mut b = Batch::default();
        let billing = &receipt.billing;
        let t = receipt.usage.task;
        if billing.amount > 0 {
            let amount = billing.amount;
            let memo = format!("receipt {}", receipt.short_id());
            {
                let c = self.account(&billing.consumer);
                c.balance -= amount;
                c.spent += amount;
            }
            self.entry(
                &mut b,
                &billing.consumer,
                LedgerKind::Charge,
                -amount,
                Some(t),
                memo.clone(),
            );
            self.account(&billing.provider).held += amount;
            self.entry(
                &mut b,
                &billing.provider,
                LedgerKind::Hold,
                amount,
                Some(t),
                memo,
            );
            let hold = Hold {
                task: t,
                consumer: billing.consumer.clone(),
                provider: billing.provider.clone(),
                amount,
                release_after_unix: billing.recorded_unix + self.policy.hold_secs,
            };
            self.holds.insert(t, hold.clone());
            b.put_holds.push(hold);
        }
        self.commit(b, Some(receipt));
    }

    /// Makes held earnings spendable once their hold period is over and
    /// their task is not waiting on a check.
    pub(crate) fn release_holds(&mut self, now: u64) {
        let due: Vec<TaskRef> = self
            .holds
            .values()
            .filter(|h| h.release_after_unix <= now && !self.check_pending(h.task))
            .map(|h| h.task)
            .collect();
        if due.is_empty() {
            return;
        }
        let mut b = Batch::default();
        for t in due {
            let Some(h) = self.holds.remove(&t) else {
                continue;
            };
            {
                let p = self.account(&h.provider);
                p.held -= h.amount;
                p.balance += h.amount;
                p.earned += h.amount;
            }
            self.entry(
                &mut b,
                &h.provider,
                LedgerKind::Release,
                h.amount,
                Some(t),
                "hold period over".into(),
            );
            b.del_holds.push(t);
        }
        self.commit(b, None);
    }

    fn check_pending(&self, t: TaskRef) -> bool {
        self.jobs
            .get(&t.job)
            .and_then(|j| j.tasks.get(t.index as usize))
            .is_some_and(|task| task.state == pumasi_proto::msg::TaskState::Verifying)
    }

    /// Cancels a run's held earning and refunds the consumer, because the
    /// run's result was rejected.
    pub(crate) fn forfeit(&mut self, t: TaskRef, reason: &str) {
        let Some(h) = self.holds.remove(&t) else {
            return;
        };
        let mut b = Batch::default();
        self.account(&h.provider).held -= h.amount;
        self.entry(
            &mut b,
            &h.provider,
            LedgerKind::Forfeit,
            -h.amount,
            Some(t),
            reason.to_string(),
        );
        {
            let c = self.account(&h.consumer);
            c.balance += h.amount;
            c.spent -= h.amount;
        }
        self.entry(
            &mut b,
            &h.consumer,
            LedgerKind::Refund,
            h.amount,
            Some(t),
            reason.to_string(),
        );
        if let Some(task) = self
            .jobs
            .get_mut(&t.job)
            .and_then(|j| j.tasks.get_mut(t.index as usize))
        {
            task.cost -= h.amount;
        }
        b.del_holds.push(t);
        info!(task = %t, provider = %h.provider, amount = %format_credits(h.amount), "earning forfeited: {reason}");
        self.commit(b, None);
    }

    pub fn deposit(&mut self, account: &str, amount: Micros, memo: &str) -> Result<AccountView> {
        ensure!(amount > 0, "amount must be positive");
        self.ensure_account(account)?;
        let mut b = Batch::default();
        self.account(account).balance += amount;
        self.entry(
            &mut b,
            account,
            LedgerKind::Deposit,
            amount,
            None,
            memo.to_string(),
        );
        self.commit(b, None);
        // Credit can unblock work waiting for a paying account.
        self.schedule();
        Ok(self.accounts[account].clone())
    }

    pub fn payout(&mut self, account: &str, amount: Micros, memo: &str) -> Result<AccountView> {
        ensure!(amount > 0, "amount must be positive");
        let Some(a) = self.accounts.get(account) else {
            bail!("no account {account:?}");
        };
        if a.balance < amount {
            bail!(
                "{account} has {} credits available (held earnings are not available until released)",
                format_credits(a.balance)
            );
        }
        let mut b = Batch::default();
        self.account(account).balance -= amount;
        self.entry(
            &mut b,
            account,
            LedgerKind::Payout,
            -amount,
            None,
            memo.to_string(),
        );
        self.commit(b, None);
        Ok(self.accounts[account].clone())
    }

    pub fn accounts(&self) -> Vec<AccountView> {
        let mut v: Vec<AccountView> = self.accounts.values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    pub fn ledger(&self, account: Option<&str>, limit: usize) -> Result<Vec<LedgerEntry>> {
        self.store.ledger(account, limit)
    }
}
