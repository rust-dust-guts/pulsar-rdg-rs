//! Transaction coordinator administration — `/admin/v3/transactions`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Transactions`. Every endpoint requires
//! `transactionCoordinatorEnabled=true` on the broker; without it the coordinator
//! is absent and reads fail.
//!
//! This is the *observability* surface for transactions. Beginning, committing and
//! aborting transactions is a client-protocol concern, not an admin one, and is
//! not implemented in this client yet.

use std::collections::BTreeMap;

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        models::{
            PositionInPendingAckStats, TransactionBufferInternalStats, TransactionBufferStats,
            TransactionCoordinatorInfo, TransactionCoordinatorInternalStats,
            TransactionCoordinatorStats, TransactionInBufferStats, TransactionInPendingAckStats,
            TransactionMetadata, TransactionPendingAckInternalStats, TransactionPendingAckStats,
            TxnId,
        },
        parse_topic, AdminClient,
    },
    Error,
};

/// Handle for the `transactions` group of admin operations.
///
/// Obtained from [`AdminClient::transactions`].
pub struct Transactions<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Transactions<'_> {
    /// Transactions live under `/admin/v3`, unlike every other group.
    fn txn_url(&self, segments: &[&str]) -> String {
        let mut url = format!("{}/admin/v3/transactions", self.client.admin_url());
        for segment in segments {
            url.push('/');
            url.push_str(segment);
        }
        url
    }

    /// A topic as the three path segments these routes expect:
    /// `tenant/namespace/encodedLocalName`.
    ///
    /// The domain is deliberately absent — the broker's routes are
    /// `.../{tenant}/{namespace}/{topic}`, matching Java's `getRestPath(false)`.
    /// Transactions are only supported on persistent topics.
    fn topic_segments(topic: &str) -> Result<[String; 3], Error> {
        let (_, tenant, namespace, name) = parse_topic(topic)?;
        Ok([
            crate::admin::encode_segment(tenant),
            crate::admin::encode_segment(namespace),
            crate::admin::encode_segment(name),
        ])
    }

    /// Lists the transaction coordinators and the brokers hosting them.
    pub async fn list_transaction_coordinators(
        &self,
    ) -> Result<Vec<TransactionCoordinatorInfo>, Error> {
        let url = self.txn_url(&["coordinators"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets stats for every coordinator, keyed by coordinator id.
    pub async fn get_coordinator_stats(
        &self,
    ) -> Result<BTreeMap<String, TransactionCoordinatorStats>, Error> {
        let url = self.txn_url(&["coordinatorStats"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets stats for one coordinator.
    pub async fn get_coordinator_stats_by_id(
        &self,
        coordinator_id: i32,
    ) -> Result<TransactionCoordinatorStats, Error> {
        let url = self.txn_url(&["coordinatorStats"]);
        self.client
            .send_json(
                Method::GET,
                &url,
                &[("coordinatorId", coordinator_id.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Gets internal (managed-ledger) stats for one coordinator.
    pub async fn get_coordinator_internal_stats(
        &self,
        coordinator_id: i32,
        metadata: bool,
    ) -> Result<TransactionCoordinatorInternalStats, Error> {
        let url = self.txn_url(&["coordinatorInternalStats", &coordinator_id.to_string()]);
        self.client
            .send_json(
                Method::GET,
                &url,
                &[("metadata", metadata.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Gets everything the coordinator knows about one transaction.
    pub async fn get_transaction_metadata(
        &self,
        txn_id: TxnId,
    ) -> Result<TransactionMetadata, Error> {
        let [most, least] = txn_id.as_segments();
        let url = self.txn_url(&["transactionMetadata", &most, &least]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets a transaction's position in one topic's transaction buffer.
    pub async fn get_transaction_in_buffer_stats(
        &self,
        txn_id: TxnId,
        topic: &str,
    ) -> Result<TransactionInBufferStats, Error> {
        let [tenant, namespace, name] = Self::topic_segments(topic)?;
        let [most, least] = txn_id.as_segments();
        let url = self.txn_url(&[
            "transactionInBufferStats",
            &tenant,
            &namespace,
            &name,
            &most,
            &least,
        ]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets a transaction's pending-ack position on one subscription.
    pub async fn get_transaction_in_pending_ack_stats(
        &self,
        txn_id: TxnId,
        topic: &str,
        subscription: &str,
    ) -> Result<TransactionInPendingAckStats, Error> {
        let [tenant, namespace, name] = Self::topic_segments(topic)?;
        let [most, least] = txn_id.as_segments();
        let url = self.txn_url(&[
            "transactionInPendingAckStats",
            &tenant,
            &namespace,
            &name,
            &crate::admin::encode_segment(subscription),
            &most,
            &least,
        ]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets transaction-buffer state for one topic.
    pub async fn get_transaction_buffer_stats(
        &self,
        topic: &str,
        low_water_marks: bool,
        segment_stats: bool,
    ) -> Result<TransactionBufferStats, Error> {
        let [tenant, namespace, name] = Self::topic_segments(topic)?;
        let url = self.txn_url(&["transactionBufferStats", &tenant, &namespace, &name]);
        self.client
            .send_json(
                Method::GET,
                &url,
                &[
                    ("lowWaterMarks", low_water_marks.to_string()),
                    ("segmentStats", segment_stats.to_string()),
                ],
                NO_BODY,
            )
            .await
    }

    /// Gets internal transaction-buffer stats for one topic.
    pub async fn get_transaction_buffer_internal_stats(
        &self,
        topic: &str,
        metadata: bool,
    ) -> Result<TransactionBufferInternalStats, Error> {
        let [tenant, namespace, name] = Self::topic_segments(topic)?;
        let url = self.txn_url(&["transactionBufferInternalStats", &tenant, &namespace, &name]);
        self.client
            .send_json(
                Method::GET,
                &url,
                &[("metadata", metadata.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Gets pending-acknowledgement state for one subscription.
    pub async fn get_pending_ack_stats(
        &self,
        topic: &str,
        subscription: &str,
        low_water_marks: bool,
    ) -> Result<TransactionPendingAckStats, Error> {
        let [tenant, namespace, name] = Self::topic_segments(topic)?;
        let url = self.txn_url(&[
            "pendingAckStats",
            &tenant,
            &namespace,
            &name,
            &crate::admin::encode_segment(subscription),
        ]);
        self.client
            .send_json(
                Method::GET,
                &url,
                &[("lowWaterMarks", low_water_marks.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Gets internal pending-ack stats for one subscription.
    pub async fn get_pending_ack_internal_stats(
        &self,
        topic: &str,
        subscription: &str,
        metadata: bool,
    ) -> Result<TransactionPendingAckInternalStats, Error> {
        let [tenant, namespace, name] = Self::topic_segments(topic)?;
        let url = self.txn_url(&[
            "pendingAckInternalStats",
            &tenant,
            &namespace,
            &name,
            &crate::admin::encode_segment(subscription),
        ]);
        self.client
            .send_json(
                Method::GET,
                &url,
                &[("metadata", metadata.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Lists transactions open longer than `timeout_ms`, keyed by transaction id.
    pub async fn get_slow_transactions(
        &self,
        timeout_ms: i64,
    ) -> Result<BTreeMap<String, TransactionMetadata>, Error> {
        let url = self.txn_url(&["slowTransactions", &timeout_ms.to_string()]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists slow transactions on one coordinator.
    pub async fn get_slow_transactions_by_coordinator_id(
        &self,
        coordinator_id: i32,
        timeout_ms: i64,
    ) -> Result<BTreeMap<String, TransactionMetadata>, Error> {
        let url = self.txn_url(&["slowTransactions", &timeout_ms.to_string()]);
        self.client
            .send_json(
                Method::GET,
                &url,
                &[("coordinatorId", coordinator_id.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Changes the number of transaction coordinators.
    ///
    /// The count can only grow.
    pub async fn scale_transaction_coordinators(&self, replicas: i32) -> Result<(), Error> {
        let url = self.txn_url(&["transactionCoordinator", "replicas"]);
        self.client
            .send_empty(Method::POST, &url, &[], Some(&replicas))
            .await
    }

    /// Aborts a transaction.
    ///
    /// The one mutating operation on this interface; the rest are read-only.
    pub async fn abort_transaction(&self, txn_id: TxnId) -> Result<(), Error> {
        let [most, least] = txn_id.as_segments();
        let url = self.txn_url(&["abortTransaction", &most, &least]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Gets position stats within a subscription's pending-ack store.
    pub async fn get_position_stats_in_pending_ack(
        &self,
        topic: &str,
        subscription: &str,
        ledger_id: i64,
        entry_id: i64,
        batch_index: Option<i32>,
    ) -> Result<PositionInPendingAckStats, Error> {
        let [tenant, namespace, name] = Self::topic_segments(topic)?;
        let url = self.txn_url(&[
            "positionStatsInPendingAck",
            &tenant,
            &namespace,
            &name,
            &crate::admin::encode_segment(subscription),
            &ledger_id.to_string(),
            &entry_id.to_string(),
        ]);
        let mut query = vec![];
        if let Some(index) = batch_index {
            query.push(("batchIndex", index.to_string()));
        }
        self.client
            .send_json(Method::GET, &url, &query, NO_BODY)
            .await
    }
}
