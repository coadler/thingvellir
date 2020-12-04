use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::{
    Commit, CommitPolicy, CommitPolicyNoDefault, DefaultCommitPolicy, InnerCommitPolicy,
    ServiceHandleMessage, ShardStats,
};
use crate::ShardError;
pub(super) struct ServiceHandleShardSender<Key, Data> {
    sender: mpsc::Sender<ServiceHandleMessage<Key, Data>>,
}

impl<Key, Data> ServiceHandleShardSender<Key, Data> {
    pub(super) fn from_sender(sender: mpsc::Sender<ServiceHandleMessage<Key, Data>>) -> Self {
        Self { sender }
    }
}

impl<Key, Data> Clone for ServiceHandleShardSender<Key, Data> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<Key, Data> ServiceHandleShardSender<Key, Data> {
    pub(super) async fn execute<F, T>(&mut self, key: Key, func: F) -> Result<T, ShardError>
    where
        F: FnOnce(&Data) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        self.sender
            .send(ServiceHandleMessage::Execute(
                key,
                Box::new(move |data: Result<&Data, ShardError>| {
                    // If the result_tx was closed, that means the `execute` future was dropped,
                    // which means no one is going to get the result of `func` anyways, so don't
                    // bother running it. In the event that the shard is overloaded, and causing
                    // timeouts, this serves as a load-shedding mechanism.
                    if result_tx.is_closed() {
                        return;
                    }

                    let result = match data {
                        Ok(data) => {
                            let result = (func)(data);
                            Ok(result)
                        }
                        Err(err) => Err(err),
                    };

                    result_tx.send(result).ok();
                }),
            ))
            .await?;

        result_rx.await?
    }

    pub(super) async fn execute_if_cached<F, T>(
        &mut self,
        key: Key,
        func: F,
    ) -> Result<Option<T>, ShardError>
    where
        F: FnOnce(&Data) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        self.sender
            .send(ServiceHandleMessage::ExecuteIfCached(
                key,
                Box::new(move |data: Option<&Data>| {
                    // If the result_tx was closed, that means the `execute` future was dropped,
                    // which means no one is going to get the result of `func` anyways, so don't
                    // bother running it. In the event that the shard is overloaded, and causing
                    // timeouts, this serves as a load-shedding mechanism.
                    if result_tx.is_closed() {
                        return;
                    }

                    let result = match data {
                        Some(data) => {
                            let result = (func)(data);
                            Ok(Some(result))
                        }
                        None => Ok(None),
                    };

                    result_tx.send(result).ok();
                }),
            ))
            .await?;

        result_rx.await?
    }

    pub(super) async fn execute_mut<F, T>(&mut self, key: Key, func: F) -> Result<T, ShardError>
    where
        F: FnOnce(&mut Data) -> Commit<T> + Send + 'static,
        T: Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        self.sender
            .send(ServiceHandleMessage::ExecuteMut(
                key,
                Box::new(
                    move |data: Result<&mut Data, ShardError>,
                          default_commit_policy: DefaultCommitPolicy| {
                        // If the `result_tx` was closed, this means that the future awaiting the execution
                        // of the mutation was dropped. We can treat this as a "write timeout", where we
                        // simply give up mutating the data. This is a means of back-pressure, to ensure that
                        // if the client has given up trying to service this request, so does the shard. In
                        // the event that the shard is behind, this back-pressure and fast-failing can ensure
                        // that the shard will catch up to real-time.
                        if result_tx.is_closed() {
                            return InnerCommitPolicy::Noop;
                        }

                        match data {
                            Ok(data) => {
                                let result = (func)(data);
                                let (commit_policy, result) = result.into_inner();
                                let result = Ok(result);

                                // We need to convert the commit policy to the inner commit policy,
                                // the only difference here is that the "Immediate" variant holds a callback
                                // which will resolving the future for the caller until the upstream layer
                                // has acknowledged the write.

                                // The code dupe around `result_tx.send(...)` kinda sucks, but unfortunately
                                // is unavoidable.
                                let commit_policy =
                                    commit_policy.apply_default(default_commit_policy);

                                match commit_policy {
                                    CommitPolicyNoDefault::Within(duration) => {
                                        result_tx.send(result).ok();
                                        InnerCommitPolicy::Within(duration)
                                    }
                                    CommitPolicyNoDefault::Noop => {
                                        result_tx.send(result).ok();
                                        InnerCommitPolicy::Noop
                                    }
                                    CommitPolicyNoDefault::Immediate => {
                                        result_tx.send(result).ok();
                                        // todo: move `result_tx.send(result)` back in here once the data persist stuff is
                                        // fully done.
                                        InnerCommitPolicy::Immediate(Box::new(move |result| {
                                            // result_tx.send(result).ok();
                                        }))
                                    }
                                }
                            }
                            Err(err) => {
                                result_tx.send(Err(err)).ok();
                                InnerCommitPolicy::Noop
                            }
                        }
                    },
                ),
            ))
            .await?;

        result_rx.await?
    }

    pub(super) async fn take_data(
        &mut self,
        key: Key,
    ) -> Result<Option<TakenData<Key, Data>>, ShardError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.sender
            .send(ServiceHandleMessage::TakeData(key, result_tx))
            .await?;

        Ok(result_rx.await?)
    }

    pub(super) async fn get_shard_stats(&mut self) -> Result<ShardStats, ShardError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.sender
            .send(ServiceHandleMessage::GetStats(result_tx))
            .await?;

        Ok(result_rx.await?)
    }
}

pub struct TakenData<Key, Data> {
    pub key: Key,
    pub data: Data,
    pub was_enqueued_at: Option<Instant>,
}
