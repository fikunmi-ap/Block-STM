// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use std::hash::Hash;
use std::cmp::min;
use crossbeam_queue::SegQueue;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock,
};
use std::iter::FromIterator;
use std::collections::HashSet;
use crate::{
    errors::Error,
    task::{ExecutionStatus, Transaction, TransactionOutput},
};

pub type Version = usize;

// pub struct ReadWriteSet<K> {
//     read_set: Vec<K>,
//     write_set: Vec<K>,
// }

pub struct Scheduler<T, E, K> {
    // Shared index (version) of the next txn to be executed/committed from the current batch
    execution_marker: AtomicUsize,
    stop_at_version: AtomicUsize,
    num_txns: usize,
    batch_size: AtomicUsize,
    batch_list: Vec<AtomicUsize>, // vector for storing txn ids of remaining batch
    batch_queue: SegQueue<usize>, // thread-safe queue for storing txn ids of remaining batch
    outputs: Vec<RwLock<Option<ExecutionStatus<T, Error<E>>>>>, // vector of txn execution outputs
    pub read_write_set: Vec<RwLock<HashSet<K>>>, // read write set of each txn
}

impl<T: TransactionOutput, E: Send + Clone, K: Hash + Clone + Eq> Scheduler<T, E, K> {
    pub fn new(num_txns: usize) -> Self {
        let mut batch_list = Vec::new();
        for i in 0..num_txns {
            batch_list.push(AtomicUsize::new(i));
        }
        Self {
            execution_marker: AtomicUsize::new(0),
            stop_at_version: AtomicUsize::new(num_txns),
            num_txns,
            batch_size: AtomicUsize::new(num_txns),
            batch_list,
            batch_queue: SegQueue::new(),
            outputs: (0..num_txns).map(|_| RwLock::new(None)).collect(),
            read_write_set: (0..num_txns).map(|_| RwLock::new(HashSet::new())).collect(),
        }
    }

    pub fn init_marker(&self) {
        self.execution_marker.store(0, Ordering::Relaxed);
    }

    // return the next txn id in the batch
    pub fn next_txn(&self) -> Option<Version> {
        let next_to_execute = self.execution_marker.fetch_add(1, Ordering::Relaxed);
        if next_to_execute < self.batch_size() {
            let id = self.batch_list[next_to_execute].load(Ordering::Relaxed);
            if id < self.stop_at_version.load(Ordering::Relaxed) {
                return Some(id);
            } 
        }
        return None;
    }

    // Reset the txn version/id to end execution earlier. The executor will stop at the smallest
    // `stop_version` when there are multiple concurrent invocation.
    pub fn set_stop_version(&self, stop_version: Version) {
        self.stop_at_version
            .fetch_min(stop_version, Ordering::Relaxed);
    }

    pub fn get_output(&self, version: Version) -> ExecutionStatus<T, Error<E>> {
        self.outputs[version].write().unwrap().take().unwrap()
    }

    pub fn write_output(&self, version: Version, output: ExecutionStatus<T, Error<E>>) {
        let mut data = self.outputs[version].write().unwrap();
        *data = Some(output);
    }

    // Adding version to the batch queue.
    pub fn add_transaction(&self, version: Version) {
        self.batch_queue.push(version);
    }

    pub fn add_read_write(self, key: &K, version: Version) {
        self.read_write_set[version].write().unwrap().insert(key.clone());
    }

    pub fn get_read_write(&self, version: Version) -> HashSet<K> {
        self.read_write_set[version].read().unwrap().clone()
    }

    pub fn renew_batch_list(&self) {
        let size = self.batch_queue.len();
        self.batch_size.store(size, Ordering::Relaxed);
        for i in 0..size {
            self.batch_list[i].store(self.batch_queue.pop().unwrap(), Ordering::Relaxed);
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size.load(Ordering::Relaxed)
    }

    // Get the last txn version/id
    pub fn num_txn(&self) -> Version {
        self.stop_at_version.load(Ordering::Relaxed)
    }

    pub fn finish(&self) -> bool {
        // finish the loop when batch is empty or stop earlier
        return self.batch_size() == 0 || (self.num_txns-self.batch_size()+1 >= self.num_txn())
    }
}
