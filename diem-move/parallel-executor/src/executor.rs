// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    errors::*,
    outcome_array::OutcomeArray,
    scheduler::Scheduler,
    task::{ExecutionStatus, ExecutorTask, ReadWriteSetInferencer, Transaction, TransactionOutput},
};
use anyhow::{bail, Result as AResult};
use mvhashmap::{MVHashMap, Version};
use num_cpus;
use rayon::{prelude::*, scope};
use std::{
    cmp::{max, min},
    hash::Hash,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

pub struct MVHashMapView<'a, K, V, T, E> {
    map: &'a MVHashMap<K, V>,
    version: Version,
    scheduler: &'a Scheduler<T, E, K>,
    // has_unexpected_read: AtomicBool,
}

impl<'a, K: Hash + Clone + Eq, V: Clone, T: TransactionOutput, E: Send + Clone> MVHashMapView<'a, K, V, T, E> {
    pub fn read(&self, key: &K) -> AResult<Option<V>> {
        self.add_read_write(key);
        match self.map.read(key) {
            Some(v) => Ok(Some(v)),
            None => Ok(None),
        }
    }

    pub fn write(&self, key: &K) {
        // println!("update lock table for id {}", self.version);
        self.map.update_lock_table(key, self.version);
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn add_read_write(&self, key: &K) {
        self.scheduler.read_write_set[self.version].write().unwrap().insert(key.clone());
        // self.scheduler.add_read_write(&key.clone(), self.version);
    }

    pub fn can_commit(&self) -> bool {
        let read_write_set = self.scheduler.get_read_write(self.version);
        // println!("read write set size {}", read_write_set.len());
        for k in read_write_set {
            match self.map.read_lock_table(&k) {
                Some(version) => {
                    if version < self.version {
                        return false;
                    }
                }
                None => {}
            }
        }
        return true;
    }
}

pub struct ParallelTransactionExecutor<T: Transaction, E: ExecutorTask, I: ReadWriteSetInferencer> {
    num_cpus: usize,
    inferencer: I,
    phantom: PhantomData<(T, E, I)>,
}

impl<T, E, I> ParallelTransactionExecutor<T, E, I>
where
    T: Transaction,
    E: ExecutorTask<T = T>,
    I: ReadWriteSetInferencer,
{
    pub fn new(inferencer: I) -> Self {
        Self {
            num_cpus: num_cpus::get(),
            inferencer,
            phantom: PhantomData,
        }
    }

    pub fn execute_transactions_parallel(
        &self,
        task_initial_arguments: E::Argument,
        signature_verified_block: Vec<T>,
    ) -> Result<Vec<E::Output>, E::Error> where <T as Transaction>::Value: std::clone::Clone {
        if signature_verified_block.is_empty() {
            return Ok(vec![]);
        }
        let num_txns = signature_verified_block.len();

        let outcomes = OutcomeArray::new(num_txns);

        let scheduler = Arc::new(Scheduler::new(num_txns));
        let shared_data = Arc::new(MVHashMap::new());
        let compute_cpus = self.num_cpus - 1; //min(1 + (num_txns / 50), self.num_cpus - 1); // Ensure we have at least 50 tx per thread.
        // println!(
        //     "Number of threads: {}",
        //     compute_cpus
        // );

        while !scheduler.finish() {
            println!(
                "Batch size: {}",
                scheduler.batch_size()
            );
            // Initialize the lock table to be empty
            shared_data.init_lock_table();
            // Initialize the execution marker to be 0
            scheduler.init_marker();

            // Execute txn batch in parallel
            scope(|s| {
                for _ in 0..(compute_cpus) {
                    s.spawn(|_| {
                        let scheduler = Arc::clone(&scheduler);
                        // Make a new executor per thread.
                        let task = E::init(task_initial_arguments);
    
                        while let Some(idx) = scheduler.next_txn() {
                            // println!("execute id {}", idx);  
                            let txn = &signature_verified_block[idx];
    
                            let view = MVHashMapView {
                                map: &shared_data,
                                version: idx,
                                scheduler: &scheduler,
                            };
                            let execute_result = task.execute_transaction(&view, txn);
                            let commit_result =
                                match execute_result {
                                    ExecutionStatus::Success(output) => {
                                        // Hack to update the lock table, since we did not implement VM hijacker for writes
                                        output.get_writes().into_iter().for_each(|(k, _)| { view.write(&k); view.add_read_write(&k); });
                                        ExecutionStatus::Success(output)
                                    }
                                    ExecutionStatus::SkipRest(output) => {
                                        // scheduler.set_stop_version(idx + 1);
                                        ExecutionStatus::SkipRest(output)
                                    }
                                    ExecutionStatus::Abort(err) => {
                                        // Abort the execution with user defined error.
                                        // scheduler.set_stop_version(idx + 1);
                                        ExecutionStatus::Abort(Error::UserError(err.clone()))
                                    }
                                };
                            
                            scheduler.write_output(idx, commit_result);
                        }
                    });
                }
            });

            // Initialize the execution marker to be 0
            scheduler.init_marker();

            // Commit txn batch in parallel
            scope(|s| {
                for _ in 0..(compute_cpus) {
                    s.spawn(|_| {
                        let scheduler = Arc::clone(&scheduler);
    
                        while let Some(idx) = scheduler.next_txn() {  
                            // println!("commit id {}", idx);  
                            let view = MVHashMapView {
                                map: &shared_data,
                                version: idx,
                                scheduler: &scheduler,
                            };

                            if view.can_commit() {
                                // println!("committed id {}", idx); 
                                let commit_result = scheduler.get_output(idx);

                                let result =
                                match commit_result {
                                    ExecutionStatus::Success(output) => ExecutionStatus::Success(output),
                                    ExecutionStatus::SkipRest(output) => {
                                        scheduler.set_stop_version(idx + 1);
                                        ExecutionStatus::SkipRest(output)
                                    }
                                    ExecutionStatus::Abort(err) => {
                                        // Abort the execution with user defined error.
                                        scheduler.set_stop_version(idx + 1);
                                        ExecutionStatus::Abort(err)
                                    }
                                };

                                outcomes.set_result(idx, result);

                            } else {
                                // Add the transaction to the next batch
                                scheduler.add_transaction(idx);
                            }
                        }
                    });
                }
            });

            // Prepare the batch for next iteration
            scheduler.renew_batch_list();
        }

        

        // Splits the head of the vec of results that are valid
        let valid_results_length = scheduler.num_txn();

        // Dropping large structures is expensive -- do this is a separate thread.
        ::std::thread::spawn(move || {
            // drop(scheduler);
            // drop(infer_result);
            drop(signature_verified_block); // Explicit drops to measure their cost.
            drop(shared_data);
        });

        outcomes.get_all_results(valid_results_length)
    }
}
