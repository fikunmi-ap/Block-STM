// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use once_cell::sync::OnceCell;
use std::{
    cmp::{max, PartialOrd},
    collections::{btree_map::BTreeMap, HashMap},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
};
use dashmap::DashMap;
use arc_swap::ArcSwap;

#[cfg(test)]
mod unit_tests;

/// A structure that holds placeholders for each write to the database
//
//  The structure is created by one thread creating the scheduling, and
//  at that point it is used as a &mut by that single thread.
//
//  Then it is passed to all threads executing as a shared reference. At
//  this point only a single thread must write to any entry, and others
//  can read from it. Only entries are mutated using interior mutability,
//  but no entries can be added or deleted.
//

pub type Version = usize;

pub struct MVHashMap<K, V> {
    lock_table: Arc<DashMap<K, AtomicUsize>>, // hashmap that stores reservations
    data_vector: Arc<DashMap<K, V>>, // hashmap that stores data
}

impl<K: Hash + Clone + Eq, V: Clone> MVHashMap<K, V> {
    pub fn new() -> Self {
        MVHashMap {
            lock_table: Arc::new(DashMap::new()),
            data_vector: Arc::new(DashMap::new()),
        }
    }

    /// Write to `key`
    pub fn write(&self, key: &K, data: &V) {
        self.data_vector.insert(key.clone(), data.clone());
    }

    pub fn read(&self, key: &K) -> Option<V> {
        if !self.data_vector.contains_key(key) {
            return None;
        }
        return Some((*self.data_vector.get(key).unwrap()).clone());
    }

    pub fn update_lock_table(&self, key: &K, version: Version) {
        let entry = self.lock_table.entry(key.clone()).or_insert(AtomicUsize::new(version));
        entry.fetch_min(version, Ordering::SeqCst);
    }

    pub fn read_lock_table(&self, key: &K) -> Option<usize> {
        if !self.lock_table.contains_key(key) {
            return None;
        }
        Some(self.lock_table.get(key).unwrap().load(Ordering::SeqCst))
    }

    pub fn init_lock_table(&self) {
        self.lock_table.clear();
    }
}
