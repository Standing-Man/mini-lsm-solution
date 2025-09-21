// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size: block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.builder.is_empty() {
            self.first_key.clear();
            self.first_key.extend(key.raw_ref());
        }

        if self.builder.add(key, value) {
            self.last_key.clear();
            self.last_key.extend(key.raw_ref());
            return;
        }

        // if the block is full, the split the new block
        self.build_block();

        // countine to add (key, value) into the splitted block
        self.add(key, value);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn build_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encode_block = builder.build().encode();
        let offset = self.data.len();
        self.meta.push(BlockMeta {
            offset: offset,
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
        });
        self.data.extend(encode_block);
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // Append the last partially filled block to data
        self.build_block();
        let meta_offset = self.data.len();
        let mut buf = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        // push the meta block offset into buf
        buf.put_u32(meta_offset as u32);
        let first_key = self.meta.first().unwrap().first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            file: file,
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            id: id,
            block_cache: block_cache,
            first_key: first_key,
            last_key: last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
