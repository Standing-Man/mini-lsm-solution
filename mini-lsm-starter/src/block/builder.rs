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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size: block_size,
            first_key: KeyVec::new(),
        }
    }

    fn usize_to_u8_vec(n: usize) -> Vec<u8> {
        let low = (n & 0xFF) as u8; // 获取低字节
        let high = ((n >> 8) & 0xFF) as u8; // 获取高字节
        vec![high, low]
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.is_empty() {
            self.first_key = key.to_key_vec();
        }
        let key_size = key.len();
        let value_size = value.len();
        let entry_size = key_size + value_size;
        let current_size = self.data.len() + self.offsets.len() * 2;
        if current_size != 0 && current_size + entry_size + 4 > self.block_size {
            return false;
        }
        self.offsets.push(current_size as u16);
        self.data.put_u16(key_size as u16);
        self.data.put_slice(key.raw_ref());
        self.data.put_u16(value_size as u16);
        self.data.put_slice(value);

        return true;
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
