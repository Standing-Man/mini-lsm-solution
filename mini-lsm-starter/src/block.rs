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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::new();

        buf.extend_from_slice(&self.data);

        // record the number of offsets
        let offset_size = self.offsets.len();
        for &offset in &self.offsets {
            buf.put_u16(offset);
        }
        buf.put_u16(offset_size as u16);

        Bytes::from(buf)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut index = data.len() - 1;
        let offset_size = (data[index - 1] as u16) << 8 | (data[index] as u16);
        index -= 2;
        let mut offsets = vec![];
        for _ in 0..offset_size {
            let offset = (data[index - 1] as u16) << 8 | (data[index] as u16);
            offsets.insert(0, offset);
            index -= 2;
        }
        let entries = data[0..=index].to_vec();
        return Self {
            data: entries,
            offsets: offsets,
        };
    }
}
