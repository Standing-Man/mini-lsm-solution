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

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    use_a: bool,
    is_equal: bool,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            use_a: false,
            is_equal: false,
        };
        if !iter.a.is_valid() && !iter.b.is_valid() {
            return Ok(iter);
        }

        if iter.a.is_valid() && !iter.b.is_valid() {
            iter.use_a = true;
            iter.is_equal = false;
            return Ok(iter);
        }

        if !iter.a.is_valid() && iter.b.is_valid() {
            iter.use_a = false;
            iter.is_equal = false;
            return Ok(iter);
        }

        if iter.a.key() <= iter.b.key() {
            iter.use_a = true;
            if iter.a.key() == iter.b.key() {
                iter.is_equal = true;
            }
        }
        Ok(iter)
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.use_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.use_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.use_a {
            self.a.next()?;
            if self.is_equal {
                self.b.next()?;
            }
        } else {
            self.b.next()?;
        }
        if !self.is_valid() {
            return Ok(());
        }

        if self.a.is_valid() && !self.b.is_valid() {
            self.use_a = true;
            self.is_equal = false;
            return Ok(());
        }

        if !self.a.is_valid() && self.b.is_valid() {
            self.use_a = false;
            self.is_equal = false;
            return Ok(());
        }

        if self.a.key() <= self.b.key() {
            self.use_a = true;
            if self.a.key() == self.b.key() {
                self.is_equal = true;
            } else {
                self.is_equal = false;
            }
        }
        Ok(())
    }
}
