// Copyright (C) 2020, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use crate::Error;
use crate::Result;

use std::collections::VecDeque;

/// Keeps track of DATAGRAM frames.
#[derive(Default)]
pub struct DatagramQueue {
    queue: Option<VecDeque<Vec<u8>>>,
    queue_max_len: usize,
    queue_bytes_size: usize,
    max_queue_bytes_size: usize,
    dropped_count: usize,
}

impl DatagramQueue {
    pub fn new(
        queue_max_len: usize, max_queue_bytes_size: usize,
    ) -> Self {
        DatagramQueue {
            queue: None,
            queue_bytes_size: 0,
            queue_max_len,
            max_queue_bytes_size,
            dropped_count: 0,
        }
    }

    pub fn push(&mut self, data: Vec<u8>) -> Result<()> {
        if self.is_full() {
            self.dropped_count += 1;
            return Err(Error::Done);
        }

        if self.max_queue_bytes_size > 0 &&
            self.queue_bytes_size + data.len() >
                self.max_queue_bytes_size
        {
            self.dropped_count += 1;
            return Err(Error::Done);
        }

        self.queue_bytes_size += data.len();
        self.queue
            .get_or_insert_with(Default::default)
            .push_back(data);

        Ok(())
    }

    pub fn peek_front_len(&self) -> Option<usize> {
        self.queue.as_ref().and_then(|q| q.front().map(|d| d.len()))
    }

    pub fn peek_front_bytes(&self, buf: &mut [u8], len: usize) -> Result<usize> {
        match self.queue.as_ref().and_then(|q| q.front()) {
            Some(d) => {
                let len = std::cmp::min(len, d.len());
                if buf.len() < len {
                    return Err(Error::BufferTooShort);
                }

                buf[..len].copy_from_slice(&d[..len]);
                Ok(len)
            },

            None => Err(Error::Done),
        }
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        if let Some(d) = self.queue.as_mut().and_then(|q| q.pop_front()) {
            self.queue_bytes_size = self.queue_bytes_size.saturating_sub(d.len());
            return Some(d);
        }

        None
    }

    pub fn has_pending(&self) -> bool {
        !self.queue.as_ref().map(|q| q.is_empty()).unwrap_or(true)
    }

    pub fn purge<F: Fn(&[u8]) -> bool>(&mut self, f: F) {
        if let Some(q) = self.queue.as_mut() {
            q.retain(|d| !f(d));
            self.queue_bytes_size = q.iter().fold(0, |total, d| total + d.len());
        }
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.queue_max_len ||
            (self.max_queue_bytes_size > 0 &&
                self.queue_bytes_size >= self.max_queue_bytes_size)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.queue.as_ref().map(|q| q.len()).unwrap_or(0)
    }

    pub fn byte_size(&self) -> usize {
        self.queue_bytes_size
    }

    /// Returns the number of datagrams that were rejected by
    /// [`push()`].
    pub fn dropped_count(&self) -> usize {
        self.dropped_count
    }

    /// Returns the configured maximum number of items in the
    /// queue.
    pub fn capacity(&self) -> usize {
        self.queue_max_len
    }

    /// Returns the configured maximum byte size of the queue.
    /// A value of 0 means unlimited.
    pub fn max_byte_size(&self) -> usize {
        self.max_queue_bytes_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_up_to_byte_limit() {
        let mut q = DatagramQueue::new(10, 100);
        // Each datagram is 30 bytes; 3 fit (90 <= 100).
        assert!(q.push(vec![0u8; 30]).is_ok());
        assert!(q.push(vec![0u8; 30]).is_ok());
        assert!(q.push(vec![0u8; 30]).is_ok());
        // 4th would be 120 > 100 → rejected.
        assert_eq!(q.push(vec![0u8; 30]), Err(Error::Done));
        assert_eq!(q.byte_size(), 90);
    }

    #[test]
    fn dropped_count_increments_on_reject() {
        let mut q = DatagramQueue::new(2, 0);
        assert!(q.push(vec![1]).is_ok());
        assert!(q.push(vec![2]).is_ok());
        assert_eq!(q.dropped_count(), 0);
        // Count-based reject.
        assert_eq!(q.push(vec![3]), Err(Error::Done));
        assert_eq!(q.dropped_count(), 1);
        assert_eq!(q.push(vec![4]), Err(Error::Done));
        assert_eq!(q.dropped_count(), 2);
    }

    #[test]
    fn dropped_count_increments_on_byte_overflow() {
        let mut q = DatagramQueue::new(10, 5);
        assert!(q.push(vec![0u8; 3]).is_ok());
        // 3 + 4 = 7 > 5 → rejected.
        assert_eq!(q.push(vec![0u8; 4]), Err(Error::Done));
        assert_eq!(q.dropped_count(), 1);
    }

    #[test]
    fn capacity_returns_max_len() {
        let q = DatagramQueue::new(42, 0);
        assert_eq!(q.capacity(), 42);
    }

    #[test]
    fn max_byte_size_returns_configured_value() {
        let q = DatagramQueue::new(10, 256);
        assert_eq!(q.max_byte_size(), 256);
    }

    #[test]
    fn zero_byte_limit_allows_unlimited_bytes() {
        let mut q = DatagramQueue::new(1000, 0);
        for _ in 0..1000 {
            assert!(q.push(vec![0u8; 1000]).is_ok());
        }
        assert_eq!(q.byte_size(), 1_000_000);
        assert_eq!(q.dropped_count(), 0);
    }

    #[test]
    fn pop_adjusts_bytes_and_allows_new_pushes() {
        let mut q = DatagramQueue::new(10, 50);
        assert!(q.push(vec![0u8; 30]).is_ok());
        assert!(q.push(vec![0u8; 20]).is_ok());
        assert_eq!(q.byte_size(), 50);
        // Full on bytes; next push rejected.
        assert_eq!(q.push(vec![0u8; 1]), Err(Error::Done));
        // Pop one (30 bytes) → frees space.
        let popped = q.pop().unwrap();
        assert_eq!(popped.len(), 30);
        assert_eq!(q.byte_size(), 20);
        // Now a 25-byte datagram fits (20 + 25 = 45 <= 50).
        assert!(q.push(vec![0u8; 25]).is_ok());
        assert_eq!(q.byte_size(), 45);
    }

    #[test]
    fn count_limit_and_byte_limit_both_enforced() {
        // count limit = 2, byte limit = 100
        let mut q = DatagramQueue::new(2, 100);
        assert!(q.push(vec![0u8; 10]).is_ok());
        assert!(q.push(vec![0u8; 10]).is_ok());
        // Hits count limit even though bytes are fine.
        assert_eq!(q.push(vec![0u8; 10]), Err(Error::Done));
        assert_eq!(q.dropped_count(), 1);

        // count limit = 100, byte limit = 20
        let mut q2 = DatagramQueue::new(100, 20);
        assert!(q2.push(vec![0u8; 15]).is_ok());
        // Hits byte limit even though count is fine.
        assert_eq!(q2.push(vec![0u8; 10]), Err(Error::Done));
        assert_eq!(q2.dropped_count(), 1);
    }
}
