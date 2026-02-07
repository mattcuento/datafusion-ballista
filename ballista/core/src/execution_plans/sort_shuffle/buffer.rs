// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! In-memory partition buffer for sort-based shuffle.
//!
//! Each output partition has a buffer that accumulates record batches
//! until the buffer is full or needs to be spilled to disk.

use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::physical_plan::coalesce::LimitedBatchCoalescer;

/// Buffer for accumulating record batches for a single output partition.
///
/// When the buffer exceeds its maximum size, it signals that it should be
/// spilled to disk.
#[derive(Debug)]
pub struct PartitionBuffer {
    /// Partition ID this buffer is for
    partition_id: usize,
    /// Buffered record batches
    batches: Vec<RecordBatch>,
    /// Current memory usage in bytes
    memory_used: usize,
    /// Number of rows in the buffer
    num_rows: usize,
    /// Schema for this partition's data
    schema: SchemaRef,
}

impl PartitionBuffer {
    /// Creates a new partition buffer.
    pub fn new(partition_id: usize, schema: SchemaRef) -> Self {
        Self {
            partition_id,
            batches: Vec::new(),
            memory_used: 0,
            num_rows: 0,
            schema,
        }
    }

    /// Returns the partition ID for this buffer.
    pub fn partition_id(&self) -> usize {
        self.partition_id
    }

    /// Returns the schema for this buffer's data.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the current memory usage in bytes.
    pub fn memory_used(&self) -> usize {
        self.memory_used
    }

    /// Returns the number of rows in the buffer.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns the number of batches in the buffer.
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Appends a record batch to the buffer.
    ///
    /// Returns the new total memory usage after appending.
    pub fn append(&mut self, batch: RecordBatch) -> usize {
        let batch_size = batch.get_array_memory_size();
        self.num_rows += batch.num_rows();
        self.memory_used += batch_size;
        self.batches.push(batch);
        self.memory_used
    }

    /// Drains all batches from the buffer, resetting it to empty.
    ///
    /// Returns the drained batches.
    pub fn drain(&mut self) -> Vec<RecordBatch> {
        self.memory_used = 0;
        self.num_rows = 0;
        std::mem::take(&mut self.batches)
    }

    /// Takes all batches from the buffer without resetting memory tracking.
    ///
    /// This is useful when the caller wants to handle the batches but the
    /// buffer will be discarded anyway.
    pub fn take_batches(&mut self) -> Vec<RecordBatch> {
        std::mem::take(&mut self.batches)
    }

    /// Drains batches from the buffer, coalescing small batches into
    /// larger ones up to `target_batch_size` rows each.
    pub fn drain_coalesced(&mut self, target_batch_size: usize) -> Vec<RecordBatch> {
        self.memory_used = 0;
        self.num_rows = 0;
        let batches = std::mem::take(&mut self.batches);
        coalesce_batches(batches, &self.schema, target_batch_size)
    }

    /// Takes all batches, coalescing small batches into larger ones
    /// up to `target_batch_size` rows each.
    pub fn take_batches_coalesced(
        &mut self,
        target_batch_size: usize,
    ) -> Vec<RecordBatch> {
        let batches = std::mem::take(&mut self.batches);
        coalesce_batches(batches, &self.schema, target_batch_size)
    }
}

/// Buffer for accumulating record batches for a single output partition.
///
/// When the buffer exceeds its maximum size, it signals that it should be
/// spilled to disk.
#[derive(Debug)]
pub struct PartitionIndexBuffer {
    /// Partition ID this buffer is for
    partition_id: usize,
    /// Buffered record batches
    batch_row_indices: Vec<(u32, u32)>,
    /// Number of rows in the buffer
    num_rows: usize,
}

impl PartitionIndexBuffer {
    /// Creates a new partition buffer.
    pub fn new(partition_id: usize) -> Self {
        Self {
            partition_id,
            batch_row_indices: Vec::new(),
            num_rows: 0,
        }
    }

    /// Materialize the PartitionIndexBuffer into partitioned RecordBatches from the
    /// InputBatchStore.
    pub fn materialize_from_indices(
        &self,
        input_batches: &InputBatchStore,
        target_batch_size: usize,
    ) -> Result<Vec<RecordBatch>> {
        if self.batch_row_indices.is_empty() {
            return Ok(Vec::new());
        }

        let mut coalescer =
            BatchCoalescer::new(input_batches.schema.clone(), target_batch_size)
                .with_biggest_coalesce_batch_size(Some(target_batch_size / 2));

        let mut result = Vec::new();

        let mut current_batch_idx = self.batch_row_indices[0].0;
        let mut row_indices = Vec::new();

        for (batch_idx, row_idx) in &self.batch_row_indices {
            if *batch_idx != current_batch_idx {
                if !row_indices.is_empty() {
                    self.materialize_batch(
                        &mut coalescer,
                        input_batches,
                        current_batch_idx,
                        &row_indices,
                        &mut result,
                    )?;
                    row_indices.clear();
                }
                current_batch_idx = *batch_idx;
            }
            row_indices.push(*row_idx);
        }

        if !row_indices.is_empty() {
            self.materialize_batch(
                &mut coalescer,
                input_batches,
                current_batch_idx,
                &row_indices,
                &mut result,
            )?;
        }

        coalescer.finish_buffered_batch()?;
        while let Some(completed) = coalescer.next_completed_batch() {
            result.push(completed);
        }

        Ok(result)
    }

    fn materialize_batch(
        &self,
        coalescer: &mut BatchCoalescer,
        input_batches: &InputBatchStore,
        batch_idx: u32,
        indices: &[u32],
        result: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        if indices.is_empty() {
            return Ok(());
        }
        let batch = input_batches.get_batch(batch_idx);
        let idx_array =
            UInt64Array::from(indices.iter().map(|&x| x as u64).collect::<Vec<_>>());
        coalescer.push_batch_with_indices(batch.clone(), &idx_array)?;

        while let Some(completed) = coalescer.next_completed_batch() {
            result.push(completed);
        }
        Ok(())
    }

    /// Returns the partition ID for this buffer.
    pub fn partition_id(&self) -> usize {
        self.partition_id
    }

    /// Returns the number of rows in the buffer.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Adds batch and row index to the buffer.
    pub fn append_index(&mut self, batch_idx: u32, row_idx: u32) {
        self.batch_row_indices.push((batch_idx, row_idx));
        self.num_rows += 1;
    }

    pub fn memory_used(&self) -> usize {
        self.batch_row_indices.len() * std::mem::size_of::<(u32, u32)>()
    }

    pub fn drain_indices(&mut self) -> Vec<(u32, u32)> {
        self.num_rows = 0;
        std::mem::take(&mut self.batch_row_indices)
    }

    pub fn is_empty(&self) -> bool {
        self.batch_row_indices.is_empty()
    }
}

/// Stores all input batches across all partitions
pub struct InputBatchStore {
    /// Vector of all incoming record batches
    batches: Vec<RecordBatch>,
    /// Schema of all batches
    schema: SchemaRef,
    /// Total memory of all record batches
    total_memory: usize,
}

impl InputBatchStore {
    /// Creates new instance of InputBatchStore
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            batches: Vec::new(),
            schema,
            total_memory: 0,
        }
    }

    /// Appends RecordBatch to the store, returning the batch index.
    pub fn push_batch(&mut self, batch: RecordBatch) -> usize {
        let batch_idx = self.batches.len();
        self.total_memory += batch.get_array_memory_size();
        self.batches.push(batch);
        batch_idx
    }

    /// Return the batch at the given index
    ///
    /// # Panics
    /// Can panic if idx >= batches.len()
    pub fn get_batch(&self, idx: u32) -> &RecordBatch {
        &self.batches[idx as usize]
    }

    /// Returns total memory of all record batches
    pub fn total_memory(&self) -> usize {
        self.total_memory
    }

    /// Drains record batches and memory stats. The same SchemaRef remains.
    pub fn clear(&mut self) {
        self.batches.clear();
        self.total_memory = 0;
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
}

/// Coalesces small batches into larger ones up to `target_batch_size`
/// rows each using DataFusion's `LimitedBatchCoalescer`.
fn coalesce_batches(
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
    target_batch_size: usize,
) -> Vec<RecordBatch> {
    if batches.len() <= 1 {
        return batches;
    }

    let mut coalescer =
        LimitedBatchCoalescer::new(schema.clone(), target_batch_size, None);
    let mut result = Vec::new();

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        // push_batch can only fail on schema mismatch, which won't
        // happen here since all batches share the same schema
        let _ = coalescer.push_batch(batch);
        while let Some(completed) = coalescer.next_completed_batch() {
            result.push(completed);
        }
    }

    // Flush remaining buffered rows
    let _ = coalescer.finish();
    while let Some(completed) = coalescer.next_completed_batch() {
        result.push(completed);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn create_test_batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
        let array = Int32Array::from(values);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_new_buffer() {
        let schema = create_test_schema();
        let buffer = PartitionBuffer::new(0, schema);

        assert_eq!(buffer.partition_id(), 0);
        assert!(buffer.is_empty());
        assert_eq!(buffer.memory_used(), 0);
        assert_eq!(buffer.num_rows(), 0);
        assert_eq!(buffer.num_batches(), 0);
    }

    #[test]
    fn test_append() {
        let schema = create_test_schema();
        let mut buffer = PartitionBuffer::new(0, schema.clone());

        let batch = create_test_batch(&schema, vec![1, 2, 3]);
        buffer.append(batch);

        assert!(!buffer.is_empty());
        assert!(buffer.memory_used() > 0);
        assert_eq!(buffer.num_rows(), 3);
        assert_eq!(buffer.num_batches(), 1);
    }

    #[test]
    fn test_drain() {
        let schema = create_test_schema();
        let mut buffer = PartitionBuffer::new(0, schema.clone());

        buffer.append(create_test_batch(&schema, vec![1, 2, 3]));
        buffer.append(create_test_batch(&schema, vec![4, 5]));

        assert_eq!(buffer.num_batches(), 2);
        assert_eq!(buffer.num_rows(), 5);

        let batches = buffer.drain();

        assert_eq!(batches.len(), 2);
        assert!(buffer.is_empty());
        assert_eq!(buffer.memory_used(), 0);
        assert_eq!(buffer.num_rows(), 0);
    }
}
