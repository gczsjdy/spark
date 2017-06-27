/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.vectorized;

import java.math.BigDecimal;
import java.util.*;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * This class is the in memory representation of rows as they are streamed through operators. It
 * is designed to maximize CPU efficiency and not storage footprint. Since it is expected that
 * each operator allocates one of these objects, the storage footprint on the task is negligible.
 *
 * The layout is a columnar with values encoded in their native format. Each RowBatch contains
 * a horizontal partitioning of the data, split into columns.
 *
 * The ColumnarBatch supports either on heap or offheap modes with (mostly) the identical API.
 *
 * TODO:
 *  - There are many TODOs for the existing APIs. They should throw a not implemented exception.
 *  - Compaction: The batch and columns should be able to compact based on a selection vector.
 */
public final class ColumnarBatch extends ColumnarBatchBase{
  private ColumnarBatch(StructType schema, int maxRows, MemoryMode memMode) {
    super(schema, maxRows, memMode);
  }
  public static ColumnarBatch allocate(StructType schema, MemoryMode memMode) {
    return new ColumnarBatch(schema, DEFAULT_BATCH_SIZE, memMode);
  }

  public static ColumnarBatch allocate(StructType type) {
    return new ColumnarBatch(type, DEFAULT_BATCH_SIZE, DEFAULT_MEMORY_MODE);
  }

  public static ColumnarBatch allocate(StructType schema, MemoryMode memMode, int maxRows) {
    return new ColumnarBatch(schema, maxRows, memMode);
  }

}
