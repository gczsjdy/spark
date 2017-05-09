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
import java.math.BigInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * This class represents a column of values and provides the main APIs to access the data
 * values. It supports all the types and contains get/put APIs as well as their batched versions.
 * The batched versions are preferable whenever possible.
 *
 * To handle nested schemas, ColumnVector has two types: Arrays and Structs. In both cases these
 * columns have child columns. All of the data is stored in the child columns and the parent column
 * contains nullability, and in the case of Arrays, the lengths and offsets into the child column.
 * Lengths and offsets are encoded identically to INTs.
 * Maps are just a special case of a two field struct.
 * Strings are handled as an Array of ByteType.
 *
 * Capacity: The data stored is dense but the arrays are not fixed capacity. It is the
 * responsibility of the caller to call reserve() to ensure there is enough room before adding
 * elements. This means that the put() APIs do not check as in common cases (i.e. flat schemas),
 * the lengths are known up front.
 *
 * Most of the APIs take the rowId as a parameter. This is the batch local 0-based row id for values
 * in the current RowBatch.
 *
 * A ColumnVector should be considered immutable once originally created. In other words, it is not
 * valid to call put APIs after reads until reset() is called.
 *
 * ColumnVectors are intended to be reused.
 */
public abstract class ColumnVector extends ColumnVectorBase implements AutoCloseable {
  protected ColumnVector(int capacity, DataType type, MemoryMode memMode) {
    super(capacity, type, memMode);
  }
  public static ColumnVector allocate(int capacity, DataType type, MemoryMode mode) {
    if (mode == MemoryMode.OFF_HEAP) {
      return new OffHeapColumnVector(capacity, type);
    } else {
      return new OnHeapColumnVector(capacity, type);
    }
  }
}
