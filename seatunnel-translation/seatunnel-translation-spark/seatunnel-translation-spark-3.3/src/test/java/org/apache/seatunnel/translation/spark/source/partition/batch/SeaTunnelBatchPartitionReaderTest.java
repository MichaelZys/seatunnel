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

package org.apache.seatunnel.translation.spark.source.partition.batch;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.util.LongAccumulator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SeaTunnelBatchPartitionReaderTest {

    @Test
    void shouldCountOnlySuccessfullyReadRecords() throws Exception {
        ParallelBatchPartitionReader delegate = mock(ParallelBatchPartitionReader.class);
        InternalRow first = mock(InternalRow.class);
        InternalRow second = mock(InternalRow.class);
        when(delegate.next()).thenReturn(true, true, false);
        when(delegate.get()).thenReturn(first, second);
        LongAccumulator sourceCounter = new LongAccumulator();
        SeaTunnelBatchPartitionReader reader =
                new SeaTunnelBatchPartitionReader(delegate, sourceCounter);

        assertTrue(reader.next());
        assertEquals(first, reader.get());
        assertTrue(reader.next());
        assertEquals(second, reader.get());
        assertFalse(reader.next());

        assertEquals(2L, sourceCounter.value());
    }
}
