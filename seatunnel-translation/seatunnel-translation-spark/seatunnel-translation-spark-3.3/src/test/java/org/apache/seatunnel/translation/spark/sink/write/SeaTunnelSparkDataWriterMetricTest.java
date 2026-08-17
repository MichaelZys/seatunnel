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

package org.apache.seatunnel.translation.spark.sink.write;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.seatunnel.translation.spark.metrics.SeaTunnelSparkMetrics.SINK_WRITE_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class SeaTunnelSparkDataWriterMetricTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id"},
                    new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                        BasicType.INT_TYPE
                    });

    @Test
    void shouldCountOnlyRecordsAcceptedBySinkWriter() throws Exception {
        SinkWriter<SeaTunnelRow, Object, Object> sinkWriter = mock(SinkWriter.class);
        SeaTunnelSparkDataWriter<Object, Object> writer =
                new SeaTunnelSparkDataWriter<>(sinkWriter, null, ROW_TYPE, 0);

        writer.write(new GenericInternalRow(new Object[] {1}));

        CustomTaskMetric metric = writer.currentMetricsValues()[0];
        assertEquals(SINK_WRITE_COUNT, metric.name());
        assertEquals(1L, metric.value());

        doThrow(new IOException("expected failure")).when(sinkWriter).write(any());
        assertThrows(
                IOException.class, () -> writer.write(new GenericInternalRow(new Object[] {2})));
        assertEquals(1L, metric.value());

        SeaTunnelSparkWriterCommitMessage<?> commitMessage =
                (SeaTunnelSparkWriterCommitMessage<?>) writer.commit();
        assertEquals(1L, commitMessage.getWrittenCount());
    }
}
