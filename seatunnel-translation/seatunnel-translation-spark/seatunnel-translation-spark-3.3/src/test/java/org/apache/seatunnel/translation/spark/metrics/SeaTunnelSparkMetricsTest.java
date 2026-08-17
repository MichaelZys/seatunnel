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

package org.apache.seatunnel.translation.spark.metrics;

import org.apache.seatunnel.translation.spark.metrics.SeaTunnelSparkMetrics.SinkWriteCountMetric;
import org.apache.seatunnel.translation.spark.metrics.SeaTunnelSparkMetrics.SourceReceivedCountMetric;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SeaTunnelSparkMetricsTest {

    @Test
    void shouldExposeZetaCompatibleNamesAndSumTaskValues() {
        SourceReceivedCountMetric sourceMetric = new SourceReceivedCountMetric();
        SinkWriteCountMetric sinkMetric = new SinkWriteCountMetric();

        assertEquals(SeaTunnelSparkMetrics.SOURCE_RECEIVED_COUNT, sourceMetric.name());
        assertEquals("12", sourceMetric.aggregateTaskMetrics(new long[] {3, 4, 5}));
        assertEquals(SeaTunnelSparkMetrics.SINK_WRITE_COUNT, sinkMetric.name());
        assertEquals("12", sinkMetric.aggregateTaskMetrics(new long[] {3, 4, 5}));
    }
}
