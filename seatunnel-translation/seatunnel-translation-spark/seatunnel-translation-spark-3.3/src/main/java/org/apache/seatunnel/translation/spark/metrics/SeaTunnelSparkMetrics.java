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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

public final class SeaTunnelSparkMetrics {

    public static final String SOURCE_RECEIVED_COUNT = "SourceReceivedCount";
    public static final String SINK_WRITE_COUNT = "SinkWriteCount";

    private static final Map<String, LongAccumulator> SOURCE_COUNTERS =
            new ConcurrentHashMap<>();

    private SeaTunnelSparkMetrics() {}

    public static LongAccumulator sourceCounter(String jobId) {
        return SOURCE_COUNTERS.computeIfAbsent(
                jobId,
                ignored ->
                        SparkSession.getActiveSession()
                                .get()
                                .sparkContext()
                                .longAccumulator(SOURCE_RECEIVED_COUNT));
    }

    public static OptionalLong takeSourceCount(String jobId) {
        LongAccumulator counter = SOURCE_COUNTERS.remove(jobId);
        return counter == null ? OptionalLong.empty() : OptionalLong.of(counter.value());
    }
}
