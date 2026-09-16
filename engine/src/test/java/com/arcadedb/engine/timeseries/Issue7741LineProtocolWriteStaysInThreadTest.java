/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7741, item 5: {@code DatabaseAbstractHandler.rejectsUnresolvableSession}'s javadoc justified the
 * {@code requiresTransaction() == false} of {@code POST /api/v1/ts/{database}/write} by saying an auto-commit
 * wrapper "would cost it the parallel shard dispatch".
 * <p>
 * It would not. {@link TimeSeriesGateway#write} calls {@code database.begin()} itself before appending, so
 * {@link TimeSeriesEngine#appendBatch} always sees an active transaction on this route and always keeps the shard
 * writes in-thread (#4957); the line-protocol ingest has never used the shard executor on HTTP or on gRPC. The
 * choice may be right for other reasons - it is, and the javadoc now gives them - but a wrong rationale is how the
 * next reader inherits a constraint that does not exist.
 * <p>
 * This test is that rationale's fact: it watches for the per-type shard threads, which exist only if something
 * dispatched to the shard executor.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7741">issue #7741</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7741LineProtocolWriteStaysInThreadTest extends TestHelper {

  private static final String TYPE = "cpu";

  private static Set<String> shardThreadsOf(final String typeName) {
    final String prefix = "ArcadeDB-TS-Shard-" + typeName + "-";
    return Thread.getAllStackTraces().keySet().stream()
        .map(Thread::getName)
        .filter(name -> name.startsWith(prefix))
        .collect(Collectors.toSet());
  }

  private static List<Sample> samples(final int count) {
    final List<Sample> samples = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      samples.add(new Sample(TYPE, Map.of("host", "web1"), Map.of("value", (double) i), 1_700_000_000_000L + i));
    return samples;
  }

  @Test
  void theLineProtocolWriteKeepsTheShardAppendsOnTheCallingThread() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 4");

    assertThat(shardThreadsOf(TYPE)).as("nothing has dispatched yet").isEmpty();

    final TimeSeriesGateway.WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database, samples(40));

    assertThat(report.written()).isEqualTo(40);
    assertThat(shardThreadsOf(TYPE))
        .as("the write route begins its own transaction, so appendBatch stays in-thread and the shard executor "
            + "is never used - which is why an auto-commit wrapper could not have cost it anything")
        .isEmpty();
  }

  /** The control: with no transaction open, the very same engine call DOES dispatch to the shard executor. */
  @Test
  void anAppendBatchWithNoTransactionStillDispatches() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE dispatched TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 4");
    final TimeSeriesEngine engine =
        ((LocalTimeSeriesType) database.getSchema().getType("dispatched")).getEngine();

    final long[] timestamps = new long[40];
    final Object[] values = new Object[40];
    for (int i = 0; i < 40; i++) {
      timestamps[i] = 1_700_000_000_000L + i;
      values[i] = (double) i;
    }

    engine.appendBatch(timestamps, new Object[][] { values });

    assertThat(shardThreadsOf("dispatched"))
        .as("so the dispatch is real, and it is the transaction on the calling thread that decides")
        .isNotEmpty();
  }
}
