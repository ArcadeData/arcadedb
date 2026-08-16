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
package com.arcadedb;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

class ProfilerTest {

  @Test
  void dumpProfileMetrics() {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    Profiler.INSTANCE.dumpMetrics(new PrintStream(out));
    assertThat(out.size() > 0).isTrue();
  }

  @Test
  void metricsToJSON() {
    JSONObject json = Profiler.INSTANCE.toJSON();
    assertThat(json.has("diskFreeSpace")).isTrue();
    assertThat(json.has("diskTotalSpace")).isTrue();
    assertThat(json.has("updateRecord")).isTrue();
    assertThat(json.has("totalDatabases")).isTrue();
  }

  /**
   * #5608: the three commit-time page-merge counters must be reachable by an operator, not only from a debugger or a
   * unit test holding a {@code PageManager}. {@code mergesDeclinedByCoverage} in particular is documented (#5596) as
   * THE signal that a writer is dirtying a mergeable page without declaring it, and the advice on it is to watch it
   * next to the two merge counters - which is impossible while none of them leaves the process.
   */
  @Test
  void pageMergeCountersAreExposed() {
    final JSONObject json = Profiler.INSTANCE.toJSON();
    assertThat(json.has("edgeAppendMerges")).isTrue();
    assertThat(json.has("txPageSlotMerges")).isTrue();
    assertThat(json.has("mergesDeclinedByCoverage")).isTrue();
    // Same nesting as every other counter, which is what the Micrometer binder and Studio read.
    assertThat(json.getJSONObject("mergesDeclinedByCoverage").has("count")).isTrue();

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    Profiler.INSTANCE.dumpMetrics(new PrintStream(out));
    final String dump = out.toString();
    assertThat(dump).contains("edgeAppendMerges=").contains("txPageSlotMerges=").contains("mergesDeclinedByCoverage=");
  }

  /**
   * #6217: the read-path pair of the counters above - a chunked read that met a moved page and completed anyway, and
   * one that had to restart because the record itself had moved - carries the same operator question ("is contention
   * being absorbed, or paid for?") and must leave the process the same way.
   */
  @Test
  void chunkChainReadCountersAreExposed() {
    final JSONObject json = Profiler.INSTANCE.toJSON();
    assertThat(json.has("chunkChainReadRevalidations")).isTrue();
    assertThat(json.has("chunkChainReadRetries")).isTrue();
    assertThat(json.getJSONObject("chunkChainReadRevalidations").has("count")).isTrue();

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    Profiler.INSTANCE.dumpMetrics(new PrintStream(out));
    assertThat(out.toString()).contains("chunkChainReadRevalidations=").contains("chunkChainReadRetries=");
  }
}
