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
package com.arcadedb.bench.env;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class CpuTopologyTest {
  @Test
  void parsesHybridI5() throws IOException {
    try (final InputStream in = getClass().getResourceAsStream("/lscpu-extended-i5-13500.txt")) {
      final CpuTopology topology = CpuTopology.parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
      assertThat(topology.performanceCpus()).isEqualTo("0-11");
      assertThat(topology.efficiencyCpus()).isEqualTo("12-19");
    }
  }

  @Test
  void nonHybridUsesAllCpusForBoth() {
    final StringBuilder sb = new StringBuilder("CPU CORE MAXMHZ\n");
    for (int i = 0; i < 8; i++)
      sb.append(i).append(' ').append(i).append(" 3000.0000\n");
    final CpuTopology topology = CpuTopology.parse(sb.toString());
    assertThat(topology.performanceCpus()).isEqualTo("0-7");
    assertThat(topology.efficiencyCpus()).isEqualTo("0-7");
  }

  private static String rows(final String header, final int from, final int to, final String mhz) {
    final StringBuilder sb = new StringBuilder(header);
    for (int i = from; i <= to; i++)
      sb.append(i).append(' ').append(i / 2).append(' ').append(mhz).append('\n');
    return sb.toString();
  }

  @Test
  void skipsCpusWithoutMaxMhz() {
    final String input = "CPU CORE MAXMHZ\n" + rows("", 0, 3, "3000.0000") + "  4    2 -\n" + rows("", 5, 7, "3000.0000");
    final CpuTopology topology = CpuTopology.parse(input);
    assertThat(topology.performanceCpus()).isEqualTo("0-3,5-7");
    assertThat(topology.efficiencyCpus()).isEqualTo("0-3,5-7");
  }

  @Test
  void favoredCoresStayInPerformanceSet() {
    final String input = rows("CPU CORE MAXMHZ\n", 0, 3, "4800.0000") + rows("", 4, 11, "4700.0000") + rows("", 12, 19, "3500.0000");
    final CpuTopology topology = CpuTopology.parse(input);
    assertThat(topology.performanceCpus()).isEqualTo("0-11");
    assertThat(topology.efficiencyCpus()).isEqualTo("12-19");
  }

  @Test
  void nonContiguousCpuset() {
    final String input = rows("CPU CORE MAXMHZ\n", 0, 3, "4800.0000") + rows("", 4, 7, "3500.0000") + rows("", 8, 11, "4800.0000");
    final CpuTopology topology = CpuTopology.parse(input);
    assertThat(topology.performanceCpus()).isEqualTo("0-3,8-11");
    assertThat(topology.efficiencyCpus()).isEqualTo("4-7");
  }
}
