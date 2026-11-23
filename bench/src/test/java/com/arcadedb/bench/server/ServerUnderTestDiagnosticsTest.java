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
package com.arcadedb.bench.server;

import com.arcadedb.serializer.json.JSONArray;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ServerUnderTestDiagnosticsTest {
  @Test
  void threadsComeFromTheStatusLine() {
    assertThat(ServerUnderTest.threadsOf("Name:\tjava\nState:\tS (sleeping)\nThreads:\t57\nVmRSS:\t1 kB\n")).isEqualTo(57);
    assertThat(ServerUnderTest.threadsOf("Name:\tjava\n")).isEqualTo(-1);
    assertThat(ServerUnderTest.threadsOf("Threads:\tmany\n")).isEqualTo(-1);
  }

  @Test
  void histogramKeepsTheTopClassesWithoutTheHeaders() {
    // the shape jcmd prints: the pid line, the column header, a separator, the class rows, the total
    final StringBuilder text = new StringBuilder("1:\n num     #instances         #bytes  class name (module)\n"
        + "-------------------------------------------------------\n");
    for (int i = 1; i <= 100; i++)
      text.append(String.format("%4d:  %10d  %10d  C%d%n", i, 1000 - i, 1000 - i, i));
    text.append("Total        123456       7890123\n");
    final JSONArray rows = ServerUnderTest.topClasses(text.toString(), 30);
    assertThat(rows.length()).isEqualTo(30);
    assertThat(rows.getString(0)).startsWith("   1:").endsWith("C1");
    assertThat(rows.getString(29)).startsWith("  30:").endsWith("C30");
    // a short histogram keeps its class rows only, never the header or the total
    final JSONArray few = ServerUnderTest.topClasses("1:\n num  #instances  #bytes  class name\n   1:  5  80  C1\nTotal  5  80\n", 30);
    assertThat(few.length()).isEqualTo(1);
    assertThat(few.getString(0)).endsWith("C1");
  }
}
