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

package com.arcadedb.containers.ha.chaos;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogLineCounterTest {
  private static final String RECOVERED   = "Ratis restarted in place (recovered storage)";
  private static final String REFORMATTED = "Ratis restarted in place (reformatted storage)";

  @Test
  void countsLinesContainingEachPattern() {
    final LogLineCounter counter = new LogLineCounter(RECOVERED, REFORMATTED);
    counter.accept("2026-10-07 INFO [RaftHAServer] " + RECOVERED + "\nother line\n");
    counter.accept("2026-10-07 INFO [RaftHAServer] " + REFORMATTED + "\n" + RECOVERED + "\n");
    assertThat(counter.finish()).containsExactly(2, 1);
  }

  @Test
  void matchSplitAcrossFramesIsCountedOnce() {
    final LogLineCounter counter = new LogLineCounter(RECOVERED, REFORMATTED);
    final String line = "INFO [RaftHAServer] " + REFORMATTED + "\n";
    for (int i = 0; i < line.length(); i += 7)
      counter.accept(line.substring(i, Math.min(line.length(), i + 7)));
    assertThat(counter.finish()).containsExactly(0, 1);
  }

  @Test
  void lastLineWithoutNewlineIsCounted() {
    final LogLineCounter counter = new LogLineCounter(RECOVERED, REFORMATTED);
    counter.accept("first\n" + RECOVERED);
    assertThat(counter.finish()).containsExactly(1, 0);
  }

  @Test
  void carriageReturnsAndEmptyInputAreHarmless() {
    final LogLineCounter counter = new LogLineCounter(RECOVERED, REFORMATTED);
    counter.accept("");
    counter.accept(RECOVERED + "\r\n\r\n");
    assertThat(counter.finish()).containsExactly(1, 0);
  }
}
