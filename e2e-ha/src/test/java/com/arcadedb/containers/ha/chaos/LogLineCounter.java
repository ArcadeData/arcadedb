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

/**
 * Counts the log lines containing each pattern while the log streams in, holding only the line being assembled: frames
 * from Docker are not guaranteed to end on a line boundary, so a partial last line is carried over to the next frame.
 */
final class LogLineCounter {
  private final String[]      patterns;
  private final int[]         counts;
  private final StringBuilder partial = new StringBuilder();

  LogLineCounter(final String... patterns) {
    this.patterns = patterns;
    this.counts = new int[patterns.length];
  }

  void accept(final String chunk) {
    int start = 0;
    for (int newline = chunk.indexOf('\n'); newline >= 0; newline = chunk.indexOf('\n', start)) {
      partial.append(chunk, start, newline);
      countLine();
      start = newline + 1;
    }
    partial.append(chunk, start, chunk.length());
  }

  /** @return the count per pattern, in constructor order, after counting a last line that has no newline */
  int[] finish() {
    countLine();
    return counts.clone();
  }

  private void countLine() {
    if (partial.isEmpty())
      return;
    final String line = partial.toString();
    for (int i = 0; i < patterns.length; i++)
      if (line.contains(patterns[i]))
        ++counts[i];
    partial.setLength(0);
  }
}
