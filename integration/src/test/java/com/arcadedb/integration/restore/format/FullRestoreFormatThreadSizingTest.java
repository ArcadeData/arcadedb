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
package com.arcadedb.integration.restore.format;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the automatic restore-thread count of issue #6086, and with it the one place where the restore deliberately
 * differs from the backup: a backup halves the cores because it runs alongside the workload it is already throttling,
 * while a restore has no such neighbour - the database it is producing is not open yet - so it takes whole cores.
 * A one-character edit could turn one into the other without anything else failing.
 */
class FullRestoreFormatThreadSizingTest {
  @ParameterizedTest
  @CsvSource({
      "1, 1",
      "2, 2",
      "3, 3",
      "4, 4",
      "8, 8",
      "16, 8",   // THE CAP: PAST A HANDFUL OF THREADS THE LIMIT IS THE ENTRY COUNT AND THE DISK, NOT THE CORES
      "64, 8",
      "128, 8" })
  void autoSizingTakesTheWholeCoresAndCapsAtEight(final int availableProcessors, final int expected) {
    assertThat(FullRestoreFormat.autoRestoreThreads(availableProcessors)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({ "0", "-1" })
  void neverReturnsAnUnusableThreadCount(final int availableProcessors) {
    assertThat(FullRestoreFormat.autoRestoreThreads(availableProcessors)).isEqualTo(1);
  }
}
