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
package com.arcadedb.integration.backup.format;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the automatic compression-thread count of issue #6072. The formula is trivial, which is exactly why it is worth
 * a test: it encodes a deliberate trade - leave half the machine to the workload the backup is already throttling, and
 * do not let a very wide machine put dozens of threads on a job that saturates the disk first - and a one-character
 * edit could change that trade without anything else failing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullBackupFormatThreadSizingTest {
  @ParameterizedTest
  @CsvSource({
      "1, 1",    // A SINGLE CORE STILL GETS A WORKER: THE PARALLEL WRITER NEEDS AT LEAST ONE, AND IT STILL PIPELINES
      "2, 1",    // COMPRESSION AGAINST THE READ
      "3, 1",
      "4, 2",
      "8, 4",
      "16, 8",
      "20, 8",   // THE CAP
      "64, 8",
      "128, 8" })
  void autoSizingHalvesTheCoresAndCapsAtEight(final int availableProcessors, final int expected) {
    assertThat(FullBackupFormat.autoCompressionThreads(availableProcessors)).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void neverReturnsAnUnusableThreadCount(final int availableProcessors) {
    assertThat(FullBackupFormat.autoCompressionThreads(availableProcessors)).isEqualTo(1);
  }
}
