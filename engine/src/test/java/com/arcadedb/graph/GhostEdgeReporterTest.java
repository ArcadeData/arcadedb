/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.graph;

import com.arcadedb.exception.RecordNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

// GhostEdgeReporter keeps JVM-wide static counters; pin this class to a single thread so its
// resetForTests()/count assertions cannot race with ghost-edge skips from other tests run in parallel.
@Execution(ExecutionMode.SAME_THREAD)
class GhostEdgeReporterTest {
  @BeforeEach
  @AfterEach
  void reset() {
    GhostEdgeReporter.resetForTests();
  }

  @Test
  void countsEachSkip() {
    assertThat(GhostEdgeReporter.getTotalSkipped()).isZero();
    GhostEdgeReporter.reportSkipped(new RecordNotFoundException("Record #98:430104 not found", null));
    GhostEdgeReporter.reportSkipped(new RecordNotFoundException("Record #12:7 not found", null));
    assertThat(GhostEdgeReporter.getTotalSkipped()).isEqualTo(2);
  }

  @Test
  void toleratesNullCause() {
    assertThatCode(() -> GhostEdgeReporter.reportSkipped(null)).doesNotThrowAnyException();
    assertThat(GhostEdgeReporter.getTotalSkipped()).isEqualTo(1);
  }
}
