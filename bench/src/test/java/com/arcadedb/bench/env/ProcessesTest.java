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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessesTest {
  @Test
  void returnsTheOutputOfACommandThatExits() {
    assertThat(Processes.output(Duration.ofSeconds(10), "echo", "hello")).isEqualTo("hello\n");
  }

  @Test
  void aCommandThatOutlivesTheTimeoutAnswersNull() {
    // sleep writes nothing: a blocking read of its output would wait the full 30 s instead of the timeout
    assertThat(Processes.output(Duration.ofMillis(200), "sleep", "30")).isNull();
  }

  @Test
  void aFailingCommandAnswersNull() {
    assertThat(Processes.output(Duration.ofSeconds(10), "false")).isNull();
  }

  @Test
  void aMissingCommandAnswersNull() {
    assertThat(Processes.output(Duration.ofSeconds(10), "no-such-command-bench-test")).isNull();
  }
}
