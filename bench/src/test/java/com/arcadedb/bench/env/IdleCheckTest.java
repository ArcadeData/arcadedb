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

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class IdleCheckTest {
  @Test
  void busyByLoad() {
    final Optional<String> reason = IdleCheck.check(5.0, 3000, 20, 0.10, 8192);
    assertThat(reason).isPresent();
    assertThat(reason.get()).contains("load");
  }

  @Test
  void busyByMemory() {
    final Optional<String> reason = IdleCheck.check(0.5, 11000, 20, 0.10, 8192);
    assertThat(reason).isPresent();
    assertThat(reason.get()).contains("memory");
  }

  @Test
  void idle() {
    assertThat(IdleCheck.check(0.5, 3000, 20, 0.10, 8192)).isEmpty();
  }
}
