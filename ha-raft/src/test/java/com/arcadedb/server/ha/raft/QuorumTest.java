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
package com.arcadedb.server.ha.raft;

import com.arcadedb.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the {@link Quorum} enum and its {@code parse} method.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QuorumTest {

  @Test
  void parseMajority() {
    assertThat(Quorum.parse("majority")).isEqualTo(Quorum.MAJORITY);
    assertThat(Quorum.parse("MAJORITY")).isEqualTo(Quorum.MAJORITY);
  }

  @Test
  void parseAll() {
    assertThat(Quorum.parse("all")).isEqualTo(Quorum.ALL);
    assertThat(Quorum.parse("ALL")).isEqualTo(Quorum.ALL);
  }

  @Test
  void parseInvalidThrows() {
    assertThatThrownBy(() -> Quorum.parse("none"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("none");
  }

  @Test
  void parseEmptyThrows() {
    assertThatThrownBy(() -> Quorum.parse(""))
        .isInstanceOf(ConfigurationException.class);
  }
}
