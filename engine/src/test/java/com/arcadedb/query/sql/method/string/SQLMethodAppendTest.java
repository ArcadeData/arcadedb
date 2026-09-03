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
package com.arcadedb.query.sql.method.string;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7028: {@code append()} skipped a null argument anywhere except first, where it returned
 * the receiver untouched and dropped every argument after it - {@code 'x'.append(null, 'A', 'B')} answered
 * {@code "x"} while {@code 'x'.append('A', null, 'B')} answered {@code "xAB"}. Both directions are pinned together.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLMethodAppendTest {

  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodAppend();
  }

  @Test
  void aLeadingNullIsSkippedLikeAnyOtherNull() {
    assertThat(method.execute("x", null, null, new Object[] { null, "A", "B" })).isEqualTo("xAB");
    assertThat(method.execute("x", null, null, new Object[] { "A", null, "B" })).isEqualTo("xAB");
    assertThat(method.execute("x", null, null, new Object[] { "A", "B", null })).isEqualTo("xAB");
  }

  @Test
  void allNullArgumentsLeaveTheReceiverUnchanged() {
    assertThat(method.execute("x", null, null, new Object[] { null })).isEqualTo("x");
    assertThat(method.execute("x", null, null, new Object[] { null, null })).isEqualTo("x");
  }

  @Test
  void noArgumentsAtAllLeaveTheReceiverUnchanged() {
    // THE FRAMEWORK ENFORCES THE ARITY (AT LEAST ONE ARGUMENT); A DIRECT CALLER MUST NOT GET AN NPE
    assertThat(method.execute("x", null, null, null)).isEqualTo("x");
    assertThat(method.execute("x", null, null, new Object[0])).isEqualTo("x");
  }

  @Test
  void aNullReceiverStaysNull() {
    assertThat(method.execute(null, null, null, new Object[] { "A" })).isNull();
    assertThat(method.execute(null, null, null, new Object[] { null, "A" })).isNull();
  }

  @Test
  void appendsEveryArgumentInOrder() {
    assertThat(method.execute("x", null, null, new Object[] { "A", 1, true })).isEqualTo("xA1true");
  }
}
