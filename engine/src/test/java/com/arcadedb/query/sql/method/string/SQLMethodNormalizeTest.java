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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLMethodNormalizeTest {

  private SQLMethod method;

  @BeforeEach
  void setup() {
    method = new SQLMethodNormalize();
  }

  @Test
  void testNull() {
    final Object result = method.execute(null, null, null, null);
    assertThat(result).isNull();
  }

  @Test
  void normalizeWithDefaultNormalizer() {
    final Object result = method.execute("À", null, null, null);
    assertThat(result).isEqualTo("A");

  }

  @Test
  void normalizeWithNFC() {
    final Object result = method.execute("À", null, null, new Object[] { "NFC" });
    assertThat(result).isEqualTo("À");
  }

  @Test
  void normalizeWithNFCAndPattern() {
    final Object result = method.execute("À", null, null, new Object[] { "NFC", "" });
    assertThat(result).isEqualTo("À");
  }

  @Test
  void catastrophicPatternArgumentIsAbortedByRegexTimeout() {
    // Issue #5886, 9th review pass: the 2nd argument is a caller-supplied regex applied via replaceAll() with
    // no bound at all - unlike text.regexReplace(), which was fixed earlier in this issue, this call site was
    // missed entirely. Uses the default 1000ms arcadedb.command.regexTimeout (this test has no database/context
    // to lower it, method.execute() is called directly with a null context here, matching this file's existing
    // convention) - still proves the abort happens near that bound rather than the tens of seconds an unbounded
    // catastrophic match takes.
    final String pathologicalInput = "a".repeat(40) + "!";

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> method.execute(pathologicalInput, null, null, new Object[] { "NFC", "(.*a){20}$" }))
        .isInstanceOf(TimeoutException.class);

    stopwatch.assertGaveUpWithin(5000, "the configured 200ms deadline from an unbounded match");
  }
}
