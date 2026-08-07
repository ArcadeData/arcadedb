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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5809: {@code substring(original, start, null)} correctly propagated {@code null} (the
 * fix for #5193) as long as {@code start < length(original)}, but fell through to the two-argument "empty tail"
 * behavior and returned {@code ""} once {@code start} reached or passed the end of the string. Null propagation
 * for an explicit {@code null} length argument must not depend on where the start index lands - the boundary check
 * that returns {@code ""} for the omitted-length form must not run before the explicit-null check for the
 * three-argument form.
 */
class CypherSubstringNullLengthBoundaryIssue5809Test extends TestHelper {

  @Test
  void substringPropagatesNullLengthWhenStartIsBeforeTheEnd() {
    // The #5193 witness - already correct before this fix, pinned here so a regression is caught alongside the
    // boundary cases below.
    assertThat(single("RETURN substring('ab', 1, null) AS r")).isNull();
  }

  @Test
  void substringPropagatesNullLengthWhenStartIsAtTheEnd() {
    assertThat(single("RETURN substring('ab', 2, null) AS r")).isNull();
  }

  @Test
  void substringPropagatesNullLengthWhenStartIsPastTheEnd() {
    assertThat(single("RETURN substring('ab', 3, null) AS r")).isNull();
  }

  @Test
  void substringStillReturnsTheEmptyTailWhenLengthIsOmittedAtOrPastTheEnd() {
    // The two-argument (length omitted) form legitimately returns the empty tail at and past the boundary - this
    // is the behavior that must not be disturbed by the null-length fix above.
    assertThat(single("RETURN substring('ab', 2) AS r")).isEqualTo("");
    assertThat(single("RETURN substring('ab', 3) AS r")).isEqualTo("");
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }
}
