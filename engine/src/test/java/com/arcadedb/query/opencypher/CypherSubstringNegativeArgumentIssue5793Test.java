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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5793: {@code substring()} with a negative start index or a negative length raised a
 * plain {@code CommandExecutionException}, which the HTTP layer maps to a 500 status - a server-error
 * classification for what is really an invalid-argument client error. The sibling functions {@code left()} and
 * {@code right()} were already fixed the same way for issue #5296: they throw {@code CommandSemanticException},
 * which extends {@code CommandParsingException} and which {@code AbstractServerHttpHandler} maps to 400. This test
 * pins {@code substring()} to the same classification.
 */
class CypherSubstringNegativeArgumentIssue5793Test extends TestHelper {

  @Test
  void substringNegativeStartIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN substring('hi', -1) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .isInstanceOf(CommandParsingException.class)
        .isNotInstanceOf(CommandExecutionException.class);
  }

  @Test
  void substringNegativeStartWithLengthIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN substring('hi', -3, 2) AS r"))
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void substringNegativeLengthIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN substring('hi', 1, -2) AS r"))
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void substringNegativeStartAndLengthIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN substring('hi', -1, 2) AS r"))
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void substringNonNegativeArgumentsStillWork() {
    // Guard against an over-broad fix that rejects legitimate calls.
    assertThat(single("RETURN substring('hello world', 6) AS r")).isEqualTo("world");
    assertThat(single("RETURN substring('hello world', 0, 5) AS r")).isEqualTo("hello");
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      rs.hasNext();
    }
  }
}
