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
package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7789: {@code ProfileStatement.toString()} rendered the literal text {@code "EXPLAIN "}
 * instead of {@code "PROFILE "}, so re-parsing a rendered {@code PROFILE <stmt>} produced an {@code ExplainStatement}
 * - a statement with different execution semantics (PROFILE actually runs the plan, EXPLAIN only plans it).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7789ProfileStatementTest extends AbstractParserTest {

  @Test
  void toStringRendersProfileNotExplain() {
    checkRightSyntax("PROFILE SELECT FROM Person");
  }

  @Test
  void roundTripKeepsTheStatementAProfile() {
    final SimpleNode parsed = checkSyntax("PROFILE SELECT FROM Person", true);
    assertThat(parsed).isInstanceOf(ProfileStatement.class);

    final StringBuilder builder = new StringBuilder();
    parsed.toString(null, builder);
    assertThat(builder.toString()).startsWith("PROFILE ");

    final SimpleNode roundTripped = checkSyntax(builder.toString(), true);
    assertThat(roundTripped).isInstanceOf(ProfileStatement.class);
  }
}
