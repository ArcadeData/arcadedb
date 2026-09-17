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

import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7793: the TYPE and BUCKET loops in {@code CheckDatabaseStatement.toString()} appended
 * the separator AFTER the element and only when {@code i > 0}, instead of BEFORE the element when {@code i > 0}
 * (the RECORD loop right below already did it correctly). A two-element list rendered as one run-together
 * identifier plus a trailing comma, which does not parse.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7793CheckDatabaseTypeBucketSeparatorTest extends AbstractParserTest {

  @Test
  void twoTypesReRenderWithASeparatorAndNoTrailingComma() {
    final Statement result = (Statement) checkRightSyntax("CHECK DATABASE TYPE Customer, Order");
    final String rendered = result.toString();

    assertThat(rendered).matches("CHECK DATABASE TYPE \\w+,\\w+");
    assertThat(rendered).doesNotEndWith(",");
  }

  @Test
  void twoBucketsReRenderWithASeparatorAndNoTrailingComma() {
    final Statement result = (Statement) checkRightSyntax("CHECK DATABASE BUCKET 1, 2");
    final String rendered = result.toString();

    assertThat(rendered).matches("CHECK DATABASE BUCKET \\d+,\\d+");
    assertThat(rendered).doesNotEndWith(",");
  }

  @Test
  void copyPreservesTypesBucketsAndFlags() {
    final CheckDatabaseStatement stmt = (CheckDatabaseStatement) new SQLAntlrParser(null)
        .parse("CHECK DATABASE TYPE Customer, Order BUCKET 1, 2 FIX DEEP COMPRESS");
    final CheckDatabaseStatement copy = stmt.copy();

    // equals() is content-based (Set semantics, so insertion order does not matter here), but iteration order of a
    // freshly-populated HashSet is not guaranteed to match the original's, so the two renders are checked for
    // well-formedness rather than byte-for-byte equality.
    assertThat(copy).isEqualTo(stmt);
    assertThat(copy.types).hasSize(2);
    assertThat(copy.buckets).hasSize(2);
    assertThat(copy.fix).isTrue();
    assertThat(copy.deep).isTrue();
    assertThat(copy.compress).isTrue();
    assertThat(copy.toString()).matches("CHECK DATABASE TYPE \\w+,\\w+ BUCKET \\d+,\\d+ FIX DEEP COMPRESS");
  }
}
