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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #5285: property access on a non-map/non-node direct expression
 * (integer, string, boolean, list literal, or function result) must raise a type error, exactly
 * like the variable-bound case ({@code WITH 42 AS v RETURN v.prop}), instead of silently returning
 * {@code null}.
 * <p>
 * Reference semantics (Neo4j): accessing a property on a scalar value is a runtime type mismatch.
 * Property access on {@code null} still propagates {@code null}, and property access on a map returns
 * the entry (or {@code null} for a missing key).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherPropertyAccessTypeErrorIssue5285Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypherpropaccess5285").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void propertyAccessOnIntegerLiteralRaisesTypeError() {
    assertRaisesTypeError("RETURN (42).prop AS result");
  }

  @Test
  void propertyAccessOnArithmeticResultRaisesTypeError() {
    assertRaisesTypeError("RETURN (40 + 2).prop AS result");
  }

  @Test
  void propertyAccessOnStringLiteralRaisesTypeError() {
    assertRaisesTypeError("RETURN ('hello').prop AS result");
  }

  @Test
  void propertyAccessOnBooleanLiteralRaisesTypeError() {
    assertRaisesTypeError("RETURN (true).prop AS result");
  }

  @Test
  void propertyAccessOnListLiteralRaisesTypeError() {
    assertRaisesTypeError("RETURN ([1, 2, 3]).prop AS result");
  }

  @Test
  void propertyAccessOnToIntegerResultRaisesTypeError() {
    assertRaisesTypeError("RETURN toInteger('42').prop AS result");
  }

  @Test
  void propertyAccessOnSizeResultRaisesTypeError() {
    assertRaisesTypeError("RETURN size([1, 2]).prop AS result");
  }

  @Test
  void variableBoundScalarStillRaisesTypeError() {
    // Control: this path already raised a type error before the fix.
    assertRaisesTypeError("WITH 42 AS value RETURN value.prop AS result");
  }

  @Test
  void propertyAccessOnMapLiteralStillWorks() {
    database.transaction(() -> {
      ResultSet rs = database.query("opencypher", "RETURN {prop: 7}.prop AS result");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("result")).intValue()).isEqualTo(7);

      // Missing key on a map is null, not an error.
      rs = database.query("opencypher", "RETURN {}.prop AS result");
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    });
  }

  @Test
  void propertyAccessOnNullStillReturnsNull() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", "RETURN (null).prop AS result");
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    });
  }

  private void assertRaisesTypeError(final String cypher) {
    final Throwable thrown = catchThrowable(() -> database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", cypher);
      // Force evaluation of the projection.
      while (rs.hasNext())
        rs.next().getProperty("result");
    }));

    assertThat(thrown).as("query should raise a type error: %s", cypher).isNotNull();
    assertThat(thrown.getMessage()).containsIgnoringCase("property");
  }
}
