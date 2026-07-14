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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5207: nullIf() on a node property returned the value instead of null
 * because the stored property (Integer) was compared to the literal (Long) with type-strict equality.
 */
class Issue5207Test extends TestHelper {
  @Test
  void nullIfOnPropertyValueMatchesLiteral() {
    database.command("opencypher", "CREATE (:A {v:1}), (:A {v:2})");

    final List<Object> results = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (a:A) RETURN a.v AS v, nullIf(a.v, 1) AS result ORDER BY v")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        results.add(r.getProperty("result"));
      }
    }

    assertThat(results).hasSize(2);
    assertThat(results.get(0)).isNull();                            // v=1 -> nullIf(1,1)=null
    assertThat(((Number) results.get(1)).intValue()).isEqualTo(2);  // v=2 -> 2
  }

  @Test
  void nullIfCrossNumericTypeEquality() {
    // Integer property vs Double literal: Cypher value equality treats 1 = 1.0 as equal.
    database.command("opencypher", "CREATE (:B {v:1})");
    try (final ResultSet rs = database.command("opencypher", "MATCH (b:B) RETURN nullIf(b.v, 1.0) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("result")).isNull();
    }
  }

  @Test
  void nullIfOnPropertyValueDiffers() {
    database.command("opencypher", "CREATE (:C {v:5})");
    try (final ResultSet rs = database.command("opencypher", "MATCH (c:C) RETURN nullIf(c.v, 9) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("result")).intValue()).isEqualTo(5);
    }
  }

  @Test
  void nullIfLiteralsStillWork() {
    try (final ResultSet rs = database.command("opencypher", "RETURN nullIf(1, 1) AS a, nullIf(1, 2) AS b")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("a")).isNull();
      assertThat(((Number) r.getProperty("b")).intValue()).isEqualTo(1);
    }
  }

  @Test
  void nullIfWithStringProperty() {
    database.command("opencypher", "CREATE (:D {name:'Brown'})");
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (d:D) RETURN coalesce(nullIf(d.name, 'Brown'), 'Hazel') AS color")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("color")).isEqualTo("Hazel");
    }
  }
}
