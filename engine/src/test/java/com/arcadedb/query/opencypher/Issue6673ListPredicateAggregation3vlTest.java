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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #6673.
 * <p>
 * The list predicates {@code all()}/{@code any()}/{@code none()}/{@code single()} lose Cypher
 * three-valued logic (3VL) when evaluated inside a projection that also contains an aggregation:
 * {@link com.arcadedb.query.opencypher.executor.ExpressionEvaluator} used to special-case
 * {@code ListPredicateExpression} with its own {@code evaluateListPredicate} that counted only
 * {@code Boolean.TRUE} matches and could only ever return {@code true} or {@code false}, never
 * {@code null} - diverging from the primary AST implementation
 * ({@link com.arcadedb.query.opencypher.ast.ListPredicateExpression#evaluate}) and from the
 * openCypher truth tables, where a predicate that is {@code null} for every element yields
 * {@code null}, not {@code false}/{@code true}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6673ListPredicateAggregation3vlTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:Person {name: 'A'})");
    database.command("opencypher", "CREATE (:Person {name: 'B'})");
    database.command("opencypher", "CREATE (:Person {name: 'C'})");
  }

  @Test
  void anyReturnsNullWhenEveryElementPredicateIsNullInsideAggregation() {
    // Every element's predicate is null (x.nonexistent > 0 -> null) over a non-empty collected list.
    final ResultSet rs = database.query("opencypher",
        "MATCH (v:Person) RETURN any(x IN collect(v) WHERE x.nonexistent > 0) AS r");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Object>getProperty("r")).isNull();
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void allReturnsNullWhenEveryElementPredicateIsNullInsideAggregation() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (v:Person) RETURN all(x IN collect(v) WHERE x.nonexistent > 0) AS r");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("r")).isNull();
  }

  @Test
  void noneReturnsNullWhenEveryElementPredicateIsNullInsideAggregation() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (v:Person) RETURN none(x IN collect(v) WHERE x.nonexistent > 0) AS r");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("r")).isNull();
  }

  @Test
  void singleReturnsNullWhenEveryElementPredicateIsNullInsideAggregation() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (v:Person) RETURN single(x IN collect(v) WHERE x.nonexistent > 0) AS r");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("r")).isNull();
  }

  @Test
  void anyStillTrueWhenAtLeastOneElementMatchesInsideAggregation() {
    // Sanity check: the fix must not turn a genuine match into null. Two elements are null, one is true.
    final ResultSet rs = database.query("opencypher",
        "MATCH (v:Person) RETURN any(x IN collect(v) WHERE x.name = 'A' OR x.nonexistent > 0) AS r");

    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("r")).isTrue();
  }

  @Test
  void noneStillFalseWhenAnElementMatchesInsideAggregation() {
    // Sanity check: a genuine true predicate among nulls must still make none() false, not null.
    final ResultSet rs = database.query("opencypher",
        "MATCH (v:Person) RETURN none(x IN collect(v) WHERE x.name = 'A' OR x.nonexistent > 0) AS r");

    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("r")).isFalse();
  }
}
