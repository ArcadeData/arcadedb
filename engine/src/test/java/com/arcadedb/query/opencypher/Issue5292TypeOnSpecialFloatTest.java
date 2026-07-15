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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behaviour tests for issue #5292, which reported that {@code type()} "crashes" on NaN and Infinity
 * FLOAT values while allegedly returning {@code FLOAT} for finite floats such as {@code type(1.5)}.
 * <p>
 * That premise does not hold. In openCypher (and in Neo4j, the reference implementation) {@code type()}
 * is the <i>relationship-type</i> function: it takes a RELATIONSHIP and returns its type name, e.g.
 * {@code KNOWS}. Passing any non-relationship value is a TypeError regardless of the value, so
 * {@code type(1.5)} is rejected exactly like {@code type(sqrt(-1))} - the reported "control query"
 * fails too, and no NaN/Infinity-specific behaviour exists. The function that returns {@code FLOAT}
 * for a value is {@code valueType()}, which ArcadeDB implements and which handles NaN and Infinity.
 * <p>
 * These tests pin that contract: {@code type()} is uniformly a client-side TypeError for every float
 * (finite or special), {@code valueType()} classifies NaN/Infinity as FLOAT, the special values
 * themselves evaluate to the correct Double, and {@code type()} still works on real relationships.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5292TypeOnSpecialFloatTest extends TestHelper {

  /** The reporter's queries, plus the finite-float "control" query they claimed returned FLOAT. */
  private static final String[] TYPE_ON_FLOAT_QUERIES = {
      "RETURN type(sqrt(-1)) AS result",
      "RETURN type(log(0)) AS result",
      "RETURN type(exp(10000)) AS result",
      "RETURN type(1.0 / 0.0) AS result",
      "RETURN type(1.5) AS result",
  };

  /**
   * type() rejects every float argument identically. The special values are not a special case: the
   * reporter's own control query, type(1.5), fails the same way, so there is no NaN/Infinity bug.
   */
  @Test
  void typeOnAnyFloatIsAUniformTypeError() {
    for (final String query : TYPE_ON_FLOAT_QUERIES)
      assertThatThrownBy(() -> database.query("cypher", query).next())
          .as("type() must reject the float argument in: %s", query)
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("relationship");
  }

  /** The TypeError must point the user at valueType(), the function that answers what they asked. */
  @Test
  void typeErrorSuggestsValueType() {
    assertThatThrownBy(() -> database.query("cypher", "RETURN type(sqrt(-1)) AS result").next())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("valueType()");
  }

  /** valueType() - the correct function - classifies NaN and both infinities as FLOAT, like Neo4j. */
  @Test
  void valueTypeClassifiesSpecialFloatsAsFloat() {
    final String[] queries = {
        "RETURN valueType(sqrt(-1)) AS result",
        "RETURN valueType(log(0)) AS result",
        "RETURN valueType(exp(10000)) AS result",
        "RETURN valueType(1.0 / 0.0) AS result",
        "RETURN valueType(1.5) AS result",
    };
    for (final String query : queries) {
      final ResultSet rs = database.query("cypher", query);
      assertThat(rs.hasNext()).as("no row for: %s", query).isTrue();
      assertThat((String) rs.next().getProperty("result")).as("valueType for: %s", query).isEqualTo("FLOAT NOT NULL");
    }
  }

  /** The special values evaluate to the correct Double, matching Neo4j's NaN / -Infinity / Infinity. */
  @Test
  void specialFloatsEvaluateToTheCorrectDouble() {
    final ResultSet rs = database.query("cypher",
        "RETURN sqrt(-1) AS nan, log(0) AS negInf, exp(10000) AS posInf, 1.0 / 0.0 AS divInf");
    assertThat(rs.hasNext()).isTrue();
    final var row = rs.next();
    assertThat((Double) row.getProperty("nan")).isNaN();
    assertThat((Double) row.getProperty("negInf")).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat((Double) row.getProperty("posInf")).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat((Double) row.getProperty("divInf")).isEqualTo(Double.POSITIVE_INFINITY);
  }

  /** type() keeps doing its actual job: returning the type name of a relationship. */
  @Test
  void typeOnRelationshipReturnsTheRelationshipType() {
    database.command("cypher", "CREATE (:Person {name: 'a'})-[:KNOWS]->(:Person {name: 'b'})");
    final ResultSet rs = database.query("cypher", "MATCH ()-[r:KNOWS]->() RETURN type(r) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((String) rs.next().getProperty("result")).isEqualTo("KNOWS");
  }
}
