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
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for #12847: caller mistakes in a function or procedure argument answered HTTP 500. {@code trim(null, ...)}
 * answers null like any function with a null input; the others raise one of the two exception types the HTTP layer answers
 * 400 ({@link InvalidArgumentException} or {@link IllegalArgumentException}), naming the argument or the missing index.
 */
class Issue12847FunctionArgumentMistakesTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE PROPERTY P.id INTEGER");
      database.command("sql", "CREATE PROPERTY P.name STRING");
      database.command("sql", "CREATE INDEX ON P (name) FULL_TEXT");
      database.command("sql", "CREATE INDEX ON P (id) UNIQUE");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE PROPERTY K.x STRING");
      database.command("sql", "CREATE INDEX ON K (x) NOTUNIQUE");
      database.command("opencypher", "CREATE (a:P {id: 1, name: 'alpha'})-[:K {x: 'a'}]->(:P {id: 2, name: 'beta'})");
    });
  }

  @Test
  void trimWithANullModeAnswersNull() {
    try (final ResultSet rs = database.query("opencypher", "RETURN trim(null, 'P', 'x') AS v")) {
      assertThat(rs.next().<Object>getProperty("v")).isNull();
    }
    try (final ResultSet rs = database.query("opencypher", "RETURN trim('xPx', 'P', 'x') AS v")) {
      assertThat(rs.next().<Object>getProperty("v")).isEqualTo("x");
    }
  }

  @Test
  void randomWalkRefusesANonNumberStepCount() {
    assertClientError("MATCH (n:P {id: 1}) CALL algo.randomWalk(n, 'x', 'K') YIELD * RETURN count(*)",
        "algo.randomWalk(): steps must be a number, got String");
    assertClientError("MATCH (n:P {id: 1}) CALL algo.randomWalk(n, [[1]], [null]) YIELD * RETURN count(*)",
        "algo.randomWalk(): steps must be a number");
    assertClientError("MATCH (n:P {id: 1}) CALL algo.randomWalk(n, null, 'K') YIELD * RETURN count(*)",
        "algo.randomWalk(): steps cannot be null");
  }

  @Test
  void facetSearchValuesWithoutAColumnarIndexIsAClientError() {
    assertClientError("RETURN facet.searchValues('P', 'name', 'a', 5) AS v", "facet.searchValues: no COLUMNAR index on P.name");
    assertClientError("RETURN facet.searchValues('K', '', {}, 0) AS v", "facet.searchValues: no COLUMNAR index on K.");
  }

  @Test
  void fullTextQueryRelationshipsRefusesANullArgumentAndAWrongIndex() {
    assertClientError("CALL db.index.fulltext.queryRelationships('K[x]', null) YIELD * RETURN count(*)", "queryString cannot be null");
    assertClientError("CALL db.index.fulltext.queryRelationships(null, 'a') YIELD * RETURN count(*)", "indexName cannot be null");
    assertClientError("CALL db.index.fulltext.queryRelationships('P[id]', 'a') YIELD * RETURN count(*)",
        "Index 'P[id]' is not a full-text index");
  }

  @Test
  void fullTextSearchIndexOnAWrongIndexIsAClientError() {
    assertClientError("CALL fulltext.searchIndex('P[id]', 'alpha') YIELD * RETURN count(*)", "Index 'P[id]' is not a full-text index");
    try (final ResultSet rs = database.query("opencypher", "CALL fulltext.searchIndex('P[name]', 'alpha') YIELD * RETURN count(*) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    }
  }

  private void assertClientError(final String cypher, final String message) {
    final Throwable error = catchThrowable(() -> {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
      }
    });
    assertThat(error).as(cypher).isNotNull();
    final Throwable clientError = CauseChain.find(error, InvalidArgumentException.class) != null ?
        CauseChain.find(error, InvalidArgumentException.class) :
        CauseChain.find(error, IllegalArgumentException.class);
    assertThat(clientError).as(cypher + " -> " + error).isNotNull();
    assertThat(clientError.getMessage()).as(cypher).contains(message);
  }
}
