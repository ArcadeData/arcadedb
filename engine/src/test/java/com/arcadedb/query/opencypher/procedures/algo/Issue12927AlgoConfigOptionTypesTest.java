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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for #12927: a config option of the wrong type ({@code direction}, {@code weightProperty}) was cast to
 * {@code String} blindly and a negative {@code topK} of {@code algo.voteRank} reached an array allocation, both answering a
 * raw JDK exception (HTTP 500) for the caller's mistake. They are refused by name with an {@link IllegalArgumentException}, which
 * the HTTP layer answers 400.
 */
class Issue12927AlgoConfigOptionTypesTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("opencypher", "CREATE (a:P {id: 1})-[:K {w: 1.0}]->(b:P {id: 2})-[:K {w: 2.0}]->(c:P {id: 3})");
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "algo.fastrp", "algo.graphsage", "algo.hashgnn", "algo.node2vec" })
  void embeddingProceduresRefuseANonStringDirection(final String procedure) {
    for (final String value : new String[] { "1", "true", "1.5", "[]", "{}" })
      assertClientError("CALL " + procedure + "({direction: " + value + "}) YIELD node RETURN count(*)",
          procedure + "(): direction must be a string, one of OUT, IN or BOTH");
  }

  @Test
  void maxKCutRefusesANonStringDirection() {
    assertClientError("CALL algo.maxKCut(2, {direction: 1}) YIELD node RETURN count(*)",
        "algo.maxKCut(): direction must be a string, one of OUT, IN or BOTH");
  }

  @ParameterizedTest
  @ValueSource(strings = { "algo.pagerank({weightProperty: %s})", "algo.louvain({weightProperty: %s})", "algo.maxKCut(2, {weightProperty: %s})" })
  void weightPropertyOptionRefusesANonString(final String call) {
    final String name = call.substring(0, call.indexOf('('));
    for (final String value : new String[] { "1", "true", "1.5", "[]", "{}" })
      assertClientError("CALL " + call.formatted(value) + " YIELD node RETURN count(*)", name + "(): weightProperty must be a string, got");
  }

  @Test
  void stringOptionsStillWork() {
    for (final String call : new String[] { "algo.fastrp({direction: 'out', dimensions: 4, iterations: 1, seed: 1})",
        "algo.graphsage({direction: 'IN', embeddingDimension: 4, layers: 1, seed: 1})",
        "algo.hashgnn({direction: 'BOTH', embeddingDimension: 8, iterations: 1, seed: 1})",
        "algo.node2vec({direction: 'OUT', embeddingDimension: 4, walkLength: 3, walksPerNode: 1, seed: 1})",
        "algo.maxKCut(2, {direction: 'BOTH', weightProperty: 'w', seed: 1})", "algo.pagerank({weightProperty: 'w'})",
        "algo.louvain({weightProperty: 'w'})", "algo.pagerank({weightProperty: null, direction: null})" })
      try (final ResultSet rs = database.query("opencypher", "CALL " + call + " YIELD node RETURN count(*) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").intValue()).as(call).isEqualTo(3);
      }
  }

  @Test
  void voteRankRefusesANegativeTopK() {
    assertClientError("CALL algo.voteRank(null, -1) YIELD nodeId RETURN count(*)", "algo.voteRank(): topK must not be negative, got -1");
    assertClientError("CALL algo.voteRank(null, -9223372036854775808) YIELD nodeId RETURN count(*)",
        "algo.voteRank(): topK must not be negative");
  }

  @Test
  void voteRankKeepsZeroAndHugeTopK() {
    try (final ResultSet rs = database.query("opencypher", "CALL algo.voteRank(null, 0) YIELD nodeId RETURN count(*) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isZero();
    }
    try (final ResultSet rs = database.query("opencypher", "CALL algo.voteRank(null, 9223372036854775807) YIELD nodeId RETURN count(*) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isPositive();
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
    final IllegalArgumentException clientError = CauseChain.find(error, IllegalArgumentException.class);
    assertThat(clientError).as(cypher + " -> " + error).isNotNull();
    assertThat(clientError.getMessage()).as(cypher).contains(message);
  }
}
