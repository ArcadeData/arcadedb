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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for #12952: a damping factor, a probability, a decay, a threshold or a tolerance outside its domain, or NaN, was
 * accepted by the {@code algo.*} procedures and answered with meaningless numbers (a negative PageRank, a probability of 7.5). They are
 * refused by name with an {@link IllegalArgumentException} (HTTP 400) and the boundary values of each domain still run.
 */
class Issue12952AlgoNumericDomainsTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("opencypher", "CREATE (a:P {id: 1})-[:K]->(b:P {id: 2})-[:K]->(c:P {id: 3}), (a)-[:K]->(c)");
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "algo.pagerank", "algo.articlerank" })
  void rankProceduresRefuseADampingFactorOutsideZeroToOne(final String procedure) {
    for (final String value : new String[] { "2.0", "-0.1", "0.0/0.0", "1.0000001" })
      assertClientError("CALL " + procedure + "({dampingFactor: " + value + "}) YIELD node RETURN count(*)",
          procedure + "(): dampingFactor must be between 0.0 and 1.0, got");
    for (final String value : new String[] { "0.0/0.0", "-1", "-0.000001" })
      assertClientError("CALL " + procedure + "({tolerance: " + value + "}) YIELD node RETURN count(*)",
          procedure + "(): tolerance must be a finite number, not negative");
    assertClientError("CALL " + procedure + "({tolerance: $t}) YIELD node RETURN count(*)", Map.of("t", Double.POSITIVE_INFINITY),
        procedure + "(): tolerance must be a finite number, not negative");
    for (final String value : new String[] { "0.0", "1.0", "0.5" })
      assertThat(count("CALL " + procedure + "({dampingFactor: " + value + ", tolerance: 0.0}) YIELD node RETURN count(*) AS c")).as(value)
          .isEqualTo(3);
  }

  @Test
  void personalizedPageRankRefusesADampingFactorOutsideZeroToOne() {
    assertClientError("MATCH (a:P {id: 1}) CALL algo.personalizedPageRank(a, 'K', 2.0) YIELD nodeId RETURN count(*)",
        "algo.personalizedPageRank(): dampingFactor must be between 0.0 and 1.0, got 2.0");
    assertClientError("MATCH (a:P {id: 1}) CALL algo.personalizedPageRank(a, {dampingFactor: -0.5}) YIELD nodeId RETURN count(*)",
        "algo.personalizedPageRank(): dampingFactor must be between 0.0 and 1.0, got -0.5");
    assertClientError("MATCH (a:P {id: 1}) CALL algo.personalizedPageRank(a, 'K', 0.85, 20, 0.0/0.0) YIELD nodeId RETURN count(*)",
        "algo.personalizedPageRank(): tolerance must be a finite number, not negative");
    assertThat(count("MATCH (a:P {id: 1}) CALL algo.personalizedPageRank(a, 'K', 0.0, 20, 0.0) YIELD nodeId RETURN count(*) AS c")).isEqualTo(3);
    assertThat(count("MATCH (a:P {id: 1}) CALL algo.personalizedPageRank(a, {dampingFactor: 1.0}) YIELD nodeId RETURN count(*) AS c")).isEqualTo(3);
  }

  @Test
  void toleranceOfTheIterativeProceduresIsRefusedWhenNegativeOrNaN() {
    assertClientError("CALL algo.katz('K', 0.005, 10, 0.0/0.0) YIELD nodeId RETURN count(*)", "algo.katz(): tolerance must be a finite number");
    assertClientError("CALL algo.katz('K', 0.005, 10, -1) YIELD nodeId RETURN count(*)", "algo.katz(): tolerance must be a finite number");
    assertClientError("CALL algo.katz('K', 0.0/0.0) YIELD nodeId RETURN count(*)", "algo.katz(): alpha must be a finite number, not negative");
    assertClientError("CALL algo.katz('K', -0.5) YIELD nodeId RETURN count(*)", "algo.katz(): alpha must be a finite number, not negative");
    assertClientError("CALL algo.hits('K', 10, 0.0/0.0) YIELD node RETURN count(*)", "algo.hits(): tolerance must be a finite number");
    assertClientError("CALL algo.hits('K', 10, -1) YIELD node RETURN count(*)", "algo.hits(): tolerance must be a finite number");
    assertClientError("CALL algo.eigenvector('K', 'BOTH', 10, 0.0/0.0) YIELD node RETURN count(*)", "algo.eigenvector(): tolerance must be a finite number");
    assertClientError("CALL algo.eigenvector('K', 'BOTH', 10, -1) YIELD node RETURN count(*)", "algo.eigenvector(): tolerance must be a finite number");

    assertThat(count("CALL algo.katz('K', 0.0, 10, 0.0) YIELD nodeId RETURN count(*) AS c")).isEqualTo(3);
    assertThat(count("CALL algo.hits('K', 10, 0.0) YIELD node RETURN count(*) AS c")).isEqualTo(3);
    assertThat(count("CALL algo.eigenvector('K', 'BOTH', 10, 0.0) YIELD node RETURN count(*) AS c")).isEqualTo(3);
  }

  @Test
  void simRankRefusesADecayFactorOutsideZeroToOne() {
    for (final String value : new String[] { "5.0", "-1.0", "0.0/0.0" })
      assertClientError("MATCH (a:P {id: 1}), (b:P {id: 2}) CALL algo.simRank(a, b, 'K', " + value + ", 5) YIELD similarity RETURN similarity",
          "algo.simRank(): decayFactor must be between 0.0 and 1.0, got");
    for (final String value : new String[] { "0.0", "1.0" })
      assertThat(count("MATCH (a:P {id: 1}), (b:P {id: 2}) CALL algo.simRank(a, b, 'K', " + value + ", 5) YIELD similarity RETURN count(*) AS c"))
          .isEqualTo(1);
  }

  @Test
  void influenceMaximizationRefusesAProbabilityOutsideZeroToOne() {
    for (final String value : new String[] { "7.5", "-0.1", "0.0/0.0" })
      assertClientError("CALL algo.influenceMaximization(2, 'K', 5, " + value + ") YIELD nodeId RETURN count(*)",
          "algo.influenceMaximization(): propagationProbability must be between 0.0 and 1.0, got");
    for (final String value : new String[] { "0.0", "1.0" })
      assertThat(count("CALL algo.influenceMaximization(2, 'K', 5, " + value + ") YIELD nodeId RETURN count(*) AS c")).isEqualTo(2);
  }

  @Test
  void slpaRefusesAThresholdOutsideZeroToOne() {
    for (final String value : new String[] { "2.0", "-0.5", "0.0/0.0" })
      assertClientError("CALL algo.slpa({threshold: " + value + "}) YIELD node RETURN count(*)",
          "algo.slpa(): threshold must be between 0.0 and 1.0, got");
    for (final String value : new String[] { "0.0", "1.0" })
      assertThat(count("CALL algo.slpa({threshold: " + value + ", iterations: 5, seed: 1}) YIELD node RETURN count(*) AS c")).isEqualTo(3);
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private void assertClientError(final String cypher, final String message) {
    assertClientError(cypher, Map.of(), message);
  }

  private void assertClientError(final String cypher, final Map<String, Object> parameters, final String message) {
    final Throwable error = catchThrowable(() -> {
      try (final ResultSet rs = database.query("opencypher", cypher, parameters)) {
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
