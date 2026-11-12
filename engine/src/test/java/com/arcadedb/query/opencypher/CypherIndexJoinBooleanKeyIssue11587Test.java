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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11587: a Cypher {@code CREATE INDEX} on a property no record has yet gives the index the key type
 * STRING without declaring the property, and the index stores a boolean under its text. A join on that property planned
 * as {@code IndexNestedLoopJoin} dropped every left row whose key was a boolean, so
 * {@code MATCH (a:A), (b:B) WHERE b.flag = a.flag} returned no row where the label scan finds them. A schemaless text
 * {@code 'true'} still differs from the boolean {@code true}: each answer is checked against the same question asked
 * without the index.
 */
class CypherIndexJoinBooleanKeyIssue11587Test extends TestHelper {

  private static final String[] QUERIES = {
      "MATCH (a:A), (b:B) WHERE b.flag = a.flag RETURN count(*) AS c",
      "MATCH (a:A) MATCH (b:B {flag: a.flag}) RETURN count(*) AS c",
      "MATCH (a:A), (b:B) WHERE b.flag = a.flag AND a.flag = true RETURN count(*) AS c" };

  private static final long[] EXPECTED = { 4L, 4L, 2L };

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE INDEX FOR (n:B) ON (n.flag)");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:A {flag: true}), (:A {flag: false}), (:A {flag: 'true'}), "
            + "(:B {flag: true}), (:B {flag: false}), (:B {flag: true}), (:B {flag: 'true'})"));
  }

  @Test
  void booleanKeysJoinThroughTheIndex() {
    for (int i = 0; i < QUERIES.length; i++) {
      assertThat(plan(QUERIES[i])).as(QUERIES[i]).contains("IndexNestedLoopJoin");
      assertThat(count(QUERIES[i])).as(QUERIES[i]).isEqualTo(EXPECTED[i]);
    }
  }

  @Test
  void sameAnswersWithoutTheIndex() {
    database.command("sql", "DROP INDEX `B[flag]`");
    for (int i = 0; i < QUERIES.length; i++) {
      assertThat(plan(QUERIES[i])).as(QUERIES[i]).doesNotContain("IndexNestedLoopJoin");
      assertThat(count(QUERIES[i])).as(QUERIES[i]).isEqualTo(EXPECTED[i]);
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
