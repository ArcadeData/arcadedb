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
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the GQL FOR ... IN ... clause (issue #3365 section 1.2).
 * FOR is a synonym of UNWIND per ISO/IEC 39075:2024 (optional feature GQ10).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3365ForClauseTest {
  private Database database;
  private String   databasePath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    databasePath = "./target/databases/testopencypher-for-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("Person");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void forIterationOverLiteralList() {
    final ResultSet result = database.query("opencypher", "FOR x IN [1, 2, 3] RETURN x");
    final List<Integer> values = new ArrayList<>();
    while (result.hasNext())
      values.add(((Number) result.next().getProperty("x")).intValue());
    assertThat(values).containsExactly(1, 2, 3);
  }

  @Test
  void forIterationOverRange() {
    final ResultSet result = database.query("opencypher", "FOR n IN range(1, 5) RETURN n");
    final List<Integer> values = new ArrayList<>();
    while (result.hasNext())
      values.add(((Number) result.next().getProperty("n")).intValue());
    assertThat(values).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  void forIterationFollowedByCreate() {
    database.transaction(() -> {
      database.command("opencypher", "FOR name IN ['Alice', 'Bob', 'Carol'] CREATE (:Person {name: name})");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN count(n) AS c");
    assertThat(((Number) verify.next().getProperty("c")).longValue()).isEqualTo(3L);
  }

  @Test
  void forProducesSameResultsAsUnwind() {
    final ResultSet forResult = database.query("opencypher", "FOR x IN [10, 20, 30] RETURN x");
    final ResultSet unwindResult = database.query("opencypher", "UNWIND [10, 20, 30] AS x RETURN x");

    final List<Integer> forValues = new ArrayList<>();
    while (forResult.hasNext())
      forValues.add(((Number) forResult.next().getProperty("x")).intValue());

    final List<Integer> unwindValues = new ArrayList<>();
    while (unwindResult.hasNext())
      unwindValues.add(((Number) unwindResult.next().getProperty("x")).intValue());

    assertThat(forValues).isEqualTo(unwindValues).containsExactly(10, 20, 30);
  }
}
