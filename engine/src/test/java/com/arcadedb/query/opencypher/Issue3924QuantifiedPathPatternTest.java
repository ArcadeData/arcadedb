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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3924.
 * <p>
 * Cypher 25 Quantified Relationship (QR) syntax: the {@code +}, {@code *},
 * {@code {n}}, and {@code {n,m}} quantifiers placed after a relationship
 * pattern must repeat the relationship the specified number of times. This
 * is the Cypher 25 equivalent of variable-length path syntax
 * ({@code -[r*1..]->}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3924QuantifiedPathPatternTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-qpp-3924").create();
    database.getSchema().createVertexType("QuantPathTest");
    database.getSchema().createEdgeType("NEXT");

    database.transaction(() -> database.command("opencypher",
        "CREATE (a:QuantPathTest {name: 'A'})-[:NEXT]->(b:QuantPathTest {name: 'B'})-[:NEXT]->(c:QuantPathTest {name: 'C'})"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void quantifiedPathPatternWithPlus() {
    // +  means "one or more" (equivalent to *1..)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (start:QuantPathTest {name: 'A'})-[:NEXT]->+(end:QuantPathTest) RETURN end.name AS result ORDER BY result");

    final List<Object> names = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result r = resultSet.next();
      names.add(r.getProperty("result"));
    }
    assertThat(names).containsExactly("B", "C");
  }

  @Test
  void quantifiedPathPatternWithStar() {
    // *  means "zero or more" (equivalent to *0..)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (start:QuantPathTest {name: 'A'})-[:NEXT]->*(end:QuantPathTest) RETURN end.name AS result ORDER BY result");

    final List<Object> names = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result r = resultSet.next();
      names.add(r.getProperty("result"));
    }
    // 0-length path returns the start node itself, then 1-hop (B) and 2-hop (C)
    assertThat(names).containsExactly("A", "B", "C");
  }

  @Test
  void quantifiedPathPatternExactRepetition() {
    // {2} means exactly 2
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (start:QuantPathTest {name: 'A'})-[:NEXT]->{2}(end:QuantPathTest) RETURN end.name AS result ORDER BY result");

    final List<Object> names = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result r = resultSet.next();
      names.add(r.getProperty("result"));
    }
    assertThat(names).containsExactly("C");
  }

  @Test
  void quantifiedPathPatternRange() {
    // {1,2} means between 1 and 2
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (start:QuantPathTest {name: 'A'})-[:NEXT]->{1,2}(end:QuantPathTest) RETURN end.name AS result ORDER BY result");

    final List<Object> names = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result r = resultSet.next();
      names.add(r.getProperty("result"));
    }
    assertThat(names).containsExactly("B", "C");
  }
}
