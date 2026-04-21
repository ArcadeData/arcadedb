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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3923.
 * <p>
 * Cypher 25 dynamic label expressions in node/relationship patterns
 * using the {@code $(expression)} syntax must resolve labels at runtime
 * from the given expression.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3923DynamicLabelTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-dynamic-label-3923").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void dynamicLabelMatchingFromWithBinding() {
    database.command("opencypher", "CREATE (n:DynamicLabelTest {name: 'test'})");

    final ResultSet rs = database.query("opencypher",
        "WITH 'DynamicLabelTest' AS label MATCH (n:$(label)) RETURN labels(n) AS result");
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    final List<String> labels = r.getProperty("result");
    assertThat(labels).containsExactly("DynamicLabelTest");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void dynamicLabelMatchingFromParameter() {
    database.command("opencypher", "CREATE (n:DynamicLabelTest {name: 'test'})");

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:$($label)) RETURN labels(n) AS result",
        Map.of("label", "DynamicLabelTest"));
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    final List<String> labels = r.getProperty("result");
    assertThat(labels).containsExactly("DynamicLabelTest");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void dynamicLabelMatchingNonExistingLabelReturnsNothing() {
    database.command("opencypher", "CREATE (n:DynamicLabelTest {name: 'test'})");

    final ResultSet rs = database.query("opencypher",
        "WITH 'DoesNotExist' AS label MATCH (n:$(label)) RETURN n");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void dynamicLabelMatchingCombinedWithStaticLabel() {
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B").addSuperType("A");
    database.command("opencypher", "CREATE (n:B {name: 'ab'})");
    database.command("opencypher", "CREATE (n:A {name: 'aOnly'})");

    // Requires ALL labels (static + dynamic) to match: only the B node should match
    final ResultSet rs = database.query("opencypher",
        "WITH 'B' AS label MATCH (n:A:$(label)) RETURN n.name AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("ab");
    assertThat(rs.hasNext()).isFalse();
  }
}
