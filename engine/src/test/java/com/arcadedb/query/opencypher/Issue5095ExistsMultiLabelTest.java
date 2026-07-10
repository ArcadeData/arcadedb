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
 * Regression test for GitHub issue #5095.
 * <p>
 * {@code exists((n:A:B)-->(:Node))} must enforce the label conjunction {@code A:B}
 * on the already-bound start variable {@code n}, not only the traversal existence.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5095ExistsMultiLabelTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-5095-exists-multilabel").create();
    database.transaction(() ->
      // n1: labels A+B, no outgoing edge
      // n2: labels A+B, has outgoing edge   -> should match
      // n3: label A only, has outgoing edge -> should NOT match
      // n4: label B only, has outgoing edge -> should NOT match
      database.command("opencypher", """
          CREATE \
          (n1:Node:A:B {id: 1}),\
          (n2:Node:A:B {id: 2})-[:LINK]->(n1),\
          (n3:Node:A {id: 3})-[:LINK]->(n1),\
          (n4:Node:B {id: 4})-[:LINK]->(n1)"""));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private List<Integer> runIds(final String query) {
    final List<Integer> ids = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", query);
    while (rs.hasNext()) {
      final Result r = rs.next();
      ids.add(((Number) r.getProperty("id")).intValue());
    }
    return ids;
  }

  @Test
  void existsWithMultiLabelBoundStartEnforcesConjunction() {
    final List<Integer> ids = runIds(
        "MATCH (n:Node) WHERE exists((n:A:B)-->(:Node)) RETURN n.id AS id ORDER BY id");
    assertThat(ids).containsExactly(2);
  }

  @Test
  void controlExplicitPreFilter() {
    final List<Integer> ids = runIds(
        "MATCH (n:Node:A:B) WHERE exists((n)-->(:Node)) RETURN n.id AS id ORDER BY id");
    assertThat(ids).containsExactly(2);
  }

  @Test
  void controlSimpleExistsWithoutLabelConstraint() {
    final List<Integer> ids = runIds(
        "MATCH (n:Node) WHERE exists((n)-->(:Node)) RETURN n.id AS id ORDER BY id");
    assertThat(ids).containsExactly(2, 3, 4);
  }

  @Test
  void existsWithSingleExtraLabelBoundStart() {
    // Only nodes that also have label A and an outgoing edge: n2 (A+B) and n3 (A only)
    final List<Integer> ids = runIds(
        "MATCH (n:Node) WHERE exists((n:A)-->(:Node)) RETURN n.id AS id ORDER BY id");
    assertThat(ids).containsExactly(2, 3);
  }

  @Test
  void notExistsWithMultiLabelBoundStart() {
    // NOT exists((n:A:B)-->) is true for nodes that do NOT have both labels with an outgoing edge.
    // n1 (A+B, no edge), n3 (A only), n4 (B only) satisfy NOT; n2 does not.
    final List<Integer> ids = runIds(
        "MATCH (n:Node) WHERE NOT exists((n:A:B)-->(:Node)) RETURN n.id AS id ORDER BY id");
    assertThat(ids).containsExactly(1, 3, 4);
  }
}
