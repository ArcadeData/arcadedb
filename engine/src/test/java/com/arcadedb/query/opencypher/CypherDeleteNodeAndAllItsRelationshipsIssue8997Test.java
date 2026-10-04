/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code MATCH (n)-[r]-() DELETE r, n} deletes a node bound to several relationships: the DeleteConnectedNode check runs once
 * every row is processed, not per row (#8997).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDeleteNodeAndAllItsRelationshipsIssue8997Test extends TestHelper {

  @BeforeEach
  void load() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (n:N {id: 1}), (n)-[:R]->(:N {id: 2}), (n)-[:R]->(:N {id: 3}),
               (m:N {id: 4}), (m)-[:R]->(:N {id: 5})""").close());
  }

  private List<String> remaining() {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:N) OPTIONAL MATCH (n)-[r]->() RETURN n.id AS id, count(r) AS out ORDER BY id")) {
      while (rs.hasNext()) {
        final var r = rs.next();
        out.add(r.<Object>getProperty("id") + ":" + r.<Object>getProperty("out"));
      }
    }
    return out;
  }

  @Test
  void deleteRelationshipsAndNodeWithTwoRelationships() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:N {id: 4})-[r]-() DELETE r, n").close());
    database.transaction(() -> database.command("opencypher", "MATCH (n:N {id: 1})-[r]-() DELETE r, n").close());
    assertThat(remaining()).containsExactly("2:0", "3:0", "5:0");
  }

  @Test
  void deleteWithOptionalMatch() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:N {id: 1}) OPTIONAL MATCH (n)-[r]-() DELETE n, r").close());
    assertThat(remaining()).containsExactly("2:0", "3:0", "4:1", "5:0");
  }

  @Test
  void nodeStillConnectedAfterEveryRowStillFails() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (n:N {id: 1})-[r]-() DELETE n").close()))
        .hasMessageContaining("DeleteConnectedNode")
        .isInstanceOf(CommandExecutionException.class);
    assertThat(remaining()).containsExactly("1:2", "2:0", "3:0", "4:1", "5:0");
  }

  @Test
  void selfLoopAndSharedNode() {
    database.transaction(() -> database.command("opencypher", "CREATE (x:S {id: 9})-[:L]->(x), (x)-[:L]->(:S {id: 10})").close());
    database.transaction(() -> database.command("opencypher", "MATCH (n:S {id: 9})-[r]-() DELETE r, n").close());
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:S) RETURN n.id AS id")) {
      assertThat(rs.stream().map(r -> r.<Object>getProperty("id")).toList()).containsExactly(10);
    }
  }

  @Test
  void sameIsolatedNodeBoundInTwoRows() {
    database.transaction(() -> database.command("opencypher", "CREATE (:I {id: 1}), (:I {id: 2})").close());
    database.transaction(() -> database.command("opencypher", "MATCH (a:I), (b:I) DELETE a, b").close());
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:I) RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  @Test
  void unidirectionalEdgeType() {
    database.getSchema().createVertexType("U");
    database.getSchema().buildEdgeType().withName("UE").withBidirectional(false).create();
    database.transaction(() -> database.command("opencypher", "CREATE (u:U {id: 1})-[:UE]->(:U {id: 2}), (u)-[:UE]->(:U {id: 3})").close());
    database.transaction(() -> database.command("opencypher", "MATCH (n:U {id: 1})-[r]-() DELETE r, n").close());
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:U) RETURN n.id AS id ORDER BY id")) {
      assertThat(rs.stream().map(r -> r.<Object>getProperty("id")).toList()).containsExactly(2, 3);
    }
  }

  @Test
  void insideAnOpenTransactionTheNodeIsDeletedWhenTheStatementEnds() {
    database.begin();
    database.command("opencypher", "MATCH (n:N {id: 1})-[r]-() DELETE r, n").close();
    database.commit();
    assertThat(remaining()).containsExactly("2:0", "3:0", "4:1", "5:0");
  }

  @Test
  void failingStatementWithoutAnOuterTransactionDeletesNothing() {
    // node 1 also has an edge of another type that the pattern does not match, so it is still connected at the end
    database.getSchema().createEdgeType("OTHER");
    database.transaction(() -> database.command("opencypher", "MATCH (a:N {id: 1}), (b:N {id: 5}) CREATE (a)-[:OTHER]->(b)").close());

    assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:N {id: 1})-[r:R]->() DELETE r, n").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("DeleteConnectedNode");
    assertThat(remaining()).containsExactly("1:3", "2:0", "3:0", "4:1", "5:0");
  }

  @Test
  void limitDownstreamStillAppliesTheWholeDelete() {
    database.command("opencypher", "MATCH (n:N {id: 1})-[r]-() DELETE r, n RETURN 1 AS one LIMIT 1").close();
    // the DELETE consumes its whole input before a downstream LIMIT can stop it
    assertThat(remaining()).containsExactly("2:0", "3:0", "4:1", "5:0");
  }

  @Test
  void failingStatementWithReturnLeavesTheGraphUnchanged() {
    database.getSchema().createEdgeType("OTHER");
    database.transaction(() -> database.command("opencypher", "MATCH (a:N {id: 1}), (b:N {id: 5}) CREATE (a)-[:OTHER]->(b)").close());

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (n:N {id: 1})-[r:R]->() DELETE r, n RETURN 1 AS one")) {
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(CommandExecutionException.class).hasMessageContaining("DeleteConnectedNode");
    assertThat(remaining()).containsExactly("1:3", "2:0", "3:0", "4:1", "5:0");
  }

  @Test
  void foreachDeleteOfNodeWithSeveralRelationships() {
    database.transaction(() -> database.command("opencypher",
        "MATCH (n:N {id: 1})-[r]-() WITH n, collect(r) AS rs FOREACH (x IN rs | DELETE x) DELETE n").close());
    assertThat(remaining()).containsExactly("2:0", "3:0", "4:1", "5:0");
  }
}
