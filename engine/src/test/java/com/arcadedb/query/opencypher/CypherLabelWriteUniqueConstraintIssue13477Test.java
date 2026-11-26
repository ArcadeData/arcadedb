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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13477: a Cypher label write rewrites the vertex under the composite type, which inherits the UNIQUE index of the
 * original label. The copy used to be saved while the original was still live in the same transaction, so the copy's key
 * met the original's key in the inherited index and the statement failed with a {@link DuplicatedKeyException}. Neo4j
 * accepts these statements. The original (and its edges) is now deleted before the copy (and its edges) is written.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherLabelWriteUniqueConstraintIssue13477Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Item) REQUIRE n.id IS UNIQUE");
  }

  @Test
  void mergeThenSetLabelOnAKeyCreatedInTheSameStatement() {
    database.command("opencypher", """
        UNWIND [{id:'item-0', v:0}, {id:'item-1', v:1}] AS item
        MERGE (i:Item{id: item.id}) SET i.v = item.v, i:Container""");

    assertItemsAreContainers(Map.of("item-0", 0L, "item-1", 1L));
    assertUniquenessStillEnforced();
  }

  @Test
  void mergeThenSetLabelInsideAnExplicitTransaction() {
    database.transaction(() -> database.command("opencypher", """
        UNWIND [{id:'item-0', v:0}, {id:'item-1', v:1}] AS item
        MERGE (i:Item{id: item.id}) SET i.v = item.v, i:Container"""));

    assertItemsAreContainers(Map.of("item-0", 0L, "item-1", 1L));
    assertUniquenessStillEnforced();
  }

  @Test
  void setLabelOnCommittedVerticesUpdatedInTheSameStatement() {
    database.command("opencypher", "CREATE (:Item {id:'item-0', v:0}), (:Item {id:'item-1', v:1})");

    database.command("opencypher", "MATCH (i:Item) SET i.v = i.v + 10, i:Container");

    assertItemsAreContainers(Map.of("item-0", 10L, "item-1", 11L));
    assertUniquenessStillEnforced();
  }

  @Test
  void setLabelOnAVertexCreatedEarlierInTheSameTransaction() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Item {id:'item-0', v:0}), (:Item {id:'item-1', v:1})");
      database.command("opencypher", "MATCH (i:Item) SET i:Container");
    });

    assertItemsAreContainers(Map.of("item-0", 0L, "item-1", 1L));
    assertUniquenessStillEnforced();
  }

  @Test
  void edgesWithAUniqueKeyAndASelfLoopAreMigratedInTheSameTransaction() {
    database.command("sql", "CREATE EDGE TYPE LINK IF NOT EXISTS");
    database.command("sql", "CREATE PROPERTY LINK.key IF NOT EXISTS STRING");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON LINK (key) UNIQUE");

    database.transaction(() -> {
      database.command("opencypher", """
          CREATE (a:Item {id:'item-0', v:0}), (b:Other {name:'b'}), (c:Other {name:'c'})
          CREATE (a)-[:LINK {key:'out'}]->(b), (c)-[:LINK {key:'in'}]->(a), (a)-[:LINK {key:'self'}]->(a)""");
      database.command("opencypher", "MATCH (i:Item {id:'item-0'}) SET i:Container");
    });

    assertItemsAreContainers(Map.of("item-0", 0L));
    final List<String> links = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s)-[r:LINK]->(t) RETURN r.key AS key, labels(s) AS s, labels(t) AS t ORDER BY key")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        links.add(row.getProperty("key") + ":" + sorted(row.getProperty("s")) + "->" + sorted(row.getProperty("t")));
      }
    }
    assertThat(links).containsExactly(//
        "in:[Other]->[Container, Item]",//
        "out:[Container, Item]->[Other]",//
        "self:[Container, Item]->[Container, Item]");

    // the unique index of the edges still holds every key once and still refuses a duplicate
    assertThat(count("MATCH ()-[r:LINK]->() RETURN count(r) AS c")).isEqualTo(3L);
    assertRefusedAsDuplicate("MATCH (b:Other {name:'b'}), (c:Other {name:'c'}) CREATE (b)-[:LINK {key:'self'}]->(c)");
  }

  @Test
  void edgesWithAUniqueKeyOfAVertexWithoutUniqueKeysAreMigratedInTheSameTransaction() {
    database.command("sql", "CREATE EDGE TYPE LINK IF NOT EXISTS");
    database.command("sql", "CREATE PROPERTY LINK.key IF NOT EXISTS STRING");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON LINK (key) UNIQUE");

    database.transaction(() -> {
      database.command("opencypher", """
          CREATE (a:Plain {name:'a'}), (b:Other {name:'b'})
          CREATE (a)-[:LINK {key:'out'}]->(b), (b)-[:LINK {key:'in'}]->(a), (a)-[:LINK {key:'self'}]->(a)""");
      database.command("opencypher", "MATCH (p:Plain) SET p:Tagged");
    });

    assertThat(count("MATCH (p:Plain:Tagged)-[r:LINK]-() RETURN count(DISTINCT r) AS c")).isEqualTo(3L);
    assertThat(count("MATCH ()-[r:LINK]->() RETURN count(r) AS c")).isEqualTo(3L);
    assertRefusedAsDuplicate("MATCH (b:Other {name:'b'}) CREATE (b)-[:LINK {key:'out'}]->(b)");
  }

  private void assertItemsAreContainers(final Map<String, Long> expected) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (i:Item) RETURN i.id AS id, i.v AS v, labels(i) AS labels")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(sorted(row.getProperty("labels"))).containsExactly("Container", "Item");
        final String id = row.getProperty("id");
        assertThat(((Number) row.getProperty("v")).longValue()).isEqualTo(expected.get(id));
        rows.add(id);
      }
    }
    assertThat(rows).containsExactlyInAnyOrderElementsOf(expected.keySet());
    assertThat(count("MATCH (c:Container) RETURN count(c) AS c")).isEqualTo((long) expected.size());
  }

  private void assertUniquenessStillEnforced() {
    assertRefusedAsDuplicate("CREATE (:Item {id:'item-0'})");
    assertRefusedAsDuplicate("CREATE (:Container:Item {id:'item-0'})");
    assertThat(count("MATCH (i:Item {id:'item-0'}) RETURN count(i) AS c")).isEqualTo(1L);
  }

  private void assertRefusedAsDuplicate(final String statement) {
    assertThatThrownBy(() -> database.command("opencypher", statement)).satisfies(e -> {
      Throwable cause = e;
      while (cause != null && !(cause instanceof DuplicatedKeyException))
        cause = cause.getCause();
      assertThat(cause).as("a DuplicatedKeyException in the cause chain of %s", e).isNotNull();
    });
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private static List<String> sorted(final Object labels) {
    final List<String> list = new ArrayList<>();
    for (final Object label : (List<?>) labels)
      list.add(label.toString());
    list.sort(null);
    return list;
  }
}
