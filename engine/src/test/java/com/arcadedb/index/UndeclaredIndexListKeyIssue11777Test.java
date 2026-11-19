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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #11777: an openCypher index on a property nothing declares is STRING-keyed, and a list used to be keyed
 * by {@code List.toString()}, so {@code [1.5, 2.0]} and {@code [1.5, 2]} (equal under Cypher {@code =}) were two keys. An
 * index seek with the other spelling missed the node, MERGE created a duplicate and a UNIQUE constraint admitted both. The
 * key of a list on that path is now its canonical text (#11578, {@code Type.canonicalListKey}); these tests drive the
 * Cypher entry points the issue reports.
 */
class UndeclaredIndexListKeyIssue11777Test extends TestHelper {

  @Test
  void seekFindsTheListWhateverSpellingItsNumbersHave() {
    database.transaction(() -> database.command("opencypher", "CREATE (:K {k:[1.5, 2.0], id:'stored'})"));
    database.command("opencypher", "CREATE INDEX FOR (n:K) ON (n.k)");
    assertThat(database.getSchema().getType("K").getPropertyIfExists("k")).as("the property stays undeclared").isNull();

    assertThat(ids("RETURN [1.5, 2.0] = [1.5, 2] AS eq", "eq", Map.of())).containsExactly(true);
    // the scan, with no index usable on a computed value
    assertThat(ids("MATCH (n:K) WITH n, [x IN n.k | x] AS k WHERE k = [1.5, 2] RETURN n.id AS id", "id", Map.of()))
        .containsExactly("stored");
    // the index seeks: a parameter, a literal and an inline map
    assertThat(ids("MATCH (n:K) WHERE n.k = $p RETURN n.id AS id", "id", Map.of("p", List.of(1.5, 2)))).containsExactly("stored");
    assertThat(ids("MATCH (n:K) WHERE n.k = $p RETURN n.id AS id", "id", Map.of("p", List.of(1.5d, 2L)))).containsExactly("stored");
    assertThat(ids("MATCH (n:K) WHERE n.k = [1.5, 2] RETURN n.id AS id", "id", Map.of())).containsExactly("stored");
    assertThat(ids("MATCH (n:K {k:[1.5, 2]}) RETURN n.id AS id", "id", Map.of())).containsExactly("stored");
    assertThat(ids("MATCH (n:K {k:[1.5, 2.0]}) RETURN n.id AS id", "id", Map.of())).containsExactly("stored");
  }

  @Test
  void mergeWithTheOtherSpellingMatchesTheIndexedNode() {
    database.transaction(() -> database.command("opencypher", "CREATE (:K {k:[1.5, 2.0], id:'stored'})"));
    database.command("opencypher", "CREATE INDEX FOR (n:K) ON (n.k)");

    database.transaction(() -> database.command("opencypher", "MERGE (n:K {k:[1.5, 2]}) ON CREATE SET n.id = 'dup'"));
    database.transaction(() -> database.command("opencypher", "MERGE (n:K {k:$p}) ON CREATE SET n.id = 'dup2'",
        Map.of("p", List.of(1.5f, 2))));
    assertThat(ids("MATCH (n:K) RETURN n.id AS id ORDER BY id", "id", Map.of())).containsExactly("stored");
    assertThat(database.getSchema().getIndexByName("K[k]").countEntries()).isEqualTo(1L);
  }

  @Test
  void uniqueConstraintRefusesAnEqualListWithTheOtherSpelling() {
    database.transaction(() -> database.command("opencypher", "CREATE (:K {k:[1.5, 2.0], id:'stored'})"));
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:K) REQUIRE n.k IS UNIQUE");

    // MERGE finds the stored node through the unique index instead of creating a duplicate
    database.transaction(() -> database.command("opencypher", "MERGE (n:K {k:[1.5, 2]}) ON CREATE SET n.id = 'dup'"));
    assertThat(ids("MATCH (n:K) RETURN n.id AS id ORDER BY id", "id", Map.of())).containsExactly("stored");

    // a CREATE of either spelling, and of nested equal lists, collides with the stored key
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "CREATE (n:K {k:[1.5, 2], id:'dup2'})"))).isInstanceOf(DuplicatedKeyException.class);
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "CREATE (n:K {k:[1.5, 2.0], id:'dup3'})"))).isInstanceOf(DuplicatedKeyException.class);

    database.transaction(() -> database.command("opencypher", "CREATE (:K {k:[[1, 2.0], 3], id:'nested'})"));
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "CREATE (n:K {k:[[1.0, 2], 3.0], id:'nested2'})"))).isInstanceOf(
        DuplicatedKeyException.class);

    assertThat(ids("MATCH (a:K), (b:K) WHERE a.id < b.id AND a.k = b.k RETURN a.id AS id", "id", Map.of())).isEmpty();
    assertThat(ids("MATCH (n:K) RETURN n.id AS id ORDER BY id", "id", Map.of())).containsExactly("nested", "stored");
  }

  private List<Object> ids(final String query, final String column, final Map<String, Object> params) {
    final List<Object> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty(column));
    }
    return ids;
  }
}
