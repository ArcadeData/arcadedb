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
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code SET n = map} replaces every property of the record, so a property whose name starts with {@code @} goes too unless the name
 * is reserved record metadata ({@code MutableDocument.isReservedName}: {@code @rid}, {@code @type}, {@code @cat}, {@code @in},
 * {@code @out}, {@code @props}, {@code @this}). Any other {@code @}-prefixed name is an ordinary property, written and read back like the
 * rest (issue #9799) - and replaced like the rest: the replace used to skip every name that starts with {@code @}, so
 * {@code SET n.`@x` = 1} followed by {@code SET n = {id: 1}} kept {@code @x}. Neo4j 5.26 answers every statement below as asserted.
 */
class CypherSetReplaceMapAtPrefixedPropertyTest extends TestHelper {

  @SuppressWarnings("unchecked")
  private Map<String, Object> run(final String cypher, final String column) {
    final Map<String, Object>[] properties = new Map[1];
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", cypher);
      final Result row = rs.next();
      properties[0] = new TreeMap<>((Map<String, Object>) row.getProperty(column));
      rs.close();
    });
    return properties[0];
  }

  private QueryStatistics statsOf(final String cypher) {
    final QueryStatistics[] stats = new QueryStatistics[1];
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", cypher);
      while (rs.hasNext())
        rs.next();
      stats[0] = rs.getStatistics().orElseThrow();
      rs.close();
    });
    return stats[0];
  }

  @Test
  void replaceMapRemovesAtPrefixedPropertiesOfANode() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:T {id: 1}) SET n.`@x` = 1, n.`@` = 2").close());

    final Map<String, Object> properties = run("MATCH (n:T {id: 1}) SET n = {id: 1, k: 1} RETURN properties(n) AS p", "p");

    assertThat(properties.keySet()).containsExactly("id", "k");
  }

  @Test
  void replaceMapKeepsTheAtPrefixedKeysItWrites() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:T {id: 1, `@old`: 1})").close());

    final Map<String, Object> properties = run("MATCH (n:T {id: 1}) SET n = {id: 1, `@new`: 2} RETURN properties(n) AS p", "p");

    assertThat(properties.keySet()).containsExactly("@new", "id");
  }

  @Test
  void replaceMapOnAJustCreatedNodeRemovesAtPrefixedPropertiesToo() {
    // the folded form: the SET is written onto the node the CREATE is about to save (NewNodeSetFolding)
    final Map<String, Object> properties = run("CREATE (n:V {`@a`: 1, id: 3}) SET n = {id: 3} RETURN properties(n) AS p", "p");

    assertThat(properties.keySet()).containsExactly("id");
  }

  @Test
  void replaceMapRemovesAtPrefixedPropertiesOfARelationship() {
    // Graphiti's SET e = edge
    database.transaction(() -> database.command("opencypher", "CREATE (:T {id: 5})-[:E {`@attr`: 'x', uuid: 'u1'}]->(:T {id: 6})").close());

    final Map<String, Object> properties = run("MATCH ()-[e:E]->() SET e = {uuid: 'u1', fact: 'f'} RETURN properties(e) AS p", "p");

    assertThat(properties.keySet()).containsExactly("fact", "uuid");
  }

  @Test
  void replaceMapCountsTheRemovedAtPrefixedPropertyLikeAnyOther() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:T {id: 1, `@x`: 1})").close());

    // id is written (1) and @x removed (1), as Neo4j counts them
    assertThat(statsOf("MATCH (n:T {id: 1}) SET n = {id: 1}").getPropertiesSet()).isEqualTo(2);
    assertThat(statsOf("CREATE (n:U {id: 1, `@x`: 1}) SET n = {id: 1}").getPropertiesSet()).isEqualTo(4);
  }
}
