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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8097: {@code db.index.vector.queryNodes} resolves a vertex-identifier key by hand-building a
 * {@code SELECT}, and spliced the three schema names it needs - the index's type, its vector property and its
 * id property - into that text with no quoting at all.
 * <p>
 * All three names come from the index metadata rather than from the procedure's arguments, which is what makes
 * this narrower than the {@code merge.node} splice of #8072. It is not narrow enough to be safe: a type name is
 * a Cypher label and openCypher creates a type for whatever label the query spells, so
 * {@code CREATE (:`Person Node` ...)} yields {@code SELECT vec FROM Person Node WHERE id = ?}, which does not
 * parse. The same holds for any of the three names carrying a space, colliding with a SQL keyword, or starting
 * with a digit.
 * <p>
 * The key itself is bound as a parameter, so this was a broken query rather than an injection. Each test below
 * puts an unquotable name in exactly ONE of the three positions, so a fix that quotes only some of them cannot
 * pass the set.
 * <p>
 * The lookup has two entry points, not one. {@code vector.neighbors} is the ArcadeDB-native SQL function onto the
 * same by-id resolution and carried a byte-for-byte copy of the splice; the grep that establishes there is no third
 * is {@code grep -rn 'getIdPropertyName()' engine/src/main/java server/src/main/java}, which answers with the
 * declaration in {@code LSMVectorIndex} and exactly these two callers. Both are driven below, because fixing only
 * the Neo4j-compatible procedure is the same half-fix #7057 called out on this pair.
 */
class CypherVectorQueryNodesIdentifierQuotingIssue8097Test extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int NODES      = 12;

  @Override
  public void beginTest() {
    // 1. the TYPE name is unquotable; the two property names are ordinary.
    createVectorType("Person Node", "uuid", "embedding");
    // 2. the VECTOR PROPERTY name is unquotable; the type and id names are ordinary.
    createVectorType("SpacedVectorProperty", "uuid", "name embedding");
    // 3. the ID PROPERTY name is unquotable; the type and vector names are ordinary.
    createVectorType("SpacedIdProperty", "external id", "embedding");
    // 4. all three ordinary: the fix must not change the answer on the common case.
    createVectorType("Plain", "uuid", "embedding");
  }

  /**
   * The shape the issue reports: an openCypher label carrying a space. Before the fix the generated text was
   * {@code SELECT embedding FROM Person Node WHERE uuid = ? LIMIT 1}, so the lookup threw rather than resolving
   * the key to that vertex's vector.
   */
  @Test
  void aVertexIdentifierKeyResolvesWhenTheTypeNameCarriesASpace() {
    assertNearestIsSelf("Person Node", "embedding", "uuid", "u3");
  }

  /** The indexed property lands in the {@code SELECT} list, and is spliced by the same statement. */
  @Test
  void aVertexIdentifierKeyResolvesWhenTheVectorPropertyNameCarriesASpace() {
    assertNearestIsSelf("SpacedVectorProperty", "name embedding", "uuid", "u3");
  }

  /** The id property lands in the {@code WHERE} clause, the third and last of the spliced names. */
  @Test
  void aVertexIdentifierKeyResolvesWhenTheIdPropertyNameCarriesASpace() {
    assertNearestIsSelf("SpacedIdProperty", "embedding", "external id", "u3");
  }

  /**
   * The regression guard. Quoting a name that never needed it must return exactly the answer the raw splice
   * returned, or the fix trades one broken lookup for another.
   */
  @Test
  void aVertexIdentifierKeyStillResolvesWhenEveryNameIsOrdinary() {
    assertNearestIsSelf("Plain", "embedding", "uuid", "u7");
  }

  /**
   * {@code vector.neighbors} is the SQL entry point onto the same by-id resolution, reachable from SQL directly and
   * from Cypher as {@code CALL vector.neighbors(...)}. An unquotable type name broke it identically.
   */
  @Test
  void vectorNeighborsResolvesAVertexIdentifierKeyWhenTheTypeNameCarriesASpace() {
    assertNeighborsNearestIsSelf("Person Node", "embedding", "uuid", "u3");
  }

  /** The indexed property, in {@code vector.neighbors}'s copy of the {@code SELECT} list. */
  @Test
  void vectorNeighborsResolvesAVertexIdentifierKeyWhenTheVectorPropertyNameCarriesASpace() {
    assertNeighborsNearestIsSelf("SpacedVectorProperty", "name embedding", "uuid", "u3");
  }

  /** The id property, in {@code vector.neighbors}'s copy of the {@code WHERE} clause. */
  @Test
  void vectorNeighborsResolvesAVertexIdentifierKeyWhenTheIdPropertyNameCarriesASpace() {
    assertNeighborsNearestIsSelf("SpacedIdProperty", "embedding", "external id", "u3");
  }

  /** The regression guard on the SQL entry point: ordinary names must answer exactly as before. */
  @Test
  void vectorNeighborsStillResolvesAVertexIdentifierKeyWhenEveryNameIsOrdinary() {
    assertNeighborsNearestIsSelf("Plain", "embedding", "uuid", "u7");
  }

  /**
   * The {@code vector.neighbors} half of {@link #assertNearestIsSelf}. The function answers maps rather than
   * vertices, so the id is read out of the entry's own copy of the record's properties.
   */
  private void assertNeighborsNearestIsSelf(final String typeName, final String vectorProperty,
      final String idProperty, final String id) {
    final Map<String, Object> params = new HashMap<>();
    params.put("key", id);

    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(vector.neighbors('" + typeName + "[" + vectorProperty + "]', :key, 3))", params)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty(idProperty));
    }

    assertThat(ids).as("vector.neighbors must resolve a vertex-identifier key on type '%s'", typeName).isNotEmpty();
    assertThat(ids.getFirst()).as("a vertex is its own nearest neighbour").isEqualTo(id);
  }

  /**
   * Asks for the nearest neighbours of the vertex identified by {@code id}, passing the identifier itself rather
   * than a vector - the branch that builds the {@code SELECT}. The vertex is its own nearest neighbour, so the
   * first row coming back as that same vertex proves the lookup resolved the key to its stored vector.
   */
  private void assertNearestIsSelf(final String typeName, final String vectorProperty, final String idProperty,
      final String id) {
    final Map<String, Object> params = new HashMap<>();
    params.put("k", 3);
    params.put("key", id);

    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('" + typeName + "[" + vectorProperty + "]', $k, $key) YIELD node AS n "
            + "RETURN n.`" + idProperty + "` AS id", params)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }

    assertThat(ids).as("a vertex-identifier key on type '%s' must resolve to that vertex's vector", typeName)
        .isNotEmpty();
    assertThat(ids.getFirst()).as("a vertex is its own nearest neighbour").isEqualTo(id);
  }

  private void createVectorType(final String typeName, final String idProperty, final String vectorProperty) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE `" + typeName + "`");
      database.command("sql", "CREATE PROPERTY `" + typeName + "`.`" + idProperty + "` STRING");
      database.command("sql", "CREATE PROPERTY `" + typeName + "`.`" + vectorProperty + "` ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON `" + typeName + "` (`" + idProperty + "`) UNIQUE");
      database.command("sql", "CREATE INDEX ON `" + typeName + "` (`" + vectorProperty + "`) LSM_VECTOR METADATA {"
          + "dimensions: " + DIMENSIONS + ", similarity: 'COSINE', idPropertyName: '" + idProperty + "'}");
    });

    database.transaction(() -> {
      for (int i = 0; i < NODES; i++)
        database.newVertex(typeName).set(idProperty, "u" + i).set(vectorProperty, vector(i)).save();
    });
  }

  private static float[] vector(final int i) {
    final float[] v = new float[DIMENSIONS];
    v[i % DIMENSIONS] = 1.0f;
    v[(i + 1) % DIMENSIONS] = 0.1f + (i % 7) * 0.01f;
    v[(i + 2) % DIMENSIONS] = 0.05f + (i % 5) * 0.02f;
    return v;
  }
}
