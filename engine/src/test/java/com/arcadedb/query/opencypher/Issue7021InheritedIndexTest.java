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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7021: an index declared on a parent type is inherited by every child type - the schema creates a
 * sub-index for each child bucket - and SQL has always used it, planning
 * {@code FETCH FROM INDEX Node[id] / FILTER ITEMS BY TYPE Entity}. OpenCypher asked the type for
 * {@code getAllIndexes(false)}, which answers only with the indexes the type declares ITSELF, so a query on
 * the child type found no index and fell back to a full label scan. Since a child type cannot own a second
 * index on a property its parent already indexes, that left no way at all to get an index seek on the child.
 * <p>
 * The seek that replaces the scan reads a polymorphic index, so its cursor also carries the parent's own
 * records and every sibling child's: the label filter that keeps the answer right is what the SQL plan
 * spells as FILTER ITEMS BY TYPE. Every test below therefore checks the rows, not only the plan, and pins
 * them against the SQL engine over the same data.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7021InheritedIndexTest {
  private static final String DATABASE_PATH = "./target/databases/issue-7021-inherited-index";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory(DATABASE_PATH).create();

    // Node <- Entity, and Node <- Other: a sibling child whose records live in the same polymorphic index
    // and must never leak into an Entity answer.
    database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
    database.getSchema().getType("Node").createProperty("code", Type.STRING);
    database.getSchema().createVertexType("Entity").addSuperType("Node");
    database.getSchema().createVertexType("Other").addSuperType("Node");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Node", "id");
    // Non-unique, so the same code can legitimately be carried by records of two different child types.
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Node", "code");

    // The same shape on the relationship side: LINK <- SUBLINK, with the index declared on the parent.
    database.getSchema().createEdgeType("LINK").createProperty("tag", Type.STRING);
    database.getSchema().createEdgeType("SUBLINK").addSuperType("LINK");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "LINK", "tag");

    database.transaction(() -> {
      database.newVertex("Entity").set("id", "foo-1").save();
      database.newVertex("Entity").set("id", "foo-2").save();
      // Same property, same index, different concrete types.
      database.newVertex("Node").set("id", "bar-1").save();
      database.newVertex("Other").set("id", "baz-1").set("code", "shared").save();
      for (int i = 0; i < 64; i++)
        database.newVertex("Entity").set("id", "decoy-" + i).save();
    });

    database.transaction(() -> {
      final MutableVertex from = (MutableVertex) database.lookupByKey("Entity", "id", "foo-1").next().asVertex().modify();
      final MutableVertex to = (MutableVertex) database.lookupByKey("Entity", "id", "foo-2").next().asVertex().modify();
      from.newEdge("SUBLINK", to, "tag", "sub").save();
      from.newEdge("LINK", to, "tag", "parent").save();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void anEqualityPredicateOnAChildTypeUsesTheInheritedIndex() {
    assertThat(cypher("MATCH (e:Entity) WHERE e.id = 'foo-1' RETURN e.id AS value")).containsExactly("foo-1");
    assertSqlParity("MATCH (e:Entity) WHERE e.id = 'foo-1' RETURN e.id AS value",
        "SELECT id AS value FROM Entity WHERE id = 'foo-1'");
    assertThat(planOf("MATCH (e:Entity) WHERE e.id = 'foo-1' RETURN e.id AS value"))
        .as("the inherited Node[id] index must be seekable from the child type")
        .contains("NodeIndexSeek");
  }

  @Test
  void anInlinePropertyOnAChildTypeUsesTheInheritedIndex() {
    assertThat(cypher("MATCH (e:Entity {id: 'foo-2'}) RETURN e.id AS value")).containsExactly("foo-2");
    assertThat(planOf("MATCH (e:Entity {id: 'foo-2'}) RETURN e.id AS value")).contains("NodeIndexSeek");
  }

  @Test
  void anInListPredicateOnAChildTypeUsesTheInheritedIndex() {
    assertThat(cypher("MATCH (e:Entity) WHERE e.id IN ['foo-1', 'foo-2'] RETURN e.id AS value ORDER BY value"))
        .containsExactly("foo-1", "foo-2");
    assertSqlParity("MATCH (e:Entity) WHERE e.id IN ['foo-1', 'foo-2'] RETURN e.id AS value ORDER BY value",
        "SELECT id AS value FROM Entity WHERE id IN ['foo-1', 'foo-2'] ORDER BY id");
    assertThat(planOf("MATCH (e:Entity) WHERE e.id IN ['foo-1', 'foo-2'] RETURN e.id AS value")).contains("NodeIndexSeek");
  }

  @Test
  void aParameterizedInListOnAChildTypeUsesTheInheritedIndex() {
    assertThat(cypher("MATCH (e:Entity) WHERE e.id IN $ids RETURN e.id AS value ORDER BY value",
        Map.of("ids", List.of("foo-1", "foo-2")))).containsExactly("foo-1", "foo-2");
  }

  @Test
  void aSeekOnTheInheritedIndexNeverReturnsARecordOfAnotherType() {
    // bar-1 is a Node and baz-1 an Other: both are in the very index the Entity seek reads, and neither is
    // an Entity. This is the check the SQL plan spells as FILTER ITEMS BY TYPE.
    assertThat(cypher("MATCH (e:Entity) WHERE e.id = 'bar-1' RETURN e.id AS value")).isEmpty();
    assertThat(cypher("MATCH (e:Entity {id: 'bar-1'}) RETURN e.id AS value")).isEmpty();
    assertThat(cypher("MATCH (e:Entity) WHERE e.id IN ['foo-1', 'bar-1', 'baz-1'] RETURN e.id AS value ORDER BY value"))
        .containsExactly("foo-1");
    assertSqlParity("MATCH (e:Entity) WHERE e.id IN ['foo-1', 'bar-1', 'baz-1'] RETURN e.id AS value ORDER BY value",
        "SELECT id AS value FROM Entity WHERE id IN ['foo-1', 'bar-1', 'baz-1'] ORDER BY id");
  }

  @Test
  void aSeekOnTheParentTypeStillSeesEveryChildRecord() {
    assertThat(cypher("MATCH (n:Node) WHERE n.id IN ['foo-1', 'bar-1', 'baz-1'] RETURN n.id AS value ORDER BY value"))
        .as("the parent type is polymorphic: its own records and every child's satisfy (n:Node)")
        .containsExactly("bar-1", "baz-1", "foo-1");
    assertSqlParity("MATCH (n:Node) WHERE n.id IN ['foo-1', 'bar-1', 'baz-1'] RETURN n.id AS value ORDER BY value",
        "SELECT id AS value FROM Node WHERE id IN ['foo-1', 'bar-1', 'baz-1'] ORDER BY id");
  }

  @Test
  void aMergeOnAChildTypeUsesTheInheritedIndexWithoutMatchingAnotherType() {
    // code 'shared' is carried by an Other, not by an Entity, so MERGE must CREATE an Entity rather than
    // adopt it - which is what a polymorphic index seek without the label filter would have done.
    database.transaction(() -> query("opencypher", "MERGE (e:Entity {code: 'shared'})"));
    assertThat(cypher("MATCH (e:Entity) WHERE e.code = 'shared' RETURN e.code AS value")).containsExactly("shared");
    assertThat(cypher("MATCH (n:Node) WHERE n.code = 'shared' RETURN n.code AS value"))
        .as("the pre-existing Other and the newly merged Entity both answer (n:Node)")
        .containsExactly("shared", "shared");

    // A second MERGE finds the Entity the first one created and must not add another.
    database.transaction(() -> query("opencypher", "MERGE (e:Entity {code: 'shared'})"));
    assertThat(cypher("MATCH (e:Entity) WHERE e.code = 'shared' RETURN e.code AS value")).containsExactly("shared");
  }

  @Test
  void anEdgePredicateOnAChildEdgeTypeUsesTheInheritedIndex() {
    // A relationship pattern matches the type it names and its subtypes, never its ancestors: the LINK edge
    // tagged 'parent' lives in the very index the SUBLINK seek reads and must not answer (r:SUBLINK).
    assertThat(cypher("MATCH (a)-[r:SUBLINK]->(b) WHERE r.tag = 'sub' RETURN r.tag AS value")).containsExactly("sub");
    assertThat(cypher("MATCH (a)-[r:SUBLINK]->(b) WHERE r.tag = 'parent' RETURN r.tag AS value")).isEmpty();
    assertThat(cypher("MATCH (a)-[r:LINK]->(b) WHERE r.tag = 'sub' RETURN r.tag AS value"))
        .as("the parent edge type is polymorphic, so a SUBLINK edge answers (r:LINK)")
        .containsExactly("sub");
  }

  @Test
  void aChainedMatchOnAChildTypeUsesTheInheritedIndex() {
    // UNWIND ... MATCH routes through MatchNodeStep's WHERE-driven lookup rather than the optimizer's seek.
    assertThat(cypher("UNWIND ['foo-1', 'bar-1'] AS wanted MATCH (e:Entity) WHERE e.id = wanted RETURN e.id AS value"))
        .as("the row-driven lookup must use the inherited index and still reject the Node record")
        .containsExactly("foo-1");
  }

  private void query(final String language, final String command) {
    try (final ResultSet resultSet = database.command(language, command)) {
      while (resultSet.hasNext())
        resultSet.next();
    }
  }

  private void assertSqlParity(final String cypherQuery, final String sqlQuery) {
    assertThat(cypher(cypherQuery))
        .as("the Cypher engine must agree with SQL: %s", cypherQuery)
        .isEqualTo(values(database.query("sql", sqlQuery)));
  }

  private List<String> cypher(final String query) {
    return values(database.query("opencypher", query));
  }

  private List<String> cypher(final String query, final Map<String, Object> parameters) {
    return values(database.query("opencypher", query, parameters));
  }

  private String planOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "PROFILE " + query)) {
      while (resultSet.hasNext())
        resultSet.next();
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static List<String> values(final ResultSet resultSet) {
    final List<String> values = new ArrayList<>();
    try (resultSet) {
      while (resultSet.hasNext())
        values.add(String.valueOf(resultSet.next().<Object>getProperty("value")));
    }
    return values;
  }
}
