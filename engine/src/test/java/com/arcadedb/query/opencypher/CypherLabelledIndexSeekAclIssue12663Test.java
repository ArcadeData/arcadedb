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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A user who may not read a vertex type is refused by a walk of its buckets whatever the value is. The index seeks of the traditional
 * plan (issue #12663) reach the type through an index instead: the seek of a node with a single label loads a record only when the
 * index holds the key, and the edge index seek binds the endpoint it finds from its bucket without loading it. Left to themselves
 * the first would refuse only for a key the denied type holds, and tell a user who may not read the type whether it holds a key,
 * and the second would not refuse at all and return the rows of the relationships that end in the type.
 * <p>
 * Every statement is run with a key the denied type holds and with one it does not, and must be refused the same way for both.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherLabelledIndexSeekAclIssue12663Test {
  private static final String PATH = "target/databases/CypherLabelledIndexSeekAclIssue12663Test";

  /** A statement written with {@code $v}, and a value of it that the denied type holds, and one that nothing holds. */
  private record Shape(String statement, int present, int absent) {
  }

  private static final List<Shape> SHAPES = List.of(
      // the seek of a node with a single label: an inline map, a WHERE equality, an IN list, and the prefix of a composite key
      new Shape("MATCH (p:Secret {id: $v}) RETURN id(p) AS r", 3, 100),
      new Shape("MATCH (p:Secret) WHERE p.id = $v RETURN id(p) AS r", 3, 100),
      new Shape("MATCH (p:Secret)-[:K]->(q) WHERE p.id = $v RETURN id(q) AS r", 3, 100),
      new Shape("WITH 1 AS one MATCH (p:Secret) WHERE p.id IN [$v] RETURN id(p) AS r", 3, 100),
      new Shape("MATCH (p:SecretPair {a: $v}) RETURN id(p) AS r", 3, 100),
      new Shape("MATCH (p:SecretPair {a: $v, b: $v}) RETURN id(p) AS r", 3, 100),
      // the seek re-opened once per outer row of a chained MATCH
      new Shape("UNWIND [$v] AS x MATCH (p:Secret {id: x}) RETURN id(p) AS r", 3, 100),
      new Shape("UNWIND [$v] AS x MATCH (p:Secret) WHERE p.id = x RETURN id(p) AS r", 3, 100),
      new Shape("UNWIND [$v, $v + 1] AS x MATCH (p:Secret) WHERE p.id IN [x] RETURN id(p) AS r", 3, 100),
      // the edge index seek: a labelled target and a labelled source
      new Shape("MATCH (q)-[e:K]->(p:Secret) WHERE e.w = $v RETURN id(q) AS r", 3, 100),
      new Shape("MATCH (p:Secret)-[e:K]->(q) WHERE e.w = $v RETURN id(q) AS r", 13, 100),
      new Shape("MATCH (q)<-[e:K]-(p:Secret) WHERE e.w = $v RETURN id(q) AS r", 13, 100),
      new Shape("MATCH (q:Open)-[e:K {w: $v}]->(p:Secret) RETURN id(q) AS r", 3, 100),
      // the range, a control: that step walks the label, so it is refused for both
      new Shape("MATCH (p:Secret) WHERE p.id < $v RETURN id(p) AS r", 100, -100));

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      for (final String name : new String[] { "Open", "Secret" }) {
        final VertexType type = database.getSchema().createVertexType(name);
        type.createProperty("id", Integer.class);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      }
      final VertexType pair = database.getSchema().createVertexType("SecretPair");
      pair.createProperty("a", Integer.class);
      pair.createProperty("b", Integer.class);
      pair.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "a", "b");

      final VertexType base = database.getSchema().createVertexType("Base");
      base.createProperty("id", Integer.class);
      base.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      database.getSchema().createVertexType("Sub").addSuperType("Base");

      final EdgeType edge = database.getSchema().createEdgeType("K");
      edge.createProperty("w", Integer.class);
      edge.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "w");
    });
    database.transaction(() -> {
      final MutableVertex open = database.newVertex("Open").set("id", 1).save();
      for (int i = 1; i <= 5; i++) {
        final MutableVertex secret = database.newVertex("Secret").set("id", i).save();
        open.newEdge("K", secret, "w", i);
        secret.newEdge("K", open, "w", i + 10);
        database.newVertex("SecretPair").set("a", i).set("b", i).save();
      }
      // the keys 1 to 5 of the index of Base are held by Base itself, 6 to 10 by its subtype
      for (int i = 1; i <= 5; i++)
        database.newVertex("Base").set("id", i).save();
      for (int i = 6; i <= 10; i++)
        database.newVertex("Sub").set("id", i).save();
    });
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aTypeTheUserMayNotReadRefusesTheStatementWhateverTheKeyIs() {
    deny("Secret", "SecretPair");

    final List<String> notRefused = new ArrayList<>();
    for (final Shape shape : SHAPES)
      for (final int value : new int[] { shape.present(), shape.absent() })
        if (!refused(shape.statement(), value))
          notRefused.add(shape.statement() + " with $v = " + value);
    assertThat(notRefused).as("a denied type refuses the statement, it does not tell whether it holds a key").isEmpty();
  }

  @Test
  void aSubTypeTheUserMayNotReadRefusesTheSeekOfItsSuperTypeWhateverTheKeyIs() {
    deny("Sub");

    // the index of Base holds the vertices of Sub, and the walk of Base opens the buckets of Sub: a key only Base holds is refused too
    for (final String statement : new String[] { "MATCH (p:Base {id: $v}) RETURN id(p) AS r",
        "MATCH (p:Base) WHERE p.id = $v RETURN id(p) AS r", "WITH 1 AS one MATCH (p:Base) WHERE p.id IN [$v] RETURN id(p) AS r" })
      for (final int value : new int[] { 3, 8, 100 })
        assertThat(refused(statement, value)).as(statement + " with $v = " + value).isTrue();
  }

  @Test
  void anEdgeTypeTheUserMayNotReadRefusesTheEdgeSeekWhateverTheKeyIs() {
    deny("K");

    for (final String statement : new String[] { "MATCH (q)-[e:K]->(p) WHERE e.w = $v RETURN id(q) AS r",
        "MATCH (q:Open)-[e:K]->(p:Secret) WHERE e.w = $v RETURN id(q) AS r" })
      for (final int value : new int[] { 3, 100 })
        assertThat(refused(statement, value)).as(statement + " with $v = " + value).isTrue();
  }

  @Test
  void aTypeTheUserMayReadIsAnsweredWhileAnotherIsDenied() {
    deny("Secret", "SecretPair");

    assertThat(drain("MATCH (p:Open {id: $v}) RETURN id(p) AS r", 1)).isEqualTo(1);
    assertThat(drain("MATCH (p:Open {id: $v}) RETURN id(p) AS r", 100)).isZero();
    assertThat(drain("MATCH (p:Open) WHERE p.id = $v RETURN id(p) AS r", 1)).isEqualTo(1);
    assertThat(drain("MATCH (p:Base {id: $v}) RETURN id(p) AS r", 3)).isEqualTo(1);
    assertThat(drain("MATCH (p:Base {id: $v}) RETURN id(p) AS r", 100)).isZero();
    assertThat(drain("MATCH (q:Open)-[e:K]->(p:Open) WHERE e.w = $v RETURN id(q) AS r", 100)).isZero();
  }

  @Test
  void aUserWhoMayReadEveryTypeIsAnswered() {
    deny();

    assertThat(drain("MATCH (p:Secret {id: $v}) RETURN id(p) AS r", 3)).isEqualTo(1);
    assertThat(drain("MATCH (p:Secret {id: $v}) RETURN id(p) AS r", 100)).isZero();
    assertThat(drain("MATCH (p:Secret) WHERE p.id IN [$v] RETURN id(p) AS r", 3)).isEqualTo(1);
    assertThat(drain("MATCH (p:SecretPair {a: $v}) RETURN id(p) AS r", 3)).isEqualTo(1);
    assertThat(drain("UNWIND [$v, 4] AS x MATCH (p:Secret {id: x}) RETURN id(p) AS r", 3)).isEqualTo(2);
    assertThat(drain("MATCH (q)-[e:K]->(p:Secret) WHERE e.w = $v RETURN id(q) AS r", 3)).isEqualTo(1);
    assertThat(drain("MATCH (q)-[e:K]->(p:Secret) WHERE e.w = $v RETURN id(q) AS r", 100)).isZero();
    assertThat(drain("MATCH (p:Secret)-[e:K]->(q) WHERE e.w = $v RETURN id(q) AS r", 13)).isEqualTo(1);
    assertThat(drain("MATCH (p:Base {id: $v}) RETURN id(p) AS r", 8)).isEqualTo(1);
  }

  @Test
  void aStatementWithNoBoundUserIsAnswered() {
    // an embedded call has no user bound: nothing is denied
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);

    assertThat(drain("MATCH (p:Secret {id: $v}) RETURN id(p) AS r", 3)).isEqualTo(1);
    assertThat(drain("MATCH (q)-[e:K]->(p:Secret) WHERE e.w = $v RETURN id(q) AS r", 3)).isEqualTo(1);
  }

  private boolean refused(final String statement, final int value) {
    return catchThrowable(() -> drain(statement, value)) instanceof SecurityException;
  }

  /** The number of rows of the statement. */
  private long drain(final String cypher, final int value) {
    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", cypher, Map.of("v", value))) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  /** Denies the read of every record of the types, and grants everything else. */
  private void deny(final String... typeNames) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : typeNames)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
