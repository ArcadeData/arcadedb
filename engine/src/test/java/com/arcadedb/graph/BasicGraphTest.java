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
package com.arcadedb.graph;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class BasicGraphTest extends BaseGraphTest {
  @Test
  public void checkVertices() {
    database.begin();
    try {

      assertThat(database.countType(VERTEX1_TYPE_NAME, false)).isEqualTo(1);
      assertThat(database.countType(VERTEX2_TYPE_NAME, false)).isEqualTo(2);

      final Vertex v1 = (Vertex) database.lookupByRID(root, false);
      assertThat(v1).isNotNull();

      // TEST CONNECTED VERTICES
      assertThat(v1.getTypeName()).isEqualTo(VERTEX1_TYPE_NAME);
      assertThat(v1.get("name")).isEqualTo(VERTEX1_TYPE_NAME);

      final Iterator<Vertex> vertices2level = v1.getVertices(Vertex.DIRECTION.OUT, new String[] { EDGE1_TYPE_NAME }).iterator();
      assertThat(vertices2level).isNotNull();
      assertThat(vertices2level.hasNext()).isTrue();

      final Vertex v2 = vertices2level.next();

      assertThat(v2).isNotNull();
      assertThat(v2.getTypeName()).isEqualTo(VERTEX2_TYPE_NAME);
      assertThat(v2.get("name")).isEqualTo(VERTEX2_TYPE_NAME);

      final Iterator<Vertex> vertices2level2 = v1.getVertices(Vertex.DIRECTION.OUT, new String[] { EDGE2_TYPE_NAME }).iterator();
      assertThat(vertices2level2.hasNext()).isTrue();

      final Vertex v3 = vertices2level2.next();
      assertThat(v3).isNotNull();

      assertThat(v3.getTypeName()).isEqualTo(VERTEX2_TYPE_NAME);
      assertThat(v3.get("name")).isEqualTo("V3");

      final Iterator<Vertex> vertices3level = v2.getVertices(Vertex.DIRECTION.OUT, new String[] { EDGE2_TYPE_NAME }).iterator();
      assertThat(vertices3level).isNotNull();
      assertThat(vertices3level.hasNext()).isTrue();

      final Vertex v32 = vertices3level.next();

      assertThat(v32).isNotNull();
      assertThat(v32.getTypeName()).isEqualTo(VERTEX2_TYPE_NAME);
      assertThat(v32.get("name")).isEqualTo("V3");

      assertThat(v1.isConnectedTo(v2)).isTrue();
      assertThat(v2.isConnectedTo(v1)).isTrue();
      assertThat(v1.isConnectedTo(v3)).isTrue();
      assertThat(v3.isConnectedTo(v1)).isTrue();
      assertThat(v2.isConnectedTo(v3)).isTrue();

      assertThat(v3.isConnectedTo(v1, Vertex.DIRECTION.OUT)).isFalse();
      assertThat(v3.isConnectedTo(v2, Vertex.DIRECTION.OUT)).isFalse();

    } finally {
      database.commit();
    }
  }

  @Test
  //TODO
  public void autoPersistLightWeightEdge() {
    database.begin();
    try {
      final Vertex v1 = (Vertex) database.lookupByRID(root, false);
      assertThat(v1).isNotNull();

      final Iterator<Edge> edges3 = v1.getEdges(Vertex.DIRECTION.OUT, new String[] { EDGE2_TYPE_NAME }).iterator();
      assertThat(edges3).isNotNull();
      assertThat(edges3.hasNext()).isTrue();

      try {
        final MutableEdge edge = edges3.next().modify();
        fail("Cannot modify lightweight edges");
//        edge.set("upgraded", true);
//        edge.save();
//
//        Assertions.assertThat(edge.getIdentity().getPosition() > -1).isTrue();
      } catch (final IllegalStateException e) {
      }

    } finally {
      database.commit();
    }
  }

  @Test
  public void checkEdges() {
    database.begin();
    try {

      assertThat(database.countType(EDGE1_TYPE_NAME, false)).isEqualTo(1);
      assertThat(database.countType(EDGE2_TYPE_NAME, false)).isEqualTo(1);

      final Vertex v1 = (Vertex) database.lookupByRID(root, false);
      assertThat(v1).isNotNull();

      // TEST CONNECTED EDGES
      final Iterator<Edge> edges1 = v1.getEdges(Vertex.DIRECTION.OUT, new String[] { EDGE1_TYPE_NAME }).iterator();
      assertThat(edges1).isNotNull();
      assertThat(edges1.hasNext()).isTrue();

      final Edge e1 = edges1.next();

      assertThat(e1).isNotNull();
      assertThat(e1.getTypeName()).isEqualTo(EDGE1_TYPE_NAME);
      assertThat(e1.getOut()).isEqualTo(v1);
      assertThat(e1.get("name")).isEqualTo("E1");

      final Vertex v2 = e1.getInVertex();
      assertThat(v2.get("name")).isEqualTo(VERTEX2_TYPE_NAME);

      final Iterator<Edge> edges2 = v2.getEdges(Vertex.DIRECTION.OUT, new String[] { EDGE2_TYPE_NAME }).iterator();
      assertThat(edges2.hasNext()).isTrue();

      final Edge e2 = edges2.next();
      assertThat(e2).isNotNull();

      assertThat(e2.getTypeName()).isEqualTo(EDGE2_TYPE_NAME);
      assertThat(e2.getOut()).isEqualTo(v2);
      assertThat(e2.get("name")).isEqualTo("E2");

      final Vertex v3 = e2.getInVertex();
      assertThat(v3.get("name")).isEqualTo("V3");

      final Iterator<Edge> edges3 = v1.getEdges(Vertex.DIRECTION.OUT, new String[] { EDGE2_TYPE_NAME }).iterator();
      assertThat(edges3).isNotNull();
      assertThat(edges3.hasNext()).isTrue();

      final Edge e3 = edges3.next();

      assertThat(e3).isNotNull();
      assertThat(e3.getTypeName()).isEqualTo(EDGE2_TYPE_NAME);
      assertThat(e3.getOutVertex()).isEqualTo(v1);
      assertThat(e3.getInVertex()).isEqualTo(v3);

      v2.getEdges();

    } finally {
      database.commit();
    }
  }

  @Test
  public void updateVerticesAndEdges() {
    database.begin();
    try {

      assertThat(database.countType(EDGE1_TYPE_NAME, false)).isEqualTo(1);
      assertThat(database.countType(EDGE2_TYPE_NAME, false)).isEqualTo(1);

      final Vertex v1 = (Vertex) database.lookupByRID(root, false);
      assertThat(v1).isNotNull();

      final MutableVertex v1Copy = v1.modify();
      v1Copy.set("newProperty1", "TestUpdate1");
      v1Copy.save();

      // TEST CONNECTED EDGES
      final Iterator<Edge> edges1 = v1.getEdges(Vertex.DIRECTION.OUT, new String[] { EDGE1_TYPE_NAME }).iterator();
      assertThat(edges1).isNotNull();
      assertThat(edges1.hasNext()).isTrue();

      final Edge e1 = edges1.next();

      assertThat(e1).isNotNull();

      final MutableEdge e1Copy = e1.modify();
      e1Copy.set("newProperty2", "TestUpdate2");
      e1Copy.save();

      database.commit();

      final Vertex v1CopyReloaded = (Vertex) database.lookupByRID(v1Copy.getIdentity(), true);
      assertThat(v1CopyReloaded.get("newProperty1")).isEqualTo("TestUpdate1");
      final Edge e1CopyReloaded = (Edge) database.lookupByRID(e1Copy.getIdentity(), true);
      assertThat(e1CopyReloaded.get("newProperty2")).isEqualTo("TestUpdate2");

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  public void deleteVertices() {
    database.begin();
    try {

      final Vertex v1 = (Vertex) database.lookupByRID(root, false);
      assertThat(v1).isNotNull();

      Iterator<Vertex> vertices = v1.getVertices(Vertex.DIRECTION.OUT).iterator();
      assertThat(vertices.hasNext()).isTrue();
      Vertex v3 = vertices.next();
      assertThat(v3).isNotNull();

      assertThat(vertices.hasNext()).isTrue();
      Vertex v2 = vertices.next();
      assertThat(v2).isNotNull();

      final long totalVertices = database.countType(v1.getTypeName(), true);

      // DELETE THE VERTEX
      // -----------------------
      database.deleteRecord(v1);

      assertThat(database.countType(v1.getTypeName(), true)).isEqualTo(totalVertices - 1);

      vertices = v2.getVertices(Vertex.DIRECTION.IN).iterator();
      assertThat(vertices.hasNext()).isFalse();

      vertices = v2.getVertices(Vertex.DIRECTION.OUT).iterator();
      assertThat(vertices.hasNext()).isTrue();

      // Expecting 1 edge only: V2 is still connected to V3
      vertices = v3.getVertices(Vertex.DIRECTION.IN).iterator();
      assertThat(vertices.hasNext()).isTrue();
      vertices.next();
      assertThat(vertices.hasNext()).isFalse();

      // RELOAD AND CHECK AGAIN
      // -----------------------
      v2 = (Vertex) database.lookupByRID(v2.getIdentity(), true);

      vertices = v2.getVertices(Vertex.DIRECTION.IN).iterator();
      assertThat(vertices.hasNext()).isFalse();

      vertices = v2.getVertices(Vertex.DIRECTION.OUT).iterator();
      assertThat(vertices.hasNext()).isTrue();

      v3 = (Vertex) database.lookupByRID(v3.getIdentity(), true);

      // Expecting 1 edge only: V2 is still connected to V3
      vertices = v3.getVertices(Vertex.DIRECTION.IN).iterator();
      assertThat(vertices.hasNext()).isTrue();
      vertices.next();
      assertThat(vertices.hasNext()).isFalse();

      try {
        database.lookupByRID(root, true);
        fail("Expected deleted record");
      } catch (final RecordNotFoundException e) {
      }

    } finally {
      database.commit();
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  public void deleteEdges() {
    database.begin();
    try {

      final Vertex v1 = (Vertex) database.lookupByRID(root, false);
      assertThat(v1).isNotNull();

      Iterator<Edge> edges = v1.getEdges(Vertex.DIRECTION.OUT).iterator();
      assertThat(edges.hasNext()).isTrue();
      final Edge e3 = edges.next();
      assertThat(e3).isNotNull();

      assertThat(edges.hasNext()).isTrue();
      final Edge e2 = edges.next();
      assertThat(e2).isNotNull();

      // DELETE THE EDGE
      // -----------------------
      database.deleteRecord(e2);

      Vertex vOut = e2.getOutVertex();
      edges = vOut.getEdges(Vertex.DIRECTION.OUT).iterator();
      assertThat(edges.hasNext()).isTrue();

      edges.next();
      assertThat(edges.hasNext()).isFalse();

      Vertex vIn = e2.getInVertex();
      edges = vIn.getEdges(Vertex.DIRECTION.IN).iterator();
      assertThat(edges.hasNext()).isFalse();

      // RELOAD AND CHECK AGAIN
      // -----------------------
      try {
        database.lookupByRID(e2.getIdentity(), true);
        fail("Expected deleted record");
      } catch (final RecordNotFoundException e) {
      }

      vOut = e2.getOutVertex();
      edges = vOut.getEdges(Vertex.DIRECTION.OUT).iterator();
      assertThat(edges.hasNext()).isTrue();

      edges.next();
      assertThat(edges.hasNext()).isFalse();

      vIn = e2.getInVertex();
      edges = vIn.getEdges(Vertex.DIRECTION.IN).iterator();
      assertThat(edges.hasNext()).isFalse();

    } finally {
      database.commit();
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  public void deleteEdgesFromEdgeIterator() {
    database.begin();
    try {

      final Vertex v1 = (Vertex) database.lookupByRID(root, false);
      assertThat(v1).isNotNull();

      Iterator<Edge> edges = v1.getEdges(Vertex.DIRECTION.OUT).iterator();

      assertThat(edges.hasNext()).isTrue();
      final Edge e3 = edges.next();
      assertThat(e3).isNotNull();

      assertThat(edges.hasNext()).isTrue();
      final Edge e2 = edges.next();
      assertThat(e2).isNotNull();

      // DELETE THE EDGE
      // -----------------------
      edges.remove();

      assertThat(edges.hasNext()).isFalse();

      try {
        e2.getOutVertex();
        fail("");
      } catch (RecordNotFoundException e) {
        // EXPECTED
      }

      try {
        e2.getInVertex();
        fail("");
      } catch (RecordNotFoundException e) {
        // EXPECTED
      }

      // RELOAD AND CHECK AGAIN
      // -----------------------
      try {
        database.lookupByRID(e2.getIdentity(), true);
        fail("Expected deleted record");
      } catch (final RecordNotFoundException e) {
        // EXPECTED
      }

    } finally {
      database.commit();
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  public void selfLoopEdges() {
    database.getSchema().buildEdgeType().withName(EDGE3_TYPE_NAME).withBidirectional(false).create();

    database.begin();
    try {

      // UNIDIRECTIONAL EDGE
      final Vertex v1 = database.newVertex(VERTEX1_TYPE_NAME).save();

      database.command("sql", "create edge " + EDGE3_TYPE_NAME + " from ? to ? unidirectional", v1, v1);

      assertThat(v1.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext()).isTrue();
      assertThat(v1.getVertices(Vertex.DIRECTION.OUT).iterator().next()).isEqualTo(v1);
      assertThat(v1.getVertices(Vertex.DIRECTION.IN).iterator().hasNext()).isFalse();

      // BIDIRECTIONAL EDGE
      final Vertex v2 = database.newVertex(VERTEX1_TYPE_NAME).save();
      v2.newEdge(EDGE1_TYPE_NAME, v2).save();

      assertThat(v2.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext()).isTrue();
      assertThat(v2.getVertices(Vertex.DIRECTION.OUT).iterator().next()).isEqualTo(v2);

      assertThat(v2.getVertices(Vertex.DIRECTION.IN).iterator().hasNext()).isTrue();
      assertThat(v2.getVertices(Vertex.DIRECTION.IN).iterator().next()).isEqualTo(v2);

      database.commit();

      // UNIDIRECTIONAL EDGE
      final Vertex v1reloaded = (Vertex) database.lookupByRID(v1.getIdentity(), true);
      assertThat(v1reloaded.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext()).isTrue();
      assertThat(v1reloaded.getVertices(Vertex.DIRECTION.OUT).iterator().next()).isEqualTo(v1reloaded);
      assertThat(v1reloaded.getVertices(Vertex.DIRECTION.IN).iterator().hasNext()).isFalse();

      // BIDIRECTIONAL EDGE
      final Vertex v2reloaded = (Vertex) database.lookupByRID(v2.getIdentity(), true);

      assertThat(v2reloaded.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext()).isTrue();
      assertThat(v2reloaded.getVertices(Vertex.DIRECTION.OUT).iterator().next()).isEqualTo(v2reloaded);

      assertThat(v2reloaded.getVertices(Vertex.DIRECTION.IN).iterator().hasNext()).isTrue();
      assertThat(v2reloaded.getVertices(Vertex.DIRECTION.IN).iterator().next()).isEqualTo(v2reloaded);

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  public void shortestPath() {
    database.begin();
    try {

      final Iterator<Record> v1Iterator = database.iterateType(VERTEX1_TYPE_NAME, true);
      while (v1Iterator.hasNext()) {

        final Record v1 = v1Iterator.next();

        final Iterator<Record> v2Iterator = database.iterateType(VERTEX2_TYPE_NAME, true);
        while (v2Iterator.hasNext()) {

          final Record v2 = v2Iterator.next();

          final ResultSet result = database.query("sql", "select shortestPath(?,?) as sp", v1, v2);
          assertThat(result.hasNext()).isTrue();
          final Result line = result.next();

          assertThat(line).isNotNull();
          assertThat(line.getPropertyNames().contains("sp")).isTrue();
          List<RID> sp = line.<List<RID>>getProperty("sp");
          assertThat(sp).isNotNull();
          assertThat(sp).hasSize(2);

          assertThat(sp.get(0)).isEqualTo(v1);
          assertThat(sp.get(1)).isEqualTo(v2);
        }
      }

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  public void customFunction() {
    database.begin();
    try {
      ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionAbstract("ciao") {
        @Override
        public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
            final Object[] params,
            final CommandContext context) {
          return "Ciao";
        }

        @Override
        public String getSyntax() {
          return "just return 'ciao'";
        }
      });

      final ResultSet result = database.query("sql", "select ciao() as ciao");
      assertThat(result.hasNext()).isTrue();
      final Result line = result.next();

      assertThat(line).isNotNull();
      assertThat(line.getPropertyNames().contains("ciao")).isTrue();
      assertThat(line.<String>getProperty("ciao")).isEqualTo("Ciao");

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  public static String testReflectionMethod() {
    return "reflect on this";
  }

  @Test
  public void customReflectionFunction() {
    database.begin();
    try {
      ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().getReflectionFactory().register("test_", getClass());

      final ResultSet result = database.query("sql", "select test_testReflectionMethod() as testReflectionMethod");
      assertThat(result.hasNext()).isTrue();
      final Result line = result.next();

      assertThat(line).isNotNull();
      assertThat(line.getPropertyNames().contains("testReflectionMethod")).isTrue();
      assertThat(line.<String>getProperty("testReflectionMethod")).isEqualTo("reflect on this");

    } finally {
      new DatabaseChecker(database).setVerboseLevel(0).check();
    }
  }

  @Test
  public void rollbackEdge() {
    final AtomicReference<RID> v1RID = new AtomicReference<>();

    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX1_TYPE_NAME).save();
      v1RID.set(v1.getIdentity());
    });

    try {
      database.transaction(() -> {
        final Vertex v1a = v1RID.get().asVertex();

        final MutableVertex v2 = database.newVertex(VERTEX1_TYPE_NAME).save();

        v1a.newEdge(EDGE2_TYPE_NAME, v2);
        v1a.newEdge(EDGE2_TYPE_NAME, v2);
        //throw new ArcadeDBException();
      });

      //Assertions.fail();

    } catch (final RuntimeException e) {
      // EXPECTED
    }

    database.transaction(() -> {
      final Vertex v1a = v1RID.get().asVertex();

      final MutableVertex v2 = database.newVertex(VERTEX1_TYPE_NAME);
      v2.set("rid", v1RID.get());
      v2.save();

      assertThat(v1a.isConnectedTo(v2)).isFalse();
    });
  }

  @Test
  public void reuseRollBackedTx() {
    final AtomicReference<RID> v1RID = new AtomicReference<>();

    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX1_TYPE_NAME).save();
      v1.save();
      v1RID.set(v1.getIdentity());
    });

    database.begin();
    final Vertex v1a = v1RID.get().asVertex();
    MutableVertex v2 = database.newVertex(VERTEX1_TYPE_NAME).save();
    v1a.newEdge(EDGE2_TYPE_NAME, v2);
    v1a.newEdge(EDGE2_TYPE_NAME, v2);
    database.rollback();

    try {
      v2 = database.newVertex(VERTEX1_TYPE_NAME);
      v2.set("rid", v1RID.get());
      v2.save();

      fail("");

    } catch (final RuntimeException e) {
      // EXPECTED
    }

    assertThat(v1a.isConnectedTo(v2)).isFalse();
  }

  @Test
  public void edgeUnivocity() {
    final MutableVertex[] v1 = new MutableVertex[1];
    final MutableVertex[] v2 = new MutableVertex[1];
    database.transaction(() -> {
      final EdgeType e = database.getSchema().createEdgeType("OnlyOneBetweenVertices");
      e.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "@out", "@in");

      v1[0] = database.newVertex(VERTEX1_TYPE_NAME).set("id", 1001).save();
      v2[0] = database.newVertex(VERTEX1_TYPE_NAME).set("id", 1002).save();
      v1[0].newEdge("OnlyOneBetweenVertices", v2[0]);
    });

    try {
      database.transaction(() -> v1[0].newEdge("OnlyOneBetweenVertices", v2[0]));
      fail("");
    } catch (final DuplicatedKeyException ex) {
      // EXPECTED
    }

    database.transaction(() -> v2[0].newEdge("OnlyOneBetweenVertices", v1[0]));

    database.transaction(() -> {
      final Iterable<Edge> edges = v1[0].getEdges(Vertex.DIRECTION.OUT, "OnlyOneBetweenVertices");
      for (final Edge e : edges)
        e.delete();
    });

    database.transaction(() -> v1[0].newEdge("OnlyOneBetweenVertices", v2[0]));

    database.transaction(() -> {
      final Iterable<Edge> edges = v2[0].getEdges(Vertex.DIRECTION.OUT, "OnlyOneBetweenVertices");
      for (final Edge e : edges)
        e.delete();
    });

    database.transaction(() -> v2[0].newEdge("OnlyOneBetweenVertices", v1[0]));
  }

  @Test
  public void edgeUnivocitySQL() {
    final MutableVertex[] v1 = new MutableVertex[1];
    final MutableVertex[] v2 = new MutableVertex[1];
    database.transaction(() -> {
      database.command("sql", "create edge type OnlyOneBetweenVertices");

      database.command("sql", "create index ON OnlyOneBetweenVertices (`@out`, `@in`) UNIQUE");

      v1[0] = database.newVertex(VERTEX1_TYPE_NAME).set("id", 1001).save();
      v2[0] = database.newVertex(VERTEX1_TYPE_NAME).set("id", 1002).save();

      final ResultSet result = database.command("sql", "create edge OnlyOneBetweenVertices from ? to ?", v1[0], v2[0]);
      assertThat(result.hasNext()).isTrue();
    });

    try {
      database.transaction(() -> v1[0].newEdge("OnlyOneBetweenVertices", v2[0]));
      fail("");
    } catch (final DuplicatedKeyException ex) {
      // EXPECTED
    }

    try {
      database.transaction(() -> database.command("sql", "create edge OnlyOneBetweenVertices from ? to ?", v1[0], v2[0]));
      fail("");
    } catch (final DuplicatedKeyException ex) {
      // EXPECTED
    }

    database.transaction(
        () -> database.command("sql", "create edge OnlyOneBetweenVertices from ? to ? IF NOT EXISTS", v1[0], v2[0]));
  }

  @Test
  public void edgeConstraints() {
    final MutableVertex[] v1 = new MutableVertex[1];
    final MutableVertex[] v2 = new MutableVertex[1];
    database.transaction(() -> {
      database.command("sql", "create edge type EdgeConstraint");
      database.command("sql", "create property EdgeConstraint.`@out` LINK of " + VERTEX1_TYPE_NAME);
      database.command("sql", "create property EdgeConstraint.`@in` LINK of " + VERTEX2_TYPE_NAME);

      v1[0] = database.newVertex(VERTEX1_TYPE_NAME).set("id", 1001).save();
      v2[0] = database.newVertex(VERTEX2_TYPE_NAME).set("id", 1002).save();
      final ResultSet result = database.command("sql", "create edge EdgeConstraint from ? to ?", v1[0], v2[0]);
      assertThat(result.hasNext()).isTrue();
    });

    try {
      database.transaction(() -> v2[0].newEdge("EdgeConstraint", v1[0]));
      fail("");
    } catch (final ValidationException ex) {
      // EXPECTED
    }

    try {
      database.transaction(() -> database.command("sql", "create edge EdgeConstraint from ? to ?", v2[0], v1[0]));
      fail("");
    } catch (final ValidationException ex) {
      // EXPECTED
    }
  }

  // https://github.com/ArcadeData/arcadedb/issues/577
  @Test
  public void testEdgeTypeNotFromVertex() {
    final var vType = database.getSchema().createVertexType("a-vertex");
    final var eType = database.getSchema().createEdgeType("a-edge");

    database.begin();
    final var v1 = database.newVertex("a-vertex").save();
    final var v2 = database.newVertex("a-vertex").save();

    try {
      final Edge e1 = v1.newEdge("a-vertex", v2); // <-- expect IllegalArgumentException
      fail("Created an edge of vertex type");
    } catch (final ClassCastException e) {
      // EXPECTED
    }
  }

  // https://github.com/ArcadeData/arcadedb/issues/689
  @Test
  public void testEdgeDescendantOrder() {
    final var vType = database.getSchema().createVertexType("testEdgeDescendantOrderVertex");
    final var eType = database.getSchema().createEdgeType("testEdgeDescendantOrderEdge");

    database.transaction(() -> {
      final var v1 = database.newVertex("testEdgeDescendantOrderVertex").set("id", -1).save();
      for (int i = 0; i < 10000; i++) {
        final var v2 = database.newVertex("testEdgeDescendantOrderVertex").set("id", i).save();
        v1.newEdge("testEdgeDescendantOrderEdge", v2);
      }

      final Iterator<Vertex> vertices = v1.getVertices(Vertex.DIRECTION.OUT).iterator();
      for (int i = 10000 - 1; vertices.hasNext(); --i) {
        assertThat(vertices.next().get("id")).isEqualTo(i);
      }
    });
  }
}
