/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.remote;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.Vertex.DIRECTION;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.server.BaseGraphServerTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;

import static com.arcadedb.graph.Vertex.DIRECTION.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class RemoteDatabaseIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  public void simpleTxDocuments() throws Exception {
    testEachServer((serverIndex) -> {
      assertThat(new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
          DATABASE_NAME)).isTrue();

      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("sql", "create vertex type Person");

      // BEGIN
      database.transaction(() -> {
        // CREATE DOCUMENT VIA API
        final MutableDocument jay = database.newDocument("Person").set("name", "Jay").save();
        assertThat(jay).isNotNull();
        assertThat(jay.getString("name")).isEqualTo("Jay");
        assertThat(jay.getIdentity()).isNotNull();
        jay.save();

        // TEST DELETION AND LOOKUP
        jay.delete();
        try {
          jay.reload();
          fail();
        } catch (RecordNotFoundException e) {
          // EXPECTED
        }

        // CREATE DOCUMENT VIA SQL
        ResultSet result = database.command("SQL", "insert into Person set name = 'John'");
        assertThat((Iterator<? extends Result>) result).isNotNull();
        assertThat(result.hasNext()).isTrue();
        final Result rec = result.next();
        assertThat(rec.toJSON().toString().contains("John")).isTrue();
        assertThat(rec.toElement().toMap().get("name")).isEqualTo("John");
        final RID rid = rec.toElement().getIdentity();

        // RETRIEVE DOCUMENT WITH QUERY
        result = database.query("SQL", "select from Person where name = 'John'");
        assertThat(result.hasNext()).isTrue();

        // UPDATE DOCUMENT WITH COMMAND
        result = database.command("SQL", "update Person set lastName = 'Red' where name = 'John'");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().toJSON().getInt("count")).isEqualTo(1);

        final Document record = (Document) database.lookupByRID(rid);
        assertThat((Iterator<? extends Result>) result).isNotNull();
        assertThat(record.getString("lastName")).isEqualTo("Red");

        assertThat(database.countType("Person", true)).isEqualTo(1L);
        assertThat(database.countType("Person", false)).isEqualTo(1L);

        long totalInBuckets = 0L;
        for (int i = 0; i < 100; i++) {
          try {
            totalInBuckets += database.countBucket("Person_" + i);
          } catch (Exception e) {
            // IGNORE IT
            break;
          }
        }
        assertThat(totalInBuckets).isEqualTo(1L);
      });

      // RETRIEVE DOCUMENT WITH QUERY AFTER COMMIT
      final ResultSet result = database.query("SQL", "select from Person where name = 'John'");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("lastName")).isEqualTo("Red");
    });
  }

  @Test
  public void simpleTxGraph() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("sql", "create vertex type " + VERTEX1_TYPE_NAME);
      database.command("sql", "create property " + VERTEX1_TYPE_NAME + ".id long");
      database.command("sql", "create index on " + VERTEX1_TYPE_NAME + "(id) unique");

      database.command("sql", "create vertex type " + VERTEX2_TYPE_NAME);
      database.command("sql", "create edge type " + EDGE1_TYPE_NAME);
      database.command("sql", "create edge type " + EDGE2_TYPE_NAME);

      // BEGIN
      database.transaction(() -> {
        // CREATE VERTEX TYPE
        ResultSet result = database.command("SQL", "create vertex type Character");
        assertThat((Iterator<? extends Result>) result).isNotNull();
        assertThat(result.hasNext()).isTrue();

        // CREATE DOCUMENT VIA API
        final MutableVertex jay = database.newVertex("Character").set("name", "Jay").save();
        assertThat(jay instanceof RemoteMutableVertex).isTrue();

        assertThat(jay).isNotNull();
        assertThat(jay.getString("name")).isEqualTo("Jay");
        assertThat(jay.getIdentity()).isNotNull();
        jay.save();

        assertThat(jay).isNotNull();
        assertThat(jay.getString("name")).isEqualTo("Jay");
        assertThat(jay.getIdentity()).isNotNull();
        jay.save();

        // CREATE DOCUMENT VIA API
        final Map<String, Object> map = Map.of("on", "today", "for", "5 days");
        Edge edge = jay.newEdge(EDGE1_TYPE_NAME, jay, map).save();
        assertThat(edge instanceof RemoteMutableEdge).isTrue();
        assertThat(edge.get("on")).isEqualTo("today");
        assertThat(edge.get("for")).isEqualTo("5 days");

        // TEST DELETION AND LOOKUP
        jay.delete();
        try {
          jay.reload();
          fail();
        } catch (RecordNotFoundException e) {
          // EXPECTED
        }

        // CREATE VERTEX 1
        result = database.command("SQL", "insert into Character set name = 'John'");
        assertThat((Iterator<? extends Result>) result).isNotNull();
        assertThat(result.hasNext()).isTrue();
        Result rec = result.next();
        assertThat(rec.toJSON().toString().contains("John")).isTrue();
        assertThat(rec.toElement().toMap().get("name")).isEqualTo("John");
        final RID rid1 = rec.getIdentity().get();

        // CREATE VERTEX 2
        result = database.command("SQL", "create vertex Character set name = 'Kimbal'");
        assertThat((Iterator<? extends Result>) result).isNotNull();
        assertThat(result.hasNext()).isTrue();
        rec = result.next();
        assertThat(rec.toJSON().toString().contains("Kimbal")).isTrue();
        assertThat(rec.toElement().toMap().get("name")).isEqualTo("Kimbal");
        final RID rid2 = rec.getIdentity().get();

        // RETRIEVE VERTEX WITH QUERY
        result = database.query("SQL", "select from Character where name = 'John'");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().isVertex()).isTrue();

        // UPDATE VERTEX WITH COMMAND
        result = database.command("SQL", "update Character set lastName = 'Red' where name = 'John' or name = 'Kimbal'");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().toJSON().getInt("count")).isEqualTo(2);

        // CREATE EDGE WITH COMMAND
        result = database.command("SQL", "create edge " + EDGE1_TYPE_NAME + " from " + rid1 + " to " + rid2);
        assertThat(result.hasNext()).isTrue();
        edge = result.next().getEdge().get();

        edge.toMap();
        edge.toJSON();

        assertThat(edge.getTypeName()).isEqualTo(EDGE1_TYPE_NAME);
        assertThat(edge.getOut()).isEqualTo(rid1);
        assertThat(edge.getIn()).isEqualTo(rid2);

        Vertex record = (Vertex) database.lookupByRID(rid1);
        assertThat(record).isNotNull();
        assertThat(record.getString("name")).isEqualTo("John");
        assertThat(record.getString("lastName")).isEqualTo("Red");

        record.toMap();
        record.toJSON();

        record = (Vertex) database.lookupByRID(rid2);
        assertThat(record).isNotNull();
        assertThat(record.getString("name")).isEqualTo("Kimbal");
        assertThat(record.getString("lastName")).isEqualTo("Red");

        final MutableDocument mutable = record.modify();
        mutable.set("extra", 100);
        mutable.save();
      });

      // RETRIEVE VERTEX WITH QUERY AFTER COMMIT
      final ResultSet result = database.query("SQL", "select from Character where name = 'Kimbal'");
      assertThat(result.hasNext()).isTrue();
      final Result record = result.next();
      assertThat(record.isVertex()).isTrue();

      final Vertex kimbal = record.getVertex().get();
      assertThat(kimbal.getString("lastName")).isEqualTo("Red");
      assertThat(kimbal.getInteger("extra")).isEqualTo(100);

      assertThat(kimbal.toMap().containsKey("@cat")).isTrue();
      assertThat(kimbal.toMap().containsKey("@type")).isTrue();
      assertThat(kimbal.toMap().containsKey("@out")).isFalse();
      assertThat(kimbal.toMap().containsKey("@in")).isFalse();

      kimbal.toJSON();

      final Iterator<Vertex> connected = kimbal.getVertices(IN).iterator();
      assertThat(connected.hasNext()).isTrue();
      final Vertex albert = connected.next();
      assertThat(albert.getString("lastName")).isEqualTo("Red");

      assertThat(kimbal.countEdges(IN, null)).isEqualTo(1L);
      assertThat(kimbal.countEdges(IN, EDGE1_TYPE_NAME)).isEqualTo(1L);
      assertThat(kimbal.countEdges(IN, EDGE2_TYPE_NAME)).isEqualTo(0L);
      assertThat(kimbal.countEdges(OUT, null)).isEqualTo(0);
      assertThat(kimbal.countEdges(OUT, EDGE1_TYPE_NAME)).isEqualTo(0);
      assertThat(kimbal.countEdges(OUT, EDGE2_TYPE_NAME)).isEqualTo(0L);

      assertThat(albert.countEdges(OUT, null)).isEqualTo(1L);
      assertThat(albert.countEdges(OUT, EDGE1_TYPE_NAME)).isEqualTo(1L);
      assertThat(albert.countEdges(OUT, EDGE2_TYPE_NAME)).isEqualTo(0L);
      assertThat(albert.countEdges(IN, null)).isEqualTo(0);
      assertThat(albert.countEdges(IN, EDGE1_TYPE_NAME)).isEqualTo(0);
      assertThat(albert.countEdges(IN, EDGE2_TYPE_NAME)).isEqualTo(0);

      assertThat(kimbal.isConnectedTo(albert.getIdentity())).isTrue();
      assertThat(kimbal.isConnectedTo(albert.getIdentity(), IN)).isTrue();
      assertThat(kimbal.isConnectedTo(albert.getIdentity(), OUT)).isFalse();

      assertThat(albert.isConnectedTo(kimbal.getIdentity())).isTrue();
      assertThat(albert.isConnectedTo(kimbal.getIdentity(), OUT)).isTrue();
      assertThat(albert.isConnectedTo(kimbal.getIdentity(), IN)).isFalse();

      final MutableEdge newEdge = albert.newEdge(EDGE2_TYPE_NAME, kimbal, "since", "today");
      assertThat(albert.getIdentity()).isEqualTo(newEdge.getOut());
      assertThat(albert).isEqualTo(newEdge.getOutVertex());
      assertThat(kimbal.getIdentity()).isEqualTo(newEdge.getIn());
      assertThat(kimbal).isEqualTo(newEdge.getInVertex());

      newEdge.set("updated", true);
      newEdge.save();

      // SAME BUT FROM A MUTABLE INSTANCE
      final MutableEdge newEdge2 = albert.modify().newEdge(EDGE2_TYPE_NAME, kimbal, "since", "today");
      assertThat(albert.getIdentity()).isEqualTo(newEdge2.getOut());
      assertThat(albert).isEqualTo(newEdge2.getOutVertex());
      assertThat(kimbal.getIdentity()).isEqualTo(newEdge2.getIn());
      assertThat(kimbal).isEqualTo(newEdge2.getInVertex());
      newEdge2.delete();

      final Edge edge = albert.getEdges(OUT, EDGE2_TYPE_NAME).iterator().next();
      assertThat(albert.getIdentity()).isEqualTo(edge.getOut());
      assertThat(albert).isEqualTo(edge.getOutVertex());
      assertThat(kimbal.getIdentity()).isEqualTo(edge.getIn());
      assertThat(kimbal).isEqualTo(edge.getInVertex());
      assertThat(edge.getBoolean("updated")).isTrue();

      // DELETE THE EDGE
      edge.delete();
      assertThat(albert.getEdges(OUT, EDGE2_TYPE_NAME).iterator().hasNext()).isFalse();

      // DELETE ONE VERTEX
      albert.delete();
      try {
        database.lookupByRID(albert.getIdentity());
        fail();
      } catch (final RecordNotFoundException e) {
        // EXPECTED
      }
    });
  }

  @Test
  public void testTransactionIsolation() throws Exception {
    testEachServer((serverIndex) -> {
      final int TOTAL_TRANSACTIONS = 100;
      final int BATCH_SIZE = 100;

      assertThat(new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
          DATABASE_NAME)).isTrue();

      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      final RemoteDatabase database2 = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("sql", "create vertex type Person");

      // CREATE A PARALLEL THREAD THAT EXECUTE QUERIES AND BROWSE ALL THE RECORDS
      final AtomicBoolean checkThreadRunning = new AtomicBoolean(true);
      final Thread checkThread = new Thread(() -> {
        try {
          while (checkThreadRunning.get()) {
            Thread.sleep(1000);
            ResultSet result = database.query("SQL", "select from Person");
            assertThat(result.hasNext()).isTrue();
            int total = 0;
            while (result.hasNext()) {
              assertThat(result.next().<String>getProperty("name")).isNotNull();
              ++total;
            }
            //System.out.println("Parallel thread browsed " + total + " records (limit 20,000)");
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
      checkThread.start();

      for (int i = 0; i < TOTAL_TRANSACTIONS; i++) {
        try {
          final int executedBatches = i + 1;

          database.transaction(() -> {
            for (int j = 0; j < BATCH_SIZE; j++) {
              final MutableDocument jay = database.newDocument("Person").set("name", "Jay").save();
              assertThat(jay).isNotNull();
              assertThat(jay.getString("name")).isEqualTo("Jay");
              assertThat(jay.getIdentity()).isNotNull();
              jay.save();
            }

            // TEST ISOLATION: COUNT SHOULD SEE THE MOST RECENT CHANGES BEFORE THE COMMIT
            ResultSet result = database.query("SQL", "select count(*) as total from Person");
            assertThat(result.hasNext()).isTrue();
            assertThat((int) result.next().getProperty("total")).isEqualTo(executedBatches * BATCH_SIZE);

            // CHECK ON A PARALLEL CONNECTION THE TX ISOLATION (RECORDS LESS THEN INSERTED)
            result = database2.query("SQL", "select count(*) as total from Person");
            assertThat(result.hasNext()).isTrue();
            final int totalRecord = result.next().getProperty("total");
            assertThat(totalRecord).isLessThan(executedBatches * BATCH_SIZE).withFailMessage(
                "Found total " + totalRecord + " records but should be less than " + (executedBatches * BATCH_SIZE));

            //System.out.println("BATCH " + executedBatches + "/" + TOTAL_TRANSACTIONS);
          });
        } catch (Throwable e) {
          System.err.println("Exception at transaction " + i + "/" + TOTAL_TRANSACTIONS);
          e.printStackTrace();
          break;
        }
      }

      checkThreadRunning.set(false);
      checkThread.join(5000);

      // RETRIEVE DOCUMENT WITH QUERY AFTER COMMIT
      final ResultSet result = database.query("SQL", "select count(*) as total from Person");
      assertThat(result.hasNext()).isTrue();
      assertThat((int) result.next().getProperty("total")).isEqualTo(TOTAL_TRANSACTIONS * BATCH_SIZE);
    });
  }

  @Test
  public void testRIDAsParametersInSQL() throws Exception {
    testEachServer((serverIndex) -> {
      assertThat(new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
          DATABASE_NAME)).isTrue();

      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.getSchema().createVertexType("VT");
      database.getSchema().createEdgeType("ET");
      final Vertex v1 = database.newVertex("VT").save();
      final Vertex v2 = database.newVertex("VT").save();

      String statement = "CREATE EDGE ET FROM :fromRid TO :toRid";

      Map<String, Object> params = new HashMap<>();
      params.put("fromRid", v1.getIdentity());
      params.put("toRid", v2.getIdentity());

      database.command("sql", statement, params);

      System.out.println("Done ... ");
    });
  }

  @Test
  public void testDropRemoteInheritanceBroken() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("sqlscript", """
          CREATE VERTEX TYPE AVtx;
          CREATE VERTEX TYPE BVtx EXTENDS AVtx;
          CREATE VERTEX TYPE CVtx EXTENDS BVtx;""");

      database.command("sql", "SELECT FROM AVtx;");
      database.command("sql", "DROP TYPE CVtx;");
      database.command("sql", "SELECT FROM AVtx;");
    });
  }

  @Test
  public void testTransactionWrongSessionId() throws Exception {
    testEachServer((serverIndex) -> {
      assertThat(new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
          DATABASE_NAME)).isTrue();

      final RemoteDatabase database1 = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database1.command("sql", "create vertex type Person");

      database1.begin();

      final MutableDocument jay = database1.newDocument("Person").set("name", "Jay").save();
      assertThat(jay).isNotNull();
      assertThat(jay.getString("name")).isEqualTo("Jay");
      assertThat(jay.getIdentity()).isNotNull();
      jay.save();

      final String sessionId = database1.getSessionId();
      database1.setSessionId(sessionId + "1");

      try {
        final MutableDocument albert = database1.newDocument("Person").set("name", "John").save();
        fail();
      } catch (TransactionException e) {
        // EXPECTED
      }

      database1.setSessionId(sessionId);

      database1.commit();
    });
  }

  @Test
  public void testTxVisibility() throws Exception {
    testEachServer((serverIndex) -> {
      assertThat(new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
          DATABASE_NAME)).isTrue();

      final RemoteDatabase t1 = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      final RemoteDatabase t2 = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      t1.command("sql", "create vertex type SimpleVertex");

      t1.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      MutableVertex svt1 = t1.newVertex("SimpleVertex");
      svt1.set("s", "concurrent t1");
      svt1.save();

      RID rid = svt1.getIdentity();

      t1.commit();
      t1.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      svt1 = t1.lookupByRID(rid).asVertex().modify();

      // recover the same vertex over t2
      t2.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      MutableVertex svt2 = t2.lookupByRID(rid).asVertex().modify();

      svt2.set("s", "concurrent t2");
      svt2.save();
      t2.commit();
      svt2 = t2.lookupByRID(rid).asVertex().modify();

      svt1.set("s", "concurrent t1 - 2");
      svt1.save();
      try {
        t1.commit();
        fail("Expected ConcurrentModificationException due to concurrent update, but commit succeeded");
      } catch (ConcurrentModificationException e) {
        // EXPECTED
      }
    });
  }

  @Test
  public void testDatabaseClose() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      assertThat(database.isOpen()).isTrue();
      database.close();
      assertThat(database.isOpen()).isFalse();
      try {
        database.countType("aaa", true);
        fail();
      } catch (DatabaseIsClosedException e) {
        //EXPECTED
      }
    });
  }

  @Test
  public void testDatabaseUniqueIndex() throws Exception {
    testEachServer((serverIndex) -> {
      try (RemoteDatabase tx = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);) {
        tx.getSchema().createVertexType("SimpleVertexEx").createProperty("svuuid", String.class)
            .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);

        tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

        MutableVertex svt1 = tx.newVertex("SimpleVertexEx");
        String uuid1 = UUID.randomUUID().toString();
        svt1.set("svex", uuid1);
        svt1.set("svuuid", uuid1);
        svt1.save();
        tx.commit();

        tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
        MutableVertex svt2 = tx.newVertex("SimpleVertexEx");
        String uuid2 = UUID.randomUUID().toString();
        svt2.set("svex", uuid2);
        svt2.set("svuuid", uuid2);
        svt2.save();
        tx.commit();

        tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
        svt2.set("svuuid", uuid1);
        svt2.save();
        tx.commit();
      }
    });
  }

  @Test
  public void testDatabaseMVCC() throws Exception {
    testEachServer((serverIndex) -> {
      try (RemoteDatabase t1 = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        t1.getSchema().createVertexType("SimpleVertex");

        try (RemoteDatabase t2 = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
            BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

          t1.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
          t2.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

          t1.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
          MutableVertex mvSVt1 = t1.newVertex("SimpleVertex");
          mvSVt1.set("s", "init concurrent test");
          mvSVt1.save();

          RID rid = mvSVt1.getIdentity();
          System.out.println("RID: " + rid);
          t1.commit();
//        t1.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
          Vertex vSVt1 = t1.lookupByRID(rid).asVertex();

          t2.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
          Vertex vSVt2 = t2.lookupByRID(rid).asVertex();

          t1.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
          mvSVt1 = vSVt1.modify();
          System.out.println("mvSVt1: " + vSVt1);
          mvSVt1.set("s", "concurrent t1");
          mvSVt1.save();
          t1.commit();

          vSVt1 = t1.lookupByRID(rid).asVertex();
          System.out.println("vt1: " + vSVt1.propertiesAsMap());

          System.out.println("s from t1: " + vSVt1.getString("s"));
          System.out.println("s from t2: " + vSVt2.getString("s"));

          MutableVertex mvSVt2 = vSVt2.modify();
          mvSVt2.set("s", "concurrent t2");
          mvSVt2.save();

          try {
            t2.commit();
            Assertions.fail();
          } catch (ConcurrentModificationException e) {
            // EXPECTED
          }
        }
      }
    });
  }




  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
