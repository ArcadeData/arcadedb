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

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;

public class RemoteDatabaseIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  public void simpleTxDocuments() throws Exception {
    testEachServer((serverIndex) -> {
      Assertions.assertTrue(
          new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
              DATABASE_NAME));

      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("sql", "create vertex type Person");

      // BEGIN
      database.transaction(() -> {
        // CREATE DOCUMENT VIA API
        final MutableDocument jay = database.newDocument("Person").set("name", "Jay").save();
        Assertions.assertNotNull(jay);
        Assertions.assertEquals("Jay", jay.getString("name"));
        Assertions.assertNotNull(jay.getIdentity());
        jay.save();

        // TEST DELETION AND LOOKUP
        jay.delete();
        try {
          jay.reload();
          Assertions.fail();
        } catch (RecordNotFoundException e) {
          // EXPECTED
        }

        // CREATE DOCUMENT VIA SQL
        ResultSet result = database.command("SQL", "insert into Person set name = 'Elon'");
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.hasNext());
        final Result rec = result.next();
        Assertions.assertTrue(rec.toJSON().contains("Elon"));
        Assertions.assertEquals("Elon", rec.toElement().toMap().get("name"));
        final RID rid = rec.toElement().getIdentity();

        // RETRIEVE DOCUMENT WITH QUERY
        result = database.query("SQL", "select from Person where name = 'Elon'");
        Assertions.assertTrue(result.hasNext());

        // UPDATE DOCUMENT WITH COMMAND
        result = database.command("SQL", "update Person set lastName = 'Musk' where name = 'Elon'");
        Assertions.assertTrue(result.hasNext());
        Assertions.assertEquals(1, new JSONObject(result.next().toJSON()).getInt("count"));

        final Document record = (Document) database.lookupByRID(rid);
        Assertions.assertNotNull(result);
        Assertions.assertEquals("Musk", record.getString("lastName"));

        Assertions.assertEquals(1L, database.countType("Person", true));
        Assertions.assertEquals(1L, database.countType("Person", false));

        long totalInBuckets = 0L;
        for (int i = 0; i < 100; i++) {
          try {
            totalInBuckets += database.countBucket("Person_" + i);
          } catch (Exception e) {
            // IGNORE IT
            break;
          }
        }
        Assertions.assertEquals(1L, totalInBuckets);
      });

      // RETRIEVE DOCUMENT WITH QUERY AFTER COMMIT
      final ResultSet result = database.query("SQL", "select from Person where name = 'Elon'");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals("Musk", result.next().getProperty("lastName"));
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
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.hasNext());

        // CREATE DOCUMENT VIA API
        final MutableVertex jay = database.newVertex("Character").set("name", "Jay").save();
        Assertions.assertNotNull(jay);
        Assertions.assertEquals("Jay", jay.getString("name"));
        Assertions.assertNotNull(jay.getIdentity());
        jay.save();

        // TEST DELETION AND LOOKUP
        jay.delete();
        try {
          jay.reload();
          Assertions.fail();
        } catch (RecordNotFoundException e) {
          // EXPECTED
        }

        // CREATE VERTEX 1
        result = database.command("SQL", "insert into Character set name = 'Elon'");
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.hasNext());
        Result rec = result.next();
        Assertions.assertTrue(rec.toJSON().contains("Elon"));
        Assertions.assertEquals("Elon", rec.toElement().toMap().get("name"));
        final RID rid1 = rec.getIdentity().get();

        // CREATE VERTEX 2
        result = database.command("SQL", "create vertex Character set name = 'Kimbal'");
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.hasNext());
        rec = result.next();
        Assertions.assertTrue(rec.toJSON().contains("Kimbal"));
        Assertions.assertEquals("Kimbal", rec.toElement().toMap().get("name"));
        final RID rid2 = rec.getIdentity().get();

        // RETRIEVE VERTEX WITH QUERY
        result = database.query("SQL", "select from Character where name = 'Elon'");
        Assertions.assertTrue(result.hasNext());
        Assertions.assertTrue(result.next().isVertex());

        // UPDATE VERTEX WITH COMMAND
        result = database.command("SQL", "update Character set lastName = 'Musk' where name = 'Elon' or name = 'Kimbal'");
        Assertions.assertTrue(result.hasNext());
        Assertions.assertEquals(2, new JSONObject(result.next().toJSON()).getInt("count"));

        // CREATE EDGE WITH COMMAND
        result = database.command("SQL", "create edge " + EDGE1_TYPE_NAME + " from " + rid1 + " to " + rid2);
        Assertions.assertTrue(result.hasNext());
        final Edge edge = result.next().getEdge().get();

        edge.toMap();
        edge.toJSON();

        Assertions.assertEquals(EDGE1_TYPE_NAME, edge.getTypeName());
        Assertions.assertEquals(rid1, edge.getOut());
        Assertions.assertEquals(rid2, edge.getIn());

        Vertex record = (Vertex) database.lookupByRID(rid1);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("Elon", record.getString("name"));
        Assertions.assertEquals("Musk", record.getString("lastName"));

        record.toMap();
        record.toJSON();

        record = (Vertex) database.lookupByRID(rid2);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("Kimbal", record.getString("name"));
        Assertions.assertEquals("Musk", record.getString("lastName"));

        final MutableDocument mutable = record.modify();
        mutable.set("extra", 100);
        mutable.save();
      });

      // RETRIEVE VERTEX WITH QUERY AFTER COMMIT
      final ResultSet result = database.query("SQL", "select from Character where name = 'Kimbal'");
      Assertions.assertTrue(result.hasNext());
      final Result record = result.next();
      Assertions.assertTrue(record.isVertex());

      final Vertex kimbal = record.getVertex().get();
      Assertions.assertEquals("Musk", kimbal.getString("lastName"));
      Assertions.assertEquals(100, kimbal.getInteger("extra"));

      Assertions.assertTrue(kimbal.toMap().containsKey("@cat"));
      Assertions.assertTrue(kimbal.toMap().containsKey("@type"));
      Assertions.assertFalse(kimbal.toMap().containsKey("@out"));
      Assertions.assertFalse(kimbal.toMap().containsKey("@in"));

      kimbal.toJSON();

      final Iterator<Vertex> connected = kimbal.getVertices(Vertex.DIRECTION.IN).iterator();
      Assertions.assertTrue(connected.hasNext());
      final Vertex elon = connected.next();
      Assertions.assertEquals("Musk", elon.getString("lastName"));

      Assertions.assertEquals(1L, kimbal.countEdges(Vertex.DIRECTION.IN, null));
      Assertions.assertEquals(1L, kimbal.countEdges(Vertex.DIRECTION.IN, EDGE1_TYPE_NAME));
      Assertions.assertEquals(0L, kimbal.countEdges(Vertex.DIRECTION.IN, EDGE2_TYPE_NAME));
      Assertions.assertEquals(0, kimbal.countEdges(Vertex.DIRECTION.OUT, null));
      Assertions.assertEquals(0, kimbal.countEdges(Vertex.DIRECTION.OUT, EDGE1_TYPE_NAME));
      Assertions.assertEquals(0L, kimbal.countEdges(Vertex.DIRECTION.OUT, EDGE2_TYPE_NAME));

      Assertions.assertEquals(1L, elon.countEdges(Vertex.DIRECTION.OUT, null));
      Assertions.assertEquals(1L, elon.countEdges(Vertex.DIRECTION.OUT, EDGE1_TYPE_NAME));
      Assertions.assertEquals(0L, elon.countEdges(Vertex.DIRECTION.OUT, EDGE2_TYPE_NAME));
      Assertions.assertEquals(0, elon.countEdges(Vertex.DIRECTION.IN, null));
      Assertions.assertEquals(0, elon.countEdges(Vertex.DIRECTION.IN, EDGE1_TYPE_NAME));
      Assertions.assertEquals(0L, elon.countEdges(Vertex.DIRECTION.IN, EDGE2_TYPE_NAME));

      Assertions.assertTrue(kimbal.isConnectedTo(elon.getIdentity()));
      Assertions.assertTrue(kimbal.isConnectedTo(elon.getIdentity(), Vertex.DIRECTION.IN));
      Assertions.assertFalse(kimbal.isConnectedTo(elon.getIdentity(), Vertex.DIRECTION.OUT));

      Assertions.assertTrue(elon.isConnectedTo(kimbal.getIdentity()));
      Assertions.assertTrue(elon.isConnectedTo(kimbal.getIdentity(), Vertex.DIRECTION.OUT));
      Assertions.assertFalse(elon.isConnectedTo(kimbal.getIdentity(), Vertex.DIRECTION.IN));

      final MutableEdge newEdge = elon.newEdge(EDGE2_TYPE_NAME, kimbal, true, "since", "today");
      Assertions.assertEquals(newEdge.getOut(), elon.getIdentity());
      Assertions.assertEquals(newEdge.getOutVertex(), elon);
      Assertions.assertEquals(newEdge.getIn(), kimbal.getIdentity());
      Assertions.assertEquals(newEdge.getInVertex(), kimbal);

      newEdge.set("updated", true);
      newEdge.save();

      // SAME BUT FROM A MUTABLE INSTANCE
      final MutableEdge newEdge2 = elon.modify().newEdge(EDGE2_TYPE_NAME, kimbal, true, "since", "today");
      Assertions.assertEquals(newEdge2.getOut(), elon.getIdentity());
      Assertions.assertEquals(newEdge2.getOutVertex(), elon);
      Assertions.assertEquals(newEdge2.getIn(), kimbal.getIdentity());
      Assertions.assertEquals(newEdge2.getInVertex(), kimbal);
      newEdge2.delete();

      final Edge edge = elon.getEdges(Vertex.DIRECTION.OUT, EDGE2_TYPE_NAME).iterator().next();
      Assertions.assertEquals(edge.getOut(), elon.getIdentity());
      Assertions.assertEquals(edge.getOutVertex(), elon);
      Assertions.assertEquals(edge.getIn(), kimbal.getIdentity());
      Assertions.assertEquals(edge.getInVertex(), kimbal);
      Assertions.assertTrue(edge.getBoolean("updated"));

      // DELETE THE EDGE
      edge.delete();
      Assertions.assertFalse(elon.getEdges(Vertex.DIRECTION.OUT, EDGE2_TYPE_NAME).iterator().hasNext());

      // DELETE ONE VERTEX
      elon.delete();
      try {
        database.lookupByRID(elon.getIdentity());
        Assertions.fail();
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

      Assertions.assertTrue(
          new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
              DATABASE_NAME));

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
            Assertions.assertTrue(result.hasNext());
            int total = 0;
            while (result.hasNext()) {
              Assertions.assertNotNull(result.next().getProperty("name"));
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
              Assertions.assertNotNull(jay);
              Assertions.assertEquals("Jay", jay.getString("name"));
              Assertions.assertNotNull(jay.getIdentity());
              jay.save();
            }

            // TEST ISOLATION: COUNT SHOULD SEE THE MOST RECENT CHANGES BEFORE THE COMMIT
            ResultSet result = database.query("SQL", "select count(*) as total from Person");
            Assertions.assertTrue(result.hasNext());
            Assertions.assertEquals(executedBatches * BATCH_SIZE, (int) result.next().getProperty("total"));

            // CHECK ON A PARALLEL CONNECTION THE TX ISOLATION (RECORDS LESS THEN INSERTED)
            result = database2.query("SQL", "select count(*) as total from Person");
            Assertions.assertTrue(result.hasNext());
            final int totalRecord = result.next().getProperty("total");
            Assertions.assertTrue(totalRecord < executedBatches * BATCH_SIZE,
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
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(TOTAL_TRANSACTIONS * BATCH_SIZE, (int) result.next().getProperty("total"));
    });
  }

  @Test
  public void testRIDAsParametersInSQL() throws Exception {
    testEachServer((serverIndex) -> {
      Assertions.assertTrue(
          new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
              DATABASE_NAME));

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
  public void testTransactionWrongSessionId() throws Exception {
    testEachServer((serverIndex) -> {
      Assertions.assertTrue(
          new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
              DATABASE_NAME));

      final RemoteDatabase database1 = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database1.command("sql", "create vertex type Person");

      database1.begin();

      final MutableDocument jay = database1.newDocument("Person").set("name", "Jay").save();
      Assertions.assertNotNull(jay);
      Assertions.assertEquals("Jay", jay.getString("name"));
      Assertions.assertNotNull(jay.getIdentity());
      jay.save();

      final String sessionId = database1.getSessionId();
      database1.setSessionId(sessionId + "1");

      try {
        final MutableDocument elon = database1.newDocument("Person").set("name", "Elon").save();
        Assertions.fail();
      } catch (TransactionException e) {
        // EXPECTED
      }

      database1.setSessionId(sessionId);

      database1.commit();
    });
  }

  @Test
  public void testDatabaseClose() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      Assertions.assertTrue(database.isOpen());
      database.close();
      Assertions.assertFalse(database.isOpen());
      try {
        database.countType("aaa", true);
        Assertions.fail();
      } catch (DatabaseIsClosedException e) {
        //EXPECTED
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
