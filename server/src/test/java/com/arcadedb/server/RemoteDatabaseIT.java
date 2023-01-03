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
package com.arcadedb.server;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class RemoteDatabaseIT extends BaseGraphServerTest {

  @Test
  public void simpleTxDocuments() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, "graph", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      // BEGIN
      database.transaction(() -> {
        // CREATE DOCUMENT
        ResultSet result = database.command("SQL", "insert into Person set name = 'Elon'");
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.hasNext());
        final Result rec = result.next();
        Assertions.assertTrue(rec.toJSON().contains("Elon"));
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
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, "graph", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      // BEGIN
      database.transaction(() -> {
        // CREATE VERTEX TYPE
        ResultSet result = database.command("SQL", "create vertex type Character");
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.hasNext());

        // CREATE VERTEX 1
        result = database.command("SQL", "insert into Character set name = 'Elon'");
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.hasNext());
        Result rec = result.next();
        Assertions.assertTrue(rec.toJSON().contains("Elon"));
        final RID rid1 = rec.getIdentity().get();

        // CREATE VERTEX 2
        result = database.command("SQL", "create vertex Character set name = 'Kimbal'");
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.hasNext());
        rec = result.next();
        Assertions.assertTrue(rec.toJSON().contains("Kimbal"));
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

        Document record = (Document) database.lookupByRID(rid1);
        Assertions.assertNotNull(record);
        Assertions.assertEquals("Elon", record.getString("name"));
        Assertions.assertEquals("Musk", record.getString("lastName"));

        record.toMap();
        record.toJSON();

        record = (Document) database.lookupByRID(rid2);
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
}
