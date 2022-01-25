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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class CheckDatabaseTest extends TestHelper {

  private static final int           TOTAL = 10_000;
  private              MutableVertex root;

  @Test
  public void checkDatabase() {
    final ResultSet result = database.command("sql", "check database");
    Assertions.assertTrue(result.hasNext());
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(TOTAL, (Long) row.getProperty("totalActiveVertices"));
      Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalActiveEdges"));
      Assertions.assertEquals(0, (Long) row.getProperty("autoFix"));
    }
  }

  @Test
  public void checkTypes() {
    ResultSet result = database.command("sql", "check database type Person");
    Assertions.assertTrue(result.hasNext());
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(TOTAL, (Long) row.getProperty("totalActiveRecords"));
      Assertions.assertEquals(TOTAL, (Long) row.getProperty("totalActiveVertices"));
      Assertions.assertEquals(0, (Long) row.getProperty("autoFix"));
    }

    result = database.command("sql", "check database type Person, Knows");
    Assertions.assertTrue(result.hasNext());
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(TOTAL * 2 - 1, (Long) row.getProperty("totalActiveRecords"));
      Assertions.assertEquals(TOTAL, (Long) row.getProperty("totalActiveVertices"));
      Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalActiveEdges"));
      Assertions.assertEquals(0, (Long) row.getProperty("autoFix"));
    }
  }

  @Test
  public void checkRegularDeleteEdges() {
    database.transaction(() -> database.command("sql", "delete from Knows"));

    ResultSet result = database.command("sql", "check database");
    Assertions.assertTrue(result.hasNext());
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(0L, (Long) row.getProperty("autoFix"));
      Assertions.assertEquals(TOTAL, (Long) row.getProperty("totalActiveVertices"));
      Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalAllocatedEdges"));
      Assertions.assertEquals(0L, (Long) row.getProperty("totalActiveEdges"));
      Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalDeletedRecords"));
      Assertions.assertEquals(0, ((Collection) row.getProperty("corruptedRecords")).size());
      Assertions.assertEquals(0L, (Long) row.getProperty("missingReferenceBack"));
      Assertions.assertEquals(0L, (Long) row.getProperty("invalidLinks"));
      Assertions.assertEquals(0, ((Collection) row.getProperty("warnings")).size());
    }
  }

  @Test
  public void checkBrokenDeletedEdges() {
    final AtomicReference<RID> deletedEdge = new AtomicReference<>();

    database.transaction(() -> {
      final Iterator<Record> iter = database.iterateType("Knows", false);
      Assertions.assertTrue(iter.hasNext());

      final Record edge = iter.next();
      deletedEdge.set(edge.getIdentity());

      Assertions.assertEquals(root.getIdentity(), edge.asEdge().getOut());

      // DELETE THE EDGE AT LOW LEVEL
      database.getSchema().getBucketById(edge.getIdentity().getBucketId()).deleteRecord(edge.getIdentity());
    });

    ResultSet result = database.command("sql", "check database");
    Assertions.assertTrue(result.hasNext());
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(0, (Long) row.getProperty("autoFix"));
      Assertions.assertEquals(TOTAL, (Long) row.getProperty("totalActiveVertices"));
      Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalAllocatedEdges"));
      Assertions.assertEquals(TOTAL - 2, (Long) row.getProperty("totalActiveEdges"));
      Assertions.assertEquals(1, (Long) row.getProperty("totalDeletedRecords"));
      Assertions.assertEquals(1, ((Collection) row.getProperty("corruptedRecords")).size());
      Assertions.assertEquals(0, (Long) row.getProperty("missingReferenceBack"));
      Assertions.assertEquals(2, (Long) row.getProperty("invalidLinks"));
      Assertions.assertEquals(1, ((Collection) row.getProperty("warnings")).size());
    }

    Assertions.assertEquals(TOTAL - 2, countEdges(root.getIdentity()));
    Assertions.assertEquals(TOTAL - 1, countEdgesSegmentList(root.getIdentity()));

    result = database.command("sql", "check database fix");
    Assertions.assertTrue(result.hasNext());
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(1, (Long) row.getProperty("autoFix"));
      Assertions.assertEquals(TOTAL, (Long) row.getProperty("totalActiveVertices"));
      Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalAllocatedEdges"));
      Assertions.assertEquals(TOTAL - 2, (Long) row.getProperty("totalActiveEdges"));
      Assertions.assertEquals(1, (Long) row.getProperty("totalDeletedRecords"));
      Assertions.assertEquals(1, ((Collection) row.getProperty("corruptedRecords")).size());
      Assertions.assertEquals(0, (Long) row.getProperty("missingReferenceBack"));
      Assertions.assertEquals(2, (Long) row.getProperty("invalidLinks"));
      Assertions.assertEquals(1, ((Collection) row.getProperty("warnings")).size());
    }

    Assertions.assertEquals(TOTAL - 2, countEdges(root.getIdentity()));
    Assertions.assertEquals(TOTAL - 2, countEdgesSegmentList(root.getIdentity()));

    result = database.command("sql", "check database fix");
    Assertions.assertTrue(result.hasNext());
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(0, (Long) row.getProperty("autoFix"));
      Assertions.assertEquals(TOTAL, (Long) row.getProperty("totalActiveVertices"));
      Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalAllocatedEdges"));
      Assertions.assertEquals(TOTAL - 2, (Long) row.getProperty("totalActiveEdges"));
      Assertions.assertEquals(1, (Long) row.getProperty("totalDeletedRecords"));
      Assertions.assertEquals(0, ((Collection) row.getProperty("corruptedRecords")).size());
      Assertions.assertEquals(0, (Long) row.getProperty("missingReferenceBack"));
      Assertions.assertEquals(0, (Long) row.getProperty("invalidLinks"));
      Assertions.assertEquals(0, ((Collection) row.getProperty("warnings")).size());
    }

    Assertions.assertEquals(TOTAL - 2, countEdges(root.getIdentity()));
    Assertions.assertEquals(TOTAL - 2, countEdgesSegmentList(root.getIdentity()));
  }

  @Test
  public void checkBrokenDeletedVertex() {
    database.transaction(() -> {
      // DELETE THE VERTEX AT LOW LEVEL
      database.getSchema().getBucketById(root.getIdentity().getBucketId()).deleteRecord(root.getIdentity());
    });

    ResultSet result = database.command("sql", "check database");
    Assertions.assertTrue(result.hasNext());
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(0, (Long) row.getProperty("autoFix"));
      Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalActiveVertices"));
      Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalAllocatedEdges"));
      Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalActiveEdges"));
      Assertions.assertEquals(1, (Long) row.getProperty("totalDeletedRecords"));
      Assertions.assertEquals(TOTAL, ((Collection) row.getProperty("corruptedRecords")).size()); // ALL THE EDGES + ROOT VERTEX
      Assertions.assertEquals(0, (Long) row.getProperty("missingReferenceBack"));
      Assertions.assertEquals((TOTAL - 1) * 2, (Long) row.getProperty("invalidLinks"));
      Assertions.assertEquals(TOTAL - 1, ((Collection) row.getProperty("warnings")).size());
    }

    result = database.command("sql", "check database fix");
    Assertions.assertTrue(result.hasNext());
    Result row = result.next();

    Assertions.assertEquals("check database", row.getProperty("operation"));
    Assertions.assertEquals((TOTAL - 1) * 2, (Long) row.getProperty("autoFix"));
    Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalActiveVertices"));
    Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalAllocatedEdges"));
    Assertions.assertEquals(0, (Long) row.getProperty("totalActiveEdges"));
    Assertions.assertEquals(TOTAL, (Long) row.getProperty("totalDeletedRecords"));
    Assertions.assertEquals(TOTAL - 1, ((Collection) row.getProperty("corruptedRecords")).size());
    Assertions.assertEquals(0, (Long) row.getProperty("missingReferenceBack"));
    Assertions.assertEquals((TOTAL - 1) * 2, (Long) row.getProperty("invalidLinks"));
    Assertions.assertEquals((TOTAL - 1) * 2, ((Collection) row.getProperty("warnings")).size());

    result = database.command("sql", "check database");
    Assertions.assertTrue(result.hasNext());

    row = result.next();

    Assertions.assertEquals("check database", row.getProperty("operation"));
    Assertions.assertEquals(0, (Long) row.getProperty("autoFix"));
    Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalActiveVertices"));
    Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalAllocatedEdges"));
    Assertions.assertEquals(0, (Long) row.getProperty("totalActiveEdges"));
    Assertions.assertEquals(TOTAL, (Long) row.getProperty("totalDeletedRecords"));
    Assertions.assertEquals(0, ((Collection) row.getProperty("corruptedRecords")).size());
    Assertions.assertEquals(0, (Long) row.getProperty("missingReferenceBack"));
    Assertions.assertEquals(0, (Long) row.getProperty("invalidLinks"));
    Assertions.assertEquals(0, ((Collection) row.getProperty("warnings")).size());
  }

  @Test
  public void checkBrokenPage() {
    database.transaction(() -> {
      final Bucket bucket = database.getSchema().getBucketById(root.getIdentity().bucketId);

      try {
        final MutablePage page = ((DatabaseInternal) database).getTransaction().getPageToModify(new PageId(bucket.getId(), 0), bucket.getPageSize(), false);
        for (int i = 0; i < page.getAvailableContentSize(); i++) {
          page.writeByte(i, (byte) 4);
        }
      } catch (IOException e) {
        Assertions.fail(e);
      }
    });

    ResultSet result = database.command("sql", "check database");
    Assertions.assertTrue(result.hasNext());

    Result row = result.next();
    Assertions.assertEquals("check database", row.getProperty("operation"));
    Assertions.assertTrue((Long) row.getProperty("errors") > 0L);
    Assertions.assertEquals(0L, (Long) row.getProperty("autoFix"));
    Assertions.assertTrue((Long) row.getProperty("totalActiveVertices") < TOTAL);
    Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalAllocatedEdges"));
    Assertions.assertEquals(TOTAL - 1, (Long) row.getProperty("totalActiveEdges"));
    Assertions.assertEquals(0L, (Long) row.getProperty("totalDeletedRecords"));
    Assertions.assertEquals(0L, (Long) row.getProperty("missingReferenceBack"));
    Assertions.assertTrue((Long) row.getProperty("invalidLinks") > 0L);
    Assertions.assertTrue(((Collection) row.getProperty("warnings")).size() > 0L);
    Assertions.assertEquals(1, ((Collection<String>) row.getProperty("rebuiltIndexes")).size());
    Assertions.assertTrue(((Collection) row.getProperty("corruptedRecords")).size() > 0L);

    result = database.command("sql", "check database fix");
    Assertions.assertTrue(result.hasNext());
    row = result.next();
    Assertions.assertTrue((Long) row.getProperty("autoFix") > 0L);
    Assertions.assertEquals(0L, (Long) row.getProperty("totalActiveEdges"));
    Assertions.assertEquals(1, ((Collection<String>) row.getProperty("rebuiltIndexes")).size());
    Assertions.assertTrue((Long) row.getProperty("totalActiveVertices") < TOTAL);
    Assertions.assertTrue(((Collection) row.getProperty("corruptedRecords")).size() > 0L);

    result = database.command("sql", "check database");
    Assertions.assertTrue(result.hasNext());
    row = result.next();
    Assertions.assertEquals(0L, (Long) row.getProperty("autoFix"));
    Assertions.assertEquals(0L, (Long) row.getProperty("errors"));
    Assertions.assertEquals(0L, (Long) row.getProperty("autoFix"));
    Assertions.assertEquals(0L, (Long) row.getProperty("missingReferenceBack"));
    Assertions.assertEquals(0L, (Long) row.getProperty("invalidLinks"));
    Assertions.assertEquals(0L, ((Collection) row.getProperty("warnings")).size());
    Assertions.assertEquals(0, ((Collection<String>) row.getProperty("rebuiltIndexes")).size());
    Assertions.assertEquals(0L, ((Collection) row.getProperty("corruptedRecords")).size());

    // CHECK CORRUPTED RECORD ARE NOT INDEXED ANYMORE
    List<TypeIndex> indexes = database.getSchema().getType("Person").getIndexesByProperties("id");
    Assertions.assertEquals(1, indexes.size());

    Assertions.assertEquals((Long) row.getProperty("totalActiveVertices"), indexes.get(0).countEntries());
  }

  @Override
  protected void beginTest() {
    database.command("sql", "create vertex type Person");
    database.command("sql", "create property Person.id string");
    database.command("sql", "create index `Person[id]` on Person (id) unique");
    database.command("sql", "create edge type Knows");
    database.transaction(() -> {
      root = database.newVertex("Person").set("name", "root", "id", 0).save();
      for (int i = 1; i <= TOTAL - 1; i++) {
        MutableVertex v = database.newVertex("Person").set("name", "test", "id", i).save();
        root.newEdge("Knows", v, true);
      }
    });
  }

  private int countEdges(RID rootVertex) {
    final Iterable<Edge> iter = rootVertex.asVertex().getEdges(Vertex.DIRECTION.OUT);
    int totalEdges = 0;
    for (Edge e : iter)
      ++totalEdges;
    return totalEdges;
  }

  private int countEdgesSegmentList(RID rootVertex) {
    final EdgeLinkedList outEdges = ((DatabaseInternal) database).getGraphEngine()
        .getEdgeHeadChunk((VertexInternal) rootVertex.asVertex(), Vertex.DIRECTION.OUT);

    return (int) outEdges.count(null);
  }
}
