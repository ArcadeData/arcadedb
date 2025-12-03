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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class CheckDatabaseTest extends TestHelper {

  private static final int           TOTAL = 10_000;
  private              MutableVertex root;

  @Test
  void checkDatabase() {
    final ResultSet result = database.command("sql", "check database");
    assertThat(result.hasNext()).isTrue();
    while (result.hasNext()) {
      final Result row = result.next();

      assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
      assertThat(row.<Long>getProperty("totalActiveVertices")).isEqualTo(TOTAL);
      assertThat(row.<Long>getProperty("totalActiveEdges")).isEqualTo(TOTAL - 1);
      assertThat(row.<Long>getProperty("autoFix")).isEqualTo(0L);
    }
  }

  @Test
  void checkTypes() {
    ResultSet result = database.command("sql", "check database type Person");
    assertThat(result.hasNext()).isTrue();
    while (result.hasNext()) {
      final Result row = result.next();

      assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
      assertThat((Long) row.getProperty("totalActiveRecords")).isEqualTo(TOTAL);
      assertThat((Long) row.getProperty("totalActiveVertices")).isEqualTo(TOTAL);
      assertThat((Long) row.getProperty("autoFix")).isEqualTo(0);
    }

    result = database.command("sql", "check database type Person, Knows");
    assertThat(result.hasNext()).isTrue();
    while (result.hasNext()) {
      final Result row = result.next();

      assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
      assertThat((Long) row.getProperty("totalActiveRecords")).isEqualTo(TOTAL * 2 - 1);
      assertThat((Long) row.getProperty("totalActiveVertices")).isEqualTo(TOTAL);
      assertThat((Long) row.getProperty("totalActiveEdges")).isEqualTo(TOTAL - 1);
      assertThat((Long) row.getProperty("autoFix")).isEqualTo(0);
    }
  }

  @Test
  void checkRegularDeleteEdges() {
    database.transaction(() -> database.command("sql", "delete from Knows"));

    final ResultSet result = database.command("sql", "check database");
    assertThat(result.hasNext()).isTrue();
    while (result.hasNext()) {
      final Result row = result.next();

      assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
      assertThat((Long) row.getProperty("autoFix")).isEqualTo(0L);
      assertThat((Long) row.getProperty("totalActiveVertices")).isEqualTo(TOTAL);
      assertThat((Long) row.getProperty("totalAllocatedEdges")).isEqualTo(0);
      assertThat((Long) row.getProperty("totalActiveEdges")).isEqualTo(0L);
      assertThat((Long) row.getProperty("totalDeletedRecords")).isEqualTo(13);
      assertThat(((Collection) row.getProperty("corruptedRecords")).size()).isEqualTo(0);
      assertThat((Long) row.getProperty("missingReferenceBack")).isEqualTo(0L);
      assertThat((Long) row.getProperty("invalidLinks")).isEqualTo(0L);
      assertThat(((Collection) row.getProperty("warnings")).size()).isEqualTo(0);
    }
  }

  @Test
  void checkBrokenDeletedEdges() {
    final AtomicReference<RID> deletedEdge = new AtomicReference<>();

    database.transaction(() -> {
      final Iterator<Record> iter = database.iterateType("Knows", false);
      assertThat(iter.hasNext()).isTrue();

      final Record edge = iter.next();
      deletedEdge.set(edge.getIdentity());

      assertThat(edge.asEdge().getOut()).isEqualTo(root.getIdentity());

      // DELETE THE EDGE AT LOW LEVEL
      database.getSchema().getBucketById(edge.getIdentity().getBucketId()).deleteRecord(edge.getIdentity());
    });

    ResultSet result = database.command("sql", "check database");
    assertThat(result.hasNext()).isTrue();
    while (result.hasNext()) {
      final Result row = result.next();

      assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
      assertThat((Long) row.getProperty("autoFix")).isEqualTo(0);
      assertThat((Long) row.getProperty("totalActiveVertices")).isEqualTo(TOTAL);
      assertThat((Long) row.getProperty("totalAllocatedEdges")).isEqualTo(TOTAL - 2);
      assertThat((Long) row.getProperty("totalActiveEdges")).isEqualTo(TOTAL - 2);
      assertThat((Long) row.getProperty("totalDeletedRecords")).isEqualTo(1);
      assertThat(((Collection) row.getProperty("corruptedRecords")).size()).isEqualTo(1);
      assertThat((Long) row.getProperty("missingReferenceBack")).isEqualTo(0);
      assertThat((Long) row.getProperty("invalidLinks")).isEqualTo(2);
      assertThat(((Collection) row.getProperty("warnings")).size()).isEqualTo(1);
    }

    assertThat(countEdges(root.getIdentity())).isEqualTo(TOTAL - 2);
    assertThat(countEdgesSegmentList(root.getIdentity())).isEqualTo(TOTAL - 1);

    result = database.command("sql", "check database fix");
    assertThat(result.hasNext()).isTrue();
    while (result.hasNext()) {
      final Result row = result.next();

      assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
      assertThat((Long) row.getProperty("autoFix")).isEqualTo(1);
      assertThat((Long) row.getProperty("totalActiveVertices")).isEqualTo(TOTAL);
      assertThat((Long) row.getProperty("totalAllocatedEdges")).isEqualTo(TOTAL - 2);
      assertThat((Long) row.getProperty("totalActiveEdges")).isEqualTo(TOTAL - 2);
      assertThat((Long) row.getProperty("totalDeletedRecords")).isEqualTo(1);
      assertThat(((Collection) row.getProperty("corruptedRecords")).size()).isEqualTo(1);
      assertThat((Long) row.getProperty("missingReferenceBack")).isEqualTo(0);
      assertThat((Long) row.getProperty("invalidLinks")).isEqualTo(2);
      assertThat(((Collection) row.getProperty("warnings")).size()).isEqualTo(1);
    }

    assertThat(countEdges(root.getIdentity())).isEqualTo(TOTAL - 2);
    assertThat(countEdgesSegmentList(root.getIdentity())).isEqualTo(TOTAL - 2);

    result = database.command("sql", "check database fix");
    assertThat(result.hasNext()).isTrue();
    while (result.hasNext()) {
      final Result row = result.next();

      assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
      assertThat((Long) row.getProperty("autoFix")).isEqualTo(0);
      assertThat((Long) row.getProperty("totalActiveVertices")).isEqualTo(TOTAL);
      assertThat((Long) row.getProperty("totalAllocatedEdges")).isEqualTo(TOTAL - 2);
      assertThat((Long) row.getProperty("totalActiveEdges")).isEqualTo(TOTAL - 2);
      assertThat((Long) row.getProperty("totalDeletedRecords")).isEqualTo(1);
      assertThat(((Collection) row.getProperty("corruptedRecords")).size()).isEqualTo(0);
      assertThat((Long) row.getProperty("missingReferenceBack")).isEqualTo(0);
      assertThat((Long) row.getProperty("invalidLinks")).isEqualTo(0);
      assertThat(((Collection) row.getProperty("warnings")).size()).isEqualTo(0);
    }

    assertThat(countEdges(root.getIdentity())).isEqualTo(TOTAL - 2);
    assertThat(countEdgesSegmentList(root.getIdentity())).isEqualTo(TOTAL - 2);
  }

  @Test
  void checkBrokenDeletedVertex() {
    database.transaction(() -> {
      // DELETE THE VERTEX AT LOW LEVEL
      database.getSchema().getBucketById(root.getIdentity().getBucketId()).deleteRecord(root.getIdentity());
    });

    ResultSet result = database.command("sql", "check database");
    assertThat(result.hasNext()).isTrue();
    while (result.hasNext()) {
      final Result row = result.next();

      assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
      assertThat((Long) row.getProperty("autoFix")).isEqualTo(0);
      assertThat((Long) row.getProperty("totalActiveVertices")).isEqualTo(TOTAL - 1);
      assertThat((Long) row.getProperty("totalAllocatedEdges")).isEqualTo(TOTAL - 1);
      assertThat((Long) row.getProperty("totalActiveEdges")).isEqualTo(TOTAL - 1);
      assertThat((Long) row.getProperty("totalDeletedRecords")).isEqualTo(1);
      assertThat(((Collection) row.getProperty("corruptedRecords")).size()).isEqualTo(TOTAL); // ALL THE EDGES + ROOT VERTEX
      assertThat((Long) row.getProperty("missingReferenceBack")).isEqualTo(0);
      assertThat((Long) row.getProperty("invalidLinks")).isEqualTo((TOTAL - 1) * 2);
      assertThat(((Collection) row.getProperty("warnings")).size()).isEqualTo(TOTAL - 1);
    }

    result = database.command("sql", "check database fix");
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();

    assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
    assertThat((Long) row.getProperty("autoFix")).isEqualTo((TOTAL - 1L) * 2L);
    assertThat((Long) row.getProperty("totalActiveVertices")).isEqualTo(TOTAL - 1L);
    assertThat((Long) row.getProperty("totalAllocatedEdges")).isEqualTo(0L);
    assertThat((Long) row.getProperty("totalActiveEdges")).isEqualTo(0L);
    assertThat((Long) row.getProperty("totalDeletedRecords")).isEqualTo(1L);
    assertThat(((Collection) row.getProperty("corruptedRecords")).size()).isEqualTo(TOTAL - 1L);
    assertThat((Long) row.getProperty("missingReferenceBack")).isEqualTo(0L);
    assertThat((Long) row.getProperty("invalidLinks")).isEqualTo((TOTAL - 1) * 2L);
    assertThat(((Collection) row.getProperty("warnings")).size()).isEqualTo((TOTAL - 1) * 2L);

    result = database.command("sql", "check database");
    assertThat(result.hasNext()).isTrue();

    row = result.next();

    assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
    assertThat((Long) row.getProperty("autoFix")).isEqualTo(0);
    assertThat((Long) row.getProperty("totalActiveVertices")).isEqualTo(TOTAL - 1);
    assertThat((Long) row.getProperty("totalAllocatedEdges")).isEqualTo(0);
    assertThat((Long) row.getProperty("totalActiveEdges")).isEqualTo(0);
    assertThat((Long) row.getProperty("totalDeletedRecords")).isEqualTo(1L);
    assertThat(((Collection) row.getProperty("corruptedRecords")).size()).isEqualTo(0);
    assertThat((Long) row.getProperty("missingReferenceBack")).isEqualTo(0);
    assertThat((Long) row.getProperty("invalidLinks")).isEqualTo(0);
    assertThat(((Collection) row.getProperty("warnings")).size()).isEqualTo(0);
  }

  @Test
  void checkBrokenPage() {
    database.transaction(() -> {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(root.getIdentity().bucketId);

      try {
        final MutablePage page = ((DatabaseInternal) database).getTransaction()
            .getPageToModify(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize(), false);
        for (int i = 0; i < page.getAvailableContentSize(); i++) {
          page.writeByte(i, (byte) 4);
        }
      } catch (final IOException e) {
        Assertions.fail(e);
      }
    });

    ResultSet result = database.command("sql", "check database");
    assertThat(result.hasNext()).isTrue();

    Result row = result.next();
    assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
    assertThat((Long) row.getProperty("totalErrors") > 0L).isTrue();
    assertThat((Long) row.getProperty("autoFix")).isEqualTo(0L);
    assertThat((Long) row.getProperty("totalActiveVertices") < TOTAL).isTrue();
    assertThat((Long) row.getProperty("totalAllocatedEdges")).isEqualTo(TOTAL - 1);
    assertThat((Long) row.getProperty("totalActiveEdges")).isEqualTo(TOTAL - 1);
    assertThat((Long) row.getProperty("totalDeletedRecords")).isEqualTo(0L);
    assertThat((Long) row.getProperty("missingReferenceBack")).isEqualTo(0L);
    assertThat((Long) row.getProperty("invalidLinks") > 0L).isTrue();
    assertThat(((Collection) row.getProperty("warnings")).size() > 0L).isTrue();
    assertThat(((Collection<String>) row.getProperty("rebuiltIndexes")).size()).isEqualTo(1);
    assertThat(((Collection) row.getProperty("corruptedRecords")).size() > 0L).isTrue();

    result = database.command("sql", "check database fix");
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((Long) row.getProperty("autoFix") > 0L).isTrue();
    assertThat((Long) row.getProperty("totalActiveEdges")).isEqualTo(0L);
    assertThat(((Collection<String>) row.getProperty("rebuiltIndexes")).size()).isEqualTo(1);
    assertThat((Long) row.getProperty("totalActiveVertices") < TOTAL).isTrue();
    assertThat(((Collection) row.getProperty("corruptedRecords")).size() > 0L).isTrue();

    result = database.command("sql", "check database");
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((Long) row.getProperty("autoFix")).isEqualTo(0L);
    assertThat((Long) row.getProperty("totalErrors")).isEqualTo(0L);
    assertThat((Long) row.getProperty("autoFix")).isEqualTo(0L);
    assertThat((Long) row.getProperty("missingReferenceBack")).isEqualTo(0L);
    assertThat((Long) row.getProperty("invalidLinks")).isEqualTo(0L);
    assertThat(((Collection) row.getProperty("warnings")).size()).isEqualTo(0L);
    assertThat(((Collection<String>) row.getProperty("rebuiltIndexes")).size()).isEqualTo(0);
    assertThat(((Collection) row.getProperty("corruptedRecords")).size()).isEqualTo(0L);

    // CHECK CORRUPTED RECORD ARE NOT INDEXED ANYMORE
    final List<TypeIndex> indexes = database.getSchema().getType("Person").getIndexesByProperties("id");
    assertThat(indexes.size()).isEqualTo(1);

    assertThat(indexes.getFirst().countEntries()).isEqualTo((Long) row.getProperty("totalActiveVertices"));
  }

  @Override
  protected void beginTest() {
    database.command("sql", "create vertex type Person");
    database.command("sql", "create property Person.id string");
    database.command("sql", "create index on Person (id) unique");
    database.command("sql", "create edge type Knows");
    database.transaction(() -> {
      root = database.newVertex("Person").set("name", "root", "id", 0).save();
      for (int i = 1; i <= TOTAL - 1; i++) {
        final MutableVertex v = database.newVertex("Person").set("name", "test", "id", i).save();
        root.newEdge("Knows", v);
      }
    });
  }

  private int countEdges(final RID rootVertex) {
    final Iterable<Edge> iter = rootVertex.asVertex().getEdges(Vertex.DIRECTION.OUT);
    int totalEdges = 0;
    try {
      for (final Edge e : iter) {
        try {
          // FORCE THE LOADING
          e.has("");
          ++totalEdges;
        } catch (RecordNotFoundException Ve) {
          // IGNORE IT
        }
      }
    } catch (final NoSuchElementException e) {
      // EXPECTED FOR BROKEN EDGES WHEN THE FIRST ITEM (LAST IN THE ITERATOR) IS DELETED
    }

    return totalEdges;
  }

  private int countEdgesSegmentList(final RID rootVertex) {
    final EdgeLinkedList outEdges = ((DatabaseInternal) database).getGraphEngine()
        .getEdgeHeadChunk((VertexInternal) rootVertex.asVertex(), Vertex.DIRECTION.OUT);

    return (int) outEdges.count(null);
  }
}
