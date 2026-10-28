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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A COLUMNAR index is maintained asynchronously by its projection: no transaction stages an entry for it, and no planner or key
 * lookup reads it.
 */
class ColumnarIndexOptOutTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE PROPERTY Asset.author STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    ColumnarTestHooks.INDEX_WRITES.set(0);
  }

  @Test
  void plannerNeverSelectsIt() {
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newDocument("Asset").set("brand", i % 2 == 0 ? "x" : "y").save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM Asset WHERE brand = 'x'")) {
      final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
      final String printed = plan.prettyPrint(0, 2);
      assertThat(printed).doesNotContain("FETCH FROM INDEX").contains("FETCH FROM TYPE");
      int count = 0;
      while (rs.hasNext()) {
        assertThat(rs.next().<String>getProperty("brand")).isEqualTo("x");
        count++;
      }
      assertThat(count).isEqualTo(10);
    }

    // the same query against a NOTUNIQUE index does use it, so the assertion above can fail
    database.command("sql", "CREATE INDEX ON Asset (author) NOTUNIQUE");
    try (final ResultSet rs = database.query("sql", "SELECT FROM Asset WHERE author = 'x'")) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("FETCH FROM INDEX");
    }
  }

  @Test
  void typeIndexIsNotReadyForQueries() {
    final List<TypeIndex> indexes = database.getSchema().getType("Asset").getIndexesByProperties("brand");
    assertThat(indexes).hasSize(1);
    assertThat(indexes.getFirst().getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
    assertThat(indexes.getFirst().isReadyForQueries()).isFalse();

    assertThatThrownBy(() -> database.lookupByKey("Asset", new String[] { "brand" }, new Object[] { "x" }))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void commitDoesNoIndexWork() {
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        rids.add(database.newDocument("Asset").set("brand", "b" + (i % 17)).save().getIdentity());
      assertThat(stagedIndexEntries()).isZero();
    });

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableDocument doc = rids.get(i).asDocument().modify();
        doc.set("brand", "changed" + i).save();
        // no key was extracted: the update left no indexed-state snapshot to diff the next update against
        assertThat(((DatabaseInternal) database).getTransaction().getLastIndexedSnapshot(rids.get(i))).isNull();
      }
      for (int i = 100; i < 200; i++)
        rids.get(i).asDocument().delete();
      assertThat(stagedIndexEntries()).isZero();
    });

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Asset[brand]_columnar");
    assertThat(index.getSubIndexes().getFirst().countEntries()).isZero();
    assertThat(ColumnarTestHooks.INDEX_WRITES.get()).as("no write reached the COLUMNAR index").isZero();

    // control: the same writes against a key index stage entries, so the zero above is not a blind spot of the measure
    database.command("sql", "CREATE INDEX ON Asset (author) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Asset").set("brand", "b", "author", "a" + i).save();
      assertThat(stagedIndexEntries()).isEqualTo(10);
    });
  }

  @Test
  void graphBatchEdgesDoNoIndexWork() {
    database.command("sql", "CREATE VERTEX TYPE Node");
    database.command("sql", "CREATE EDGE TYPE Link");
    database.command("sql", "CREATE PROPERTY Link.kind STRING");
    database.command("sql", "CREATE INDEX ON Link (kind) COLUMNAR");

    final RID[] vertices = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").save();
      final MutableVertex b = database.newVertex("Node").save();
      vertices[0] = a.getIdentity();
      vertices[1] = b.getIdentity();
    });

    try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(false).build()) {
      for (int i = 0; i < 50; i++)
        batch.newEdge(vertices[0], "Link", vertices[1], "kind", "k" + (i % 3));
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM Link")) {
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(50L);
    }
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Link[kind]_columnar");
    assertThat(index.getSubIndexes()).allSatisfy(sub -> assertThat(sub.countEntries()).isZero());
    assertThat(ColumnarTestHooks.INDEX_WRITES.get()).as("the graph batch skipped the COLUMNAR index").isZero();
  }

  @Test
  void buildDoesNotShareCallerTransaction() {
    assertThat(Schema.INDEX_TYPE.COLUMNAR.buildCanShareCallerTransaction()).isFalse();
    assertThat(Schema.INDEX_TYPE.COLUMNAR.isMaintainedInTransaction()).isFalse();
    assertThat(Schema.INDEX_TYPE.COLUMNAR.isPlannable()).isFalse();
    assertThat(Schema.INDEX_TYPE.COLUMNAR.isExactKeyLookup()).isFalse();
    for (final Schema.INDEX_TYPE type : Schema.INDEX_TYPE.values())
      if (type != Schema.INDEX_TYPE.COLUMNAR) {
        assertThat(type.buildCanShareCallerTransaction()).as(type.name()).isTrue();
        assertThat(type.isMaintainedInTransaction()).as(type.name()).isTrue();
        assertThat(type.isPlannable()).as(type.name()).isTrue();
      }
  }

  @Test
  void createInsideAWritingTransactionBuildsInItsOwn() {
    // a caller transaction with pending writes to the indexed bucket: a COLUMNAR build never shares it, and the pending record
    // is neither lost nor staged as an index entry
    database.transaction(() -> {
      database.newDocument("Asset").set("author", "pending").save();
      database.command("sql", "CREATE INDEX ON Asset (author) COLUMNAR");
      assertThat(stagedIndexEntries()).isZero();
    });
    assertThat(database.getSchema().existsIndex("Asset[author]_columnar")).isTrue();
    assertThat(ColumnarTestHooks.INDEX_WRITES.get()).isZero();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM Asset WHERE author = 'pending'")) {
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
    }
  }

  @Test
  void theWriteCounterCountsADirectWrite() {
    // the control of the zero assertions above: a write that does reach the index is counted
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Asset[brand]_columnar");
    database.transaction(() -> index.getIndexesOnBuckets()[0].put(new Object[] { "x" }, new RID[] { new RID(1, 0) }));
    assertThat(ColumnarTestHooks.INDEX_WRITES.get()).isEqualTo(1);
  }

  private int stagedIndexEntries() {
    return ((DatabaseInternal) database).getTransaction().getIndexChanges().getTotalEntries();
  }
}
