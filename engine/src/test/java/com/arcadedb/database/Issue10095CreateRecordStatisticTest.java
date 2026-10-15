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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10095: the {@code createRecord} statistic was incremented where a mutable record is HANDED OUT
 * ({@code newDocument()}, {@code newVertex()}, {@code newEdgeByKeys()}) instead of where a record is CREATED, so a
 * record instantiated and then discarded was counted and every edge made through {@code Vertex.newEdge()} was not. The
 * counter now follows the rule {@code deleteRecord} (#9837) and {@code updateRecord} (#9987) settled: it is incremented
 * in the single method that owns the operation's contract, after the write actually happened.
 * <p>
 * The guarantee is the family, not one member: documents, vertices and edges (by the Java API and by SQL), through the
 * synchronous and the asynchronous path, and the inputs where "instantiated" and "created" differ - a discarded instance,
 * a validation failure, a vetoed create - plus a vertex with enough edges to allocate edge segments, which are records
 * of the engine and not records of the user.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10095CreateRecordStatisticTest extends TestHelper {

  @Override
  public void beginTest() {
    database.getSchema().createDocumentType("D1");
    database.getSchema().createVertexType("V1");
    database.getSchema().createEdgeType("E1");
  }

  @Test
  void aSavedDocumentIsCountedOnce() {
    final long creates = createCount();
    database.transaction(() -> database.newDocument("D1").set("a", 1).save());
    assertThat(createCount()).isEqualTo(creates + 1);
  }

  @Test
  void aDiscardedInstanceIsNotCounted() {
    final long creates = createCount();

    database.newDocument("D1").set("a", 1);
    database.newVertex("V1").set("a", 1);

    assertThat(createCount()).as("an instance that was never saved is not a created record").isEqualTo(creates);
  }

  @Test
  void aRecordRefusedByValidationIsNotCounted() {
    database.getSchema().getType("D1").createProperty("name", Type.STRING).setMandatory(true);

    final long creates = createCount();
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("D1").save())).isInstanceOf(
        ValidationException.class);

    assertThat(createCount()).isEqualTo(creates);

    database.transaction(() -> database.newDocument("D1").set("name", "ok").save());
    assertThat(createCount()).isEqualTo(creates + 1);
  }

  @Test
  void aCreateVetoedByAListenerIsNotCounted() {
    final BeforeRecordCreateListener veto = record -> false;
    database.getSchema().getType("D1").getEvents().registerListener(veto);
    try {
      final long creates = createCount();
      database.transaction(() -> assertThat(database.newDocument("D1").set("a", 1).saveReportingVeto()).isFalse());
      assertThat(createCount()).isEqualTo(creates);
    } finally {
      database.getSchema().getType("D1").getEvents().unregisterListener(veto);
    }
  }

  @Test
  void anEdgeCreatedThroughNewEdgeIsCounted() {
    final MutableVertex[] v = new MutableVertex[2];
    final long creates = createCount();

    database.transaction(() -> {
      v[0] = database.newVertex("V1").save();
      v[1] = database.newVertex("V1").save();
    });
    assertThat(createCount()).isEqualTo(creates + 2);

    database.transaction(() -> v[0].newEdge("E1", v[1]));
    assertThat(createCount()).as("the edge is a record created").isEqualTo(creates + 3);
  }

  @Test
  void aLightweightEdgeHasNoRecordSoItIsNotCounted() {
    database.getSchema().buildEdgeType().withName("Light").withLightweight(true).create();
    final MutableVertex[] v = new MutableVertex[2];
    database.transaction(() -> {
      v[0] = database.newVertex("V1").save();
      v[1] = database.newVertex("V1").save();
    });

    final long creates = createCount();
    database.transaction(() -> v[0].newEdge("Light", v[1]));
    assertThat(createCount()).as("a lightweight edge has no record, so there is nothing to count").isEqualTo(creates);
  }

  @Test
  void sqlInsertAndSqlCreateEdgeAreCounted() {
    final long creates = createCount();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO V1 SET k = 1");
      database.command("sql", "INSERT INTO V1 SET k = 2");
    });
    assertThat(createCount()).isEqualTo(creates + 2);

    database.transaction(() -> database.command("sql", "CREATE EDGE E1 FROM (SELECT FROM V1 WHERE k = 1) TO (SELECT FROM V1 WHERE k = 2)"));
    assertThat(createCount()).as("CREATE EDGE creates one record").isEqualTo(creates + 3);
  }

  @Test
  void newEdgeByKeysCountsTheVertexAndTheEdgeOnceEach() {
    database.getSchema().getType("V1").createProperty("id", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V1", "id");

    database.transaction(() -> {
      database.newVertex("V1").set("id", 1).save();
      database.newVertex("V1").set("id", 2).save();
    });

    final long creates = createCount();
    database.transaction(() -> database.newEdgeByKeys("V1", new String[] { "id" }, new Object[] { 1 }, "V1",
        new String[] { "id" }, new Object[] { 2 }, false, "E1", true));

    assertThat(createCount()).as("one edge, created once").isEqualTo(creates + 1);
  }

  @Test
  void edgeSegmentsAreNotCounted() {
    final int edges = 3_000;
    final MutableVertex[] hub = new MutableVertex[1];
    final long creates = createCount();

    database.transaction(() -> hub[0] = database.newVertex("V1").save());
    database.transaction(() -> {
      for (int i = 0; i < edges; i++) {
        final MutableVertex leaf = database.newVertex("V1").save();
        hub[0].newEdge("E1", leaf);
      }
    });

    assertThat(createCount()).as("1 hub + %d leaves + %d edges, and not one record more for the edge segments", edges, edges)
        .isEqualTo(creates + 1 + 2L * edges);
  }

  @Test
  void anAsynchronousCreateIsCountedOnceWhateverBuiltTheRecord() {
    final long creates = createCount();
    final AtomicInteger created = new AtomicInteger();

    database.async().createRecord(database.newDocument("D1").set("a", 1), r -> created.incrementAndGet());
    database.async().waitCompletion();

    assertThat(created.get()).isEqualTo(1);
    assertThat(createCount()).isEqualTo(creates + 1);
  }

  @Test
  void aRestoredRecordIsCounted() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("D1").set("a", 1).save().getIdentity());
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid[0]));

    final long creates = createCount();
    database.transaction(() -> database.command("sql", "RESTORE DOCUMENT D1 RID " + rid[0] + " SET a = 2"));

    assertThat(createCount()).isEqualTo(creates + 1);
  }

  @Test
  void createUpdateAndDeleteEachAdvanceOnlyTheirOwnStatistic() {
    final long creates = createCount();
    final long updates = stat("updateRecord");
    final long deletes = stat("deleteRecord");

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("D1").set("a", 0).save().getIdentity());
    database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("a", 1).save());
    database.transaction(() -> database.deleteRecord(database.lookupByRID(rid[0], true)));

    assertThat(createCount()).isEqualTo(creates + 1);
    assertThat(stat("updateRecord")).isEqualTo(updates + 1);
    assertThat(stat("deleteRecord")).isEqualTo(deletes + 1);
  }

  private long createCount() {
    return stat("createRecord");
  }

  private long stat(final String name) {
    return ((Number) database.getStats().get(name)).longValue();
  }
}
