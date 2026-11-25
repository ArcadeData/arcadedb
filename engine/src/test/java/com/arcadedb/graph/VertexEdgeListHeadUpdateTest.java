/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The first edge of a direction sets a head pointer of the vertex record, a 12-byte field at a fixed place. The write is a head-only
 * update: it leaves the properties of the record (a 4 KB embedding among them) alone, writes only the bytes of the pointer to the
 * page, and the record reads back with every property, index entry and edge it had.
 */
class VertexEdgeListHeadUpdateTest extends TestHelper {
  private static final int DIMENSIONS = 1024;

  private float[] embedding(final int seed) {
    final Random random = new Random(seed);
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < vector.length; i++)
      vector[i] = random.nextFloat();
    return vector;
  }

  private void createSchema() {
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Entity", 1);
      type.createProperty("uuid", Type.STRING);
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      type.createProperty("name", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "uuid");
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
      database.getSchema().createEdgeType("LINKS");
    });
  }

  @Test
  void theFirstEdgeLeavesTheVertexPropertiesAndIndexesAsTheyWere() {
    createSchema();
    final int vertices = 60;
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++) {
        final MutableVertex vertex = database.newVertex("Entity");
        vertex.set("uuid", "u" + i);
        vertex.set("name", "n" + (i % 7));
        vertex.set("embedding", embedding(i));
        vertex.save();
        rids.add(vertex.getIdentity());
      }
    });

    // each vertex gets its first edge in each direction, in transactions of their own, and then a second one
    for (int round = 0; round < 2; round++) {
      final int offset = round + 1;
      database.transaction(() -> {
        for (int i = 0; i < vertices; i++)
          rids.get(i).asVertex().newEdge("LINKS", rids.get((i + offset) % vertices));
      });
    }

    reopenDatabase();

    for (int i = 0; i < vertices; i++) {
      final Vertex vertex = database.lookupByRID(rids.get(i), true).asVertex();
      assertThat(vertex.getString("uuid")).isEqualTo("u" + i);
      assertThat(vertex.getString("name")).isEqualTo("n" + (i % 7));
      assertThat((float[]) vertex.get("embedding")).isEqualTo(embedding(i));
      assertThat(vertex.countEdges(Vertex.DIRECTION.OUT, "LINKS")).isEqualTo(2);
      assertThat(vertex.countEdges(Vertex.DIRECTION.IN, "LINKS")).isEqualTo(2);
      assertThat(database.lookupByKey("Entity", "uuid", "u" + i).next().getIdentity()).isEqualTo(rids.get(i));
    }
    int named = 0;
    for (final var cursor = database.lookupByKey("Entity", "name", "n3"); cursor.hasNext(); cursor.next())
      ++named;
    assertThat(named).isEqualTo((vertices + 3) / 7);
  }

  @Test
  void aHeadOnlyUpdateWritesTheBytesOfTheHeadsAndNoMore() throws ReflectiveOperationException {
    createSchema();
    final RID rid;
    database.begin();
    final MutableVertex created = database.newVertex("Entity");
    created.set("uuid", "u1");
    created.set("name", "n1");
    created.set("embedding", embedding(1));
    created.save();
    rid = created.getIdentity();
    database.commit();

    database.begin();
    final MutableVertex vertex = rid.asVertex().modify();
    vertex.setOutEdgesHeadChunk(new RID(9, 123L));
    vertex.setInEdgesHeadChunk(new RID(9, 456L));
    ((DatabaseInternal) database).updateRecordNoLock(vertex, false);

    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    // TransactionContext offers no accessor for the pages it holds: read the field as StatementSavepointPageBytesTest does
    final Field field = TransactionContext.class.getDeclaredField("modifiedPages");
    field.setAccessible(true);
    @SuppressWarnings("unchecked") final Map<PageId, MutablePage> pages = (Map<PageId, MutablePage>) field.get(tx);
    int written = 0;
    for (final MutablePage page : pages.values()) {
      final int[] ranges = page.getModifiedRanges();
      for (int r = 0; r < page.getModifiedRangeCount(); r++)
        written += ranges[r * 2 + 1] - ranges[r * 2] + 1;
    }
    database.commit();

    // The two heads are 12 bytes each (a bucket id int and a position long) at fixed places of the record, 24 bytes at most, and the
    // page's modified ranges are merged when close, so a few bytes of slack stay well below the 4 KB of the embedding: the whole
    // record rewritten (what this replaces) marks more than 4,096 bytes
    assertThat(written).as("bytes of the page marked modified by a head-only update of a record of 4 KB").isBetween(1, 64);

    final Vertex reloaded = rid.asVertex();
    assertThat(reloaded.getString("uuid")).isEqualTo("u1");
    assertThat((float[]) reloaded.get("embedding")).isEqualTo(embedding(1));
    assertThat(((VertexInternal) reloaded).peekOutEdgesHeadChunk()).isEqualTo(new RID(9, 123L));
    assertThat(((VertexInternal) reloaded).peekInEdgesHeadChunk()).isEqualTo(new RID(9, 456L));

    // the heads were made up for the test: take them back, so the integrity check that ends it finds a vertex with no edge list
    database.begin();
    final MutableVertex restored = rid.asVertex().modify();
    restored.setOutEdgesHeadChunk(null);
    restored.setInEdgesHeadChunk(null);
    ((DatabaseInternal) database).updateRecordNoLock(restored, false);
    database.commit();
    assertThat(((VertexInternal) rid.asVertex()).peekOutEdgesHeadChunk()).isNull();
  }

  @Test
  void aHeadOnlyUpdateDoesNotDecodeTheProperties() {
    createSchema();
    final RID rid;
    database.begin();
    final MutableVertex created = database.newVertex("Entity");
    created.set("uuid", "u1");
    created.set("name", "n1");
    created.set("embedding", embedding(1));
    created.save();
    rid = created.getIdentity();
    database.commit();

    database.begin();
    final MutableVertex vertex = rid.asVertex().modify();
    vertex.setOutEdgesHeadChunk(new RID(9, 123L));
    // the validation, the index diff and the serialization of the update used to decode the 1,024 floats and the strings
    ((DatabaseInternal) database).updateRecordWithoutEvents(vertex);
    assertThat(vertex.arePropertiesLoaded()).as("properties decoded by a head-only update").isFalse();

    vertex.setOutEdgesHeadChunk(null);
    ((DatabaseInternal) database).updateRecordWithoutEvents(vertex);
    database.commit();
    assertThat(rid.asVertex().getString("uuid")).isEqualTo("u1");
  }

  /**
   * Two copies of one vertex taken before either is saved: the first renames it, the second only takes the first edge. The second
   * replaces the first in the transaction, so the commit writes the stored name: the index has to follow it, not keep the rename.
   */
  @Test
  void aHeadOnlyWriteThatReplacesAQueuedCopyKeepsTheIndexInStepWithTheRecord() {
    createSchema();
    final RID[] holder = createTwoVertices();

    database.transaction(() -> {
      final MutableVertex renamed = holder[0].asVertex().modify();
      final MutableVertex linked = holder[0].asVertex().modify();
      assertThat(linked).isNotSameAs(renamed);
      renamed.set("name", "renamed").save();
      // the head write of the graph engine, on the second copy (newEdge() would take the copy the transaction holds)
      linked.setOutEdgesHeadChunk(new RID(9, 123L));
      assertThat(linked.arePropertiesLoaded()).isFalse();
      ((DatabaseInternal) database).updateRecordWithoutEvents(linked);
    });

    assertIndexAgreesWithRecord(holder[0]);
    assertThat(((VertexInternal) holder[0].asVertex()).peekOutEdgesHeadChunk()).isEqualTo(new RID(9, 123L));

    // the head was made up for the test: take it back, so the integrity check that ends it finds a vertex with no edge list
    database.transaction(() -> {
      final MutableVertex restored = holder[0].asVertex().modify();
      restored.setOutEdgesHeadChunk(null);
      ((DatabaseInternal) database).updateRecordWithoutEvents(restored);
    });
  }

  /**
   * A reload gives a queued vertex back the stored content while the index already holds the rename an earlier save brought it to.
   * The first edge taken after it is no head-only write: the index has to be brought back to what the commit writes.
   */
  @Test
  void aHeadOnlyWriteAfterAReloadKeepsTheIndexInStepWithTheRecord() {
    createSchema();
    final RID[] holder = createTwoVertices();

    database.transaction(() -> {
      final MutableVertex vertex = holder[0].asVertex().modify();
      vertex.set("name", "renamed").save();
      vertex.reload();
      assertThat(vertex.arePropertiesLoaded()).isFalse();
      vertex.newEdge("LINKS", holder[1]);
    });

    assertIndexAgreesWithRecord(holder[0]);
    assertThat(holder[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "LINKS")).isEqualTo(1);
  }

  private RID[] createTwoVertices() {
    final RID[] holder = new RID[2];
    database.transaction(() -> {
      holder[0] = database.newVertex("Entity").set("uuid", "u1").set("name", "original").save().getIdentity();
      holder[1] = database.newVertex("Entity").set("uuid", "u2").set("name", "other").save().getIdentity();
    });
    return holder;
  }

  private void assertIndexAgreesWithRecord(final RID rid) {
    final String stored = rid.asVertex().getString("name");
    for (final String name : new String[] { "original", "renamed" }) {
      final List<RID> found = new ArrayList<>();
      for (final var cursor = database.lookupByKey("Entity", "name", name); cursor.hasNext(); )
        found.add(cursor.next().getIdentity());
      if (name.equals(stored))
        assertThat(found).as("index entries for the stored name '%s'", name).containsExactly(rid);
      else
        assertThat(found).as("index entries for '%s', which the record does not hold (it holds '%s')", name, stored).isEmpty();
    }
  }

  @Test
  void aVertexWhoseStoredPropertyBreaksALaterConstraintStillTakesItsFirstEdge() {
    createSchema();
    final RID[] holder = new RID[2];
    database.transaction(() -> {
      final MutableVertex first = database.newVertex("Entity").set("uuid", "u1").set("name", "x").save();
      final MutableVertex second = database.newVertex("Entity").set("uuid", "u2").set("name", "yy").save();
      holder[0] = first.getIdentity();
      holder[1] = second.getIdentity();
    });
    // a constraint added after the data: the stored value of the first vertex no longer satisfies it
    database.transaction(() -> database.getSchema().getType("Entity").getProperty("name").setMin("5"));

    // intended behaviour of a head-only write: the properties are the stored ones, so they are not validated again, and the vertex takes
    // its edges
    database.transaction(() -> holder[0].asVertex().newEdge("LINKS", holder[1]));
    assertThat(holder[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "LINKS")).isEqualTo(1);
    assertThat(holder[1].asVertex().countEdges(Vertex.DIRECTION.IN, "LINKS")).isEqualTo(1);

    // writing the property itself is still validated
    assertThatThrownBy(() -> database.transaction(() -> holder[0].asVertex().modify().set("name", "z").save()))
        .isInstanceOf(ValidationException.class);
  }
}
