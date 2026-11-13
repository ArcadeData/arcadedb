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

import com.arcadedb.TestHelper;
import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DocumentInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression tests of the GraphBatchEdge area, one nested class per former stand-alone regression class.
 */
class GraphBatchEdgeTest {
  /**
   * Regression for issue #7448 (discussion #7439): every edge written by {@link GraphBatch} without properties was reported
   * as corrupted the first time its properties were read. The bulk writer stored the header end offset one byte short,
   * pointing at the property count instead of past it, and the property-count validation added for #5774 rejects that.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchEmptyEdgeHeader extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Work7448");
        database.getSchema().createEdgeType("Cite7448");
      });
    }

    @Test
    void propertyLessEdgeFromGraphBatchReadsBackClean() {
      final RID[] vertices = new RID[2];
      database.transaction(() -> {
        for (int i = 0; i < vertices.length; i++)
          vertices[i] = database.newVertex("Work7448").set("id", i).save().getIdentity();
      });

      try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(false).build()) {
        batch.newEdge(vertices[0], "Cite7448", vertices[1]);
      }

      final List<String> reported = WarningCapture.captureWarnings(() -> database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "select from Cite7448")) {
          assertThat(rs.hasNext()).isTrue();
          final Result row = rs.next();
          final Edge edge = row.toElement().asEdge();

          assertThat(edge.getOut()).isEqualTo(vertices[0]);
          assertThat(edge.getIn()).isEqualTo(vertices[1]);
          assertThat(edge.getPropertyNames()).isEmpty();
          assertThat(edge.toMap(false)).isEmpty();
          assertThat(edge.toJSON().has("@rid")).isTrue();
          assertThat(row.getPropertyNames()).isEmpty();
          assertThat(rs.hasNext()).isFalse();

          // The accessors above materialised the record: its header end offset must point right past the property
          // count, where the values section starts, the same layout BinarySerializer.serializeProperties() writes
          final Binary buffer = ((BaseRecord) edge).getBuffer();
          buffer.position(((DocumentInternal) edge).getPropertiesStartingPosition());
          final int headerEndOffset = buffer.getInt();
          assertThat(buffer.getUnsignedNumber()).isZero();
          assertThat(headerEndOffset).as("header end offset must follow the property count").isEqualTo(buffer.position());
        }
      }));

      assertThat(reported.stream().filter(m -> m.contains("Possible corrupted record")).toList())
          .as("a property-less batch edge must not be reported as corrupted (captured=%s)", reported)
          .isEmpty();
    }
  }

  /**
   * Regression for issue #9018: {@link GraphBatch#newEdge} given {@code null} for a property the edge type declares wrote the
   * declared type tag and no value bytes, so the reader took the following bytes as the value or ran past the record.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchNullEdgeProperty extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("N9018");
        final EdgeType e = database.getSchema().createEdgeType("E9018");
        e.createProperty("weight", Integer.class);
        e.createProperty("since", Long.class);
        e.createProperty("label", String.class);
      });
    }

    @Test
    void nullDeclaredPropertyReadsBackNull() {
      final RID[] v = new RID[2];
      database.transaction(() -> {
        for (int i = 0; i < v.length; i++)
          v[i] = database.newVertex("N9018").set("id", i).save().getIdentity();
      });

      try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(false).build()) {
        batch.newEdge(v[0], "E9018", v[1], "weight", null, "note", "hello");
        batch.newEdge(v[0], "E9018", v[1], "since", null, "weight", 7);
        batch.newEdge(v[0], "E9018", v[1], "weight", 7, "label", null);
      }

      final List<String> reported = WarningCapture.captureWarnings(() -> database.transaction(() -> {
        int checked = 0;
        for (final Edge edge : database.lookupByRID(v[0], true).asVertex().getEdges(Vertex.DIRECTION.OUT, "E9018")) {
          if (edge.has("note")) {
            assertThat(edge.get("weight")).isNull();
            assertThat(edge.getString("note")).isEqualTo("hello");
          } else if (edge.has("since")) {
            assertThat(edge.get("since")).isNull();
            assertThat(edge.getInteger("weight")).isEqualTo(7);
          } else {
            assertThat(edge.getInteger("weight")).isEqualTo(7);
            assertThat(edge.get("label")).isNull();
          }
          ++checked;
        }
        assertThat(checked).isEqualTo(3);
      }));
      assertThat(reported).isEmpty();
    }
  }

  /**
   * Regression for #9019 (the bulk edge path skipped the declared property's conversion, constraints and defaults) and
   * #9020 (a {@link Map} holding the properties was dropped by the bulk edge path).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchEdgeSchema extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("N9019");
        database.getSchema().createEdgeType("F9019");
        database.getSchema().createEdgeType("E9020");
      });
    }

    private RID[] vertices() {
      final RID[] v = new RID[2];
      database.transaction(() -> {
        for (int i = 0; i < v.length; i++)
          v[i] = database.newVertex("N9019").save().getIdentity();
      });
      return v;
    }

    private void declare(final String type, final String declaration) {
      database.command("sql", "CREATE EDGE TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".p " + declaration);
    }

    private Edge edgeOf(final RID from, final String type) {
      return database.lookupByRID(from, true).asVertex().getEdges(Vertex.DIRECTION.OUT, type).iterator().next();
    }

    private void batchEdge(final RID[] v, final String type, final boolean mixed, final Object... props) {
      try (final GraphBatch b = database.batch().build()) {
        b.newEdge(v[0], type, v[1], props);
        if (mixed)
          b.newEdge(v[0], "F9019", v[1]);
      }
    }

    @Test
    void declaredPropertyIsConvertedAndDefaulted() {
      declare("E_INT9019", "INTEGER");
      declare("E_BYTE9019", "BYTE");
      declare("E_DFLT9019", "STRING (default 'dflt')");
      declare("E_MICROS9019", "DATETIME_MICROS");

      for (final boolean mixed : new boolean[] { false, true }) {
        RID[] v = vertices();
        batchEdge(v, "E_BYTE9019", mixed, "p", 12);
        assertThat(edgeOf(v[0], "E_BYTE9019").get("p")).isEqualTo((byte) 12);

        v = vertices();
        batchEdge(v, "E_INT9019", mixed, "p", "42");
        assertThat(edgeOf(v[0], "E_INT9019").get("p")).isEqualTo(42);

        v = vertices();
        batchEdge(v, "E_DFLT9019", mixed, "other", 1);
        assertThat(edgeOf(v[0], "E_DFLT9019").get("p")).isEqualTo("dflt");

        // the same value must come back as Vertex.newEdge() stores it
        final RID[] api = vertices();
        database.transaction(() -> api[0].asVertex().modify().newEdge("E_MICROS9019", api[1], "p", 1791000000000L));
        v = vertices();
        batchEdge(v, "E_MICROS9019", mixed, "p", 1791000000000L);
        assertThat(edgeOf(v[0], "E_MICROS9019").get("p")).isEqualTo(edgeOf(api[0], "E_MICROS9019").get("p"));
      }
    }

    @Test
    void valuesOutOfRangeAndBrokenConstraintsAreRefused() {
      declare("E_SHORT9019", "SHORT");
      declare("E_LONGINT9019", "INTEGER");
      declare("E_MANDATORY9019", "STRING (mandatory true)");
      declare("E_MIN9019", "INTEGER (min 10)");
      declare("E_REGEXP9019", "STRING (regexp '[a-z]+')");

      for (final boolean mixed : new boolean[] { false, true }) {
        assertRefused("E_SHORT9019", mixed, "p", 40000);
        assertRefused("E_LONGINT9019", mixed, "p", 3000000000L);
        assertRefused("E_MANDATORY9019", mixed, "other", 1);
        assertRefused("E_MIN9019", mixed, "p", 5);
        assertRefused("E_REGEXP9019", mixed, "p", "ABC");
      }
    }

    private void assertRefused(final String type, final boolean mixed, final Object... props) {
      final RID[] v = vertices();
      assertThatThrownBy(() -> batchEdge(v, type, mixed, props)).as(type + " mixed=" + mixed).isInstanceOf(RuntimeException.class);
      if (database.isTransactionActive())
        database.rollback();
      assertThat(database.lookupByRID(v[0], true).asVertex().getEdges(Vertex.DIRECTION.OUT, type).iterator().hasNext()).as(type).isFalse();
    }

    @Test
    void mapOfPropertiesIsStoredWhateverElseTheBatchHolds() {
      final Map<String, Object> props = new LinkedHashMap<>();
      props.put("weight", 5);
      props.put("label", "x");

      for (final boolean mixed : new boolean[] { false, true }) {
        final RID[] v = vertices();
        try (final GraphBatch b = database.batch().build()) {
          b.newEdge(v[0], "E9020", v[1], props);
          if (mixed)
            b.newEdge(v[0], "F9019", v[1]);
        }
        final Edge edge = edgeOf(v[0], "E9020");
        assertThat(edge.get("weight")).isEqualTo(5);
        assertThat(edge.get("label")).isEqualTo("x");
      }
    }

    @Test
    void legacyLightEdgeOverrideStillAppliesDefaultsAndMandatory() {
      declare("E_LDFLT9019", "STRING (default 'dflt')");
      declare("E_LMAND9019", "STRING (mandatory true)");

      final RID[] v = vertices();
      try (final GraphBatch b = database.batch().withLightEdges(true).build()) {
        b.newEdge(v[0], "E_LDFLT9019", v[1]);
      }
      assertThat(edgeOf(v[0], "E_LDFLT9019").get("p")).isEqualTo("dflt");

      final RID[] w = vertices();
      assertThatThrownBy(() -> {
        try (final GraphBatch b = database.batch().withLightEdges(true).build()) {
          b.newEdge(w[0], "E_LMAND9019", w[1]);
        }
      }).isInstanceOf(RuntimeException.class);
    }

    @Test
    void malformedPropertyArgumentsAreRefused() {
      final RID[] v = vertices();
      try (final GraphBatch b = database.batch().build()) {
        assertThatThrownBy(() -> b.newEdge(v[0], "E9020", v[1], "odd")).isInstanceOf(IllegalArgumentException.class);
        final Map<String, Object> nullKey = new LinkedHashMap<>();
        nullKey.put(null, 1);
        assertThatThrownBy(() -> b.newEdge(v[0], "E9020", v[1], nullKey)).isInstanceOf(IllegalArgumentException.class);
      }
    }

    @Test
    void complexDeclaredPropertiesAreStoredLikeVertexNewEdge() {
      database.command("sql", "CREATE EDGE TYPE E_CPLX9019");
      database.command("sql", "CREATE PROPERTY E_CPLX9019.l LIST");
      database.command("sql", "CREATE PROPERTY E_CPLX9019.m MAP");
      database.command("sql", "CREATE PROPERTY E_CPLX9019.d DATE");

      final java.util.List<Object> list = java.util.List.of(1, "two");
      final Map<String, Object> map = Map.of("a", 1);
      final java.util.Date date = new java.util.Date(1791000000000L);
      final RID[] api = vertices();
      database.transaction(() -> api[0].asVertex().modify().newEdge("E_CPLX9019", api[1], "l", list, "m", map, "d", date));
      final RID[] v = vertices();
      batchEdge(v, "E_CPLX9019", false, "l", list, "m", map, "d", date);

      final Edge expected = edgeOf(api[0], "E_CPLX9019");
      final Edge actual = edgeOf(v[0], "E_CPLX9019");
      assertThat(actual.get("l")).isEqualTo(expected.get("l"));
      assertThat(actual.get("m")).isEqualTo(expected.get("m"));
      assertThat(actual.get("d")).isEqualTo(expected.get("d"));
    }
  }

  /**
   * Regression tests for #9039 (a rolled back {@code createVertex} left stale edge-segment cache entries) and #9040 (a failed
   * {@code createVertices} left its transaction open).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchFailedTransaction extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.id INTEGER");
      database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE Note");
      database.command("sql", "CREATE EDGE TYPE Link");
    }

    @Test
    void createVertexAfterRolledBackCommitStaysUsable() {
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        final int[] ids = { 1, 1, 2, 3 };
        for (final int id : ids) {
          try {
            database.begin();
            batch.createVertex("Person", "id", id);
            database.commit();
          } catch (final DuplicatedKeyException e) {
            if (database.isTransactionActive())
              database.rollback();
          }
        }
      }
      assertThat(database.countType("Person", true)).isEqualTo(3);
    }

    @Test
    void createVerticesAfterRolledBackCreateVertexThenEdgeStaysUsable() {
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        for (final int id : new int[] { 1, 1 }) {
          try {
            database.begin();
            batch.createVertex("Person", "id", id);
            database.commit();
          } catch (final DuplicatedKeyException e) {
            if (database.isTransactionActive())
              database.rollback();
          }
        }
        // THE POSITION OF THE ROLLED BACK VERTEX IS REUSED BY THE BULK PATH
        final RID[] rids = batch.createVertices("Person", new Object[][] { { "id", 2 }, { "id", 3 } });
        batch.newEdge(rids[0], "Link", rids[1]);
      }
      assertThat(database.countType("Person", true)).isEqualTo(3);
      assertThat(database.countType("Link", true)).isEqualTo(1);
    }

    @Test
    void createVertexAfterRolledBackCommitStaysUsableUnidirectional() {
      try (final GraphBatch batch = GraphBatch.builder(database).withBidirectional(false).build()) {
        final int[] ids = { 1, 1, 2 };
        for (final int id : ids) {
          try {
            database.begin();
            batch.createVertex("Person", "id", id);
            database.commit();
          } catch (final DuplicatedKeyException e) {
            if (database.isTransactionActive())
              database.rollback();
          }
        }
      }
      assertThat(database.countType("Person", true)).isEqualTo(2);
    }

    @Test
    void failedCreateVerticesLeavesNoTransactionOpen() {
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        assertThatThrownBy(() -> batch.createVertices("Person", new Object[][] { { "id", 2 }, { "id", 2 } }))
            .isInstanceOf(DuplicatedKeyException.class);
        assertThat(database.isTransactionActive()).isFalse();

        assertThatThrownBy(() -> batch.createVertices("NoSuchType", 3)).isInstanceOf(SchemaException.class);
        assertThat(database.isTransactionActive()).isFalse();

        // THE BATCH IS STILL USABLE
        final RID[] rids = batch.createVertices("Person", new Object[][] { { "id", 5 }, { "id", 6 } });
        assertThat(rids).hasSize(2);
      }
      assertThat(database.isTransactionActive()).isFalse();
      assertThat(database.countType("Person", true)).isEqualTo(2);
    }

    @Test
    void failedCreateVerticesLeavesACallerOwnedTransactionToTheCaller() {
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        database.begin();
        assertThatThrownBy(() -> batch.createVertices("Person", new Object[][] { { "id", 2 }, { "id", 2 } }))
            .isInstanceOf(IllegalStateException.class);
        // THE TRANSACTION WAS OPENED BY THE CALLER, WHO RESOLVES IT: since #9242 the batch refuses it up front, untouched
        assertThat(database.isTransactionActive()).isTrue();
        database.rollback();
      }
      assertThat(database.countType("Person", true)).isEqualTo(0);
    }
  }

  /**
   * Regression tests for issue #9242: {@link GraphBatch} committed (or, on a retry, rolled back) a transaction the
   * caller had opened, and applied its own WAL policy to the caller's writes. The batch manages its own transactions, so
   * it now refuses to run inside one, and its WAL policy never reaches a transaction that is not its own.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchCallerTransaction extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Note");
        database.getSchema().createVertexType("V");
        database.getSchema().createEdgeType("E");
      });
    }

    private void saveNote(final String tag) {
      final MutableDocument note = database.newDocument("Note");
      note.set("tag", tag);
      note.set("payload", "x".repeat(4000));
      note.save();
    }

    private long notes() {
      return database.countType("Note", true);
    }

    private long walBytes() {
      return ((Number) ((DatabaseInternal) database).getTransactionManager().getStats().get("bytesWritten")).longValue();
    }

    @Test
    void createVerticesRefusedInsideCallerTransaction() {
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        database.begin();
        saveNote("A");

        assertThatThrownBy(() -> batch.createVertices("V", 2)).isInstanceOf(IllegalStateException.class);
        assertThat(database.isTransactionActive()).isTrue();

        database.rollback();
      }
      assertThat(notes()).isZero();
      assertThat(database.countType("V", true)).isZero();
    }

    @Test
    void createVerticesWithPropertiesRefusedInsideCallerTransaction() {
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        database.begin();
        saveNote("A2");

        assertThatThrownBy(() -> batch.createVertices("V", new Object[][] { { "name", "a" }, { "name", "b" } })).isInstanceOf(
            IllegalStateException.class);
        assertThat(database.isTransactionActive()).isTrue();

        database.rollback();
      }
      assertThat(notes()).isZero();
    }

    @Test
    void flushRefusedInsideCallerTransaction() {
      try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(100).build()) {
        final RID[] v = batch.createVertices("V", 2);
        batch.newEdge(v[0], "E", v[1]);

        database.begin();
        saveNote("C1");

        assertThatThrownBy(batch::flush).isInstanceOf(IllegalStateException.class);
        assertThat(database.isTransactionActive()).isTrue();
        database.rollback();

        // Once the caller's transaction is gone the buffered edge is still there and flushes normally
        batch.flush();
      }
      assertThat(notes()).isZero();
      assertThat(database.countType("E", true)).isEqualTo(1);
    }

    @Test
    void closeRefusedInsideCallerTransactionKeepsTheBatchOpenAndItsWork() {
      final GraphBatch batch = GraphBatch.builder(database).withBatchSize(100).build();
      final RID[] v = batch.createVertices("V", 2);
      batch.newEdge(v[0], "E", v[1]);

      database.begin();
      saveNote("C3");

      assertThatThrownBy(batch::close).isInstanceOf(IllegalStateException.class);
      assertThat(database.isTransactionActive()).isTrue();
      database.rollback();

      assertThat(notes()).isZero();
      assertThat(database.countType("E", true)).isZero();

      // Nothing was dropped: closing again outside the caller's transaction writes the pending edge, in both directions
      batch.close();
      assertThat(database.countType("E", true)).isEqualTo(1);
      assertThat(database.lookupByRID(v[1], true).asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);

      // The guard was released by the real close: a new batch can start
      try (final GraphBatch next = GraphBatch.builder(database).build()) {
        assertThat(next.createVertices("V", 1)).hasSize(1);
      }
    }

    @Test
    void batchWalPolicyDoesNotReachTheCallersTransactions() {
      database.transaction(() -> saveNote("warm-up"));

      try (final GraphBatch batch = GraphBatch.builder(database).withWAL(false).withWALFlush(WALFile.FlushType.NO).build()) {
        batch.createVertices("V", 2);

        final long before = walBytes();
        database.transaction(() -> saveNote("between-batch-calls"));
        assertThat(walBytes() - before).as("the caller's own commit must reach the WAL while the batch is open")
            .isGreaterThanOrEqualTo(4000);
      }

      final long before = walBytes();
      database.transaction(() -> saveNote("after-close"));
      assertThat(walBytes() - before).isGreaterThanOrEqualTo(4000);
    }

    @Test
    void refusalLeavesTheCallersWalFlushSettingAlone() {
      try (final GraphBatch batch = GraphBatch.builder(database).withWALFlush(WALFile.FlushType.NO).build()) {
        database.begin();
        ((DatabaseInternal) database).getTransaction().setWALFlush(WALFile.FlushType.YES_FULL);

        assertThatThrownBy(() -> batch.createVertices("V", 1)).isInstanceOf(IllegalStateException.class);
        assertThat(((DatabaseInternal) database).getTransaction().getThreadWALFlush()).isEqualTo(WALFile.FlushType.YES_FULL);

        database.rollback();
      }
    }

    @Test
    void edgeThatWouldAutoFlushInsideCallerTransactionIsNotBuffered() {
      try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(2).build()) {
        final RID[] v = batch.createVertices("V", 3);
        batch.newEdge(v[0], "E", v[1]);

        database.begin();
        saveNote("auto-flush");
        assertThatThrownBy(() -> batch.newEdge(v[1], "E", v[2])).isInstanceOf(IllegalStateException.class);
        assertThat(batch.getBufferedEdgeCount()).isEqualTo(1);
        database.rollback();
      }
      // Only the accepted edge was written: the rejected one is not replayed by close()
      assertThat(database.countType("E", true)).isEqualTo(1);
    }
  }

  /**
   * Issue #9575, discussion #9559: the edge pass of a GraphImporter load over 209M vertices stopped for 15 minutes every
   * 500,000 edges. Each flush read, for every distinct vertex it connected, the vertex record itself to find the edge list to
   * append to - one random disk access per vertex on a graph far larger than the memory - although the importer had created
   * every one of those vertices without edges and knew it. {@link GraphBatch#newEdgeWithHints} lets a caller say so.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchEndpointHints extends TestHelper {
    private static final int VERTICES   = 3_000;
    private static final int EDGES      = 20_000;
    private static final int BATCH_SIZE = 2_000;

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void hintedEndpointsAreNeverReadAndKeepEveryEdge(final boolean parallelFlush) {
      final RID[] rids = createVertices();
      final int[] outDegree = new int[VERTICES];
      final int[] inDegree = new int[VERTICES];

      // the premise: without hints, the batch reads the vertices it connects
      final long readsWithoutHints = load(rids, parallelFlush, 0, outDegree, inDegree, null, null);
      assertThat(readsWithoutHints).isGreaterThan(0L);

      // a second type, untouched: hinted the way GraphImporter hints, the first time a vertex is handed to a batch per direction
      final RID[] fresh = createVertices();
      final int[] freshOut = new int[VERTICES];
      final int[] freshIn = new int[VERTICES];
      final long readsWithHints = load(fresh, parallelFlush, 1, freshOut, freshIn, new boolean[VERTICES], new boolean[VERTICES]);
      assertThat(readsWithHints).as("vertices read by a load whose every endpoint was hinted").isZero();
      assertDegrees(fresh, freshOut, freshIn);

      // a second batch over the same vertices: they have edges now, so no hint, and the edges of the first batch survive
      final long readsOfSecondBatch = load(fresh, parallelFlush, 2, freshOut, freshIn, null, null);
      assertThat(readsOfSecondBatch).isGreaterThan(0L);
      assertDegrees(fresh, freshOut, freshIn);
    }

    /**
     * A wrong hint would orphan the edge list the vertex has. Under {@code -ea}, as every test suite runs, the batch checks the
     * hint against the vertex and refuses it before writing anything over the list.
     */
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void aWrongHintIsCaughtUnderAssertions(final boolean parallelFlush) {
      boolean assertions = false;
      assert assertions = true;
      assumeTrue(assertions, "the check runs under -ea only");

      final RID[] rids = createVertices();
      final int[] outDegree = new int[VERTICES];
      load(rids, parallelFlush, 0, outDegree, new int[VERTICES], null, null);
      int withEdges = 0;
      while (outDegree[withEdges] == 0)
        ++withEdges;

      final GraphBatch batch = database.batch().withBatchSize(BATCH_SIZE).withBidirectional(true).withWAL(false)
          .withParallelFlush(parallelFlush).build();
      batch.newEdgeWithHints(rids[withEdges], "E", rids[1], GraphBatch.SOURCE_HAS_NO_OUT_EDGES);
      // the parallel flush reports what failed in a worker wrapped, hence the stack trace rather than the type
      assertThatThrownBy(batch::close).hasStackTraceContaining("hinted to have no outgoing edges");
      if (database.isTransactionActive())
        database.rollback();
    }

    @ParameterizedTest
    @ValueSource(ints = { 4, -1, 0x100 })
    void unknownHintBitsAreRefused(final int hints) {
      final RID[] rids = createVertices();
      try (final GraphBatch batch = database.batch().withBidirectional(true).withWAL(false).build()) {
        assertThatThrownBy(() -> batch.newEdgeWithHints(rids[0], "E", rids[1], hints)).isInstanceOf(IllegalArgumentException.class);
      }
    }

    private RID[] createVertices() {
      final RID[] rids = new RID[VERTICES];
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++)
          rids[i] = database.newVertex("V").set("i", i).save().getIdentity();
      });
      return rids;
    }

    /**
     * Loads EDGES random edges. With {@code outTouched}/{@code inTouched} given, an endpoint is hinted the first time it is
     * handed to the batch in that direction, as GraphImporter does.
     */
    private long load(final RID[] rids, final boolean parallelFlush, final long seed, final int[] outDegree, final int[] inDegree,
        final boolean[] outTouched, final boolean[] inTouched) {
      final Random rnd = new Random(seed);
      final GraphBatch batch = database.batch().withBatchSize(BATCH_SIZE).withBidirectional(true).withWAL(false)
          .withParallelFlush(parallelFlush).withCommitEvery(500).build();
      try {
        for (int e = 0; e < EDGES; e++) {
          final int s = rnd.nextInt(VERTICES);
          final int d = rnd.nextInt(VERTICES);
          int hints = 0;
          if (outTouched != null && !outTouched[s]) {
            outTouched[s] = true;
            hints |= GraphBatch.SOURCE_HAS_NO_OUT_EDGES;
          }
          if (inTouched != null && !inTouched[d]) {
            inTouched[d] = true;
            hints |= GraphBatch.DESTINATION_HAS_NO_IN_EDGES;
          }
          batch.newEdgeWithHints(rids[s], "E", rids[d], hints);
          outDegree[s]++;
          inDegree[d]++;
        }
      } finally {
        batch.close();
      }
      // read after close(): the incoming side is connected there
      return batch.getVertexHeadReads();
    }

    private void assertDegrees(final RID[] rids, final int[] outDegree, final int[] inDegree) {
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++) {
          final Vertex v = rids[i].asVertex();
          assertThat(v.countEdges(Vertex.DIRECTION.OUT, "E")).as("out edges of vertex %d", i).isEqualTo(outDegree[i]);
          assertThat(v.countEdges(Vertex.DIRECTION.IN, "E")).as("in edges of vertex %d", i).isEqualTo(inDegree[i]);
        }
      });
    }

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    }
  }

  /**
   * Issue #9575, discussion #9559, 4th report: the edge pass of a GraphImporter load ran at 460,000 edges/s until 172M edges,
   * then fell to 1,400/s. The edge file listed each work's citations together, so the destinations came in order and the sources
   * at random: every flush met again most of the sources it connected, and read the head segment of each one's edge list from
   * the disk to append to it. A segment this batch wrote is sized to its content, so the append never fit: the read only told
   * the batch to chain a new segment, which needs nothing but the head's RID - and that the batch keeps in memory. Once the
   * segments outgrew the memory, each of those reads was a random disk read, ~0.7 ms an edge.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchSegmentHeadReads extends TestHelper {
    private static final int VERTICES   = 2_000;
    private static final int EDGES      = 30_000;
    private static final int BATCH_SIZE = 1_000;

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void segmentsThisBatchWroteAreChainedWithoutBeingRead(final boolean parallelFlush) {
      final RID[] rids = createVertices();
      final int[] outDegree = new int[VERTICES];
      final int[] inDegree = new int[VERTICES];

      // random sources AND destinations, small flushes and drains: every vertex comes back in many of them, both directions
      final long reads = load(rids, parallelFlush, 1, outDegree, inDegree, new boolean[VERTICES], new boolean[VERTICES]);
      assertThat(reads).as("edge-list segments read back by a batch that wrote every one of them").isZero();
      assertDegrees(rids, outDegree, inDegree);
      assertDatabaseIsConsistent();
    }

    /**
     * A segment the batch did not write may have room left - a pre-allocated one, or the head of a list an earlier load wrote -
     * so it is still read and filled before anything is chained to it.
     */
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void aHeadThisBatchDidNotWriteIsStillReadAndKeepsItsEdges(final boolean parallelFlush) {
      final RID[] rids = createVertices();
      final int[] outDegree = new int[VERTICES];
      final int[] inDegree = new int[VERTICES];
      load(rids, parallelFlush, 1, outDegree, inDegree, new boolean[VERTICES], new boolean[VERTICES]);

      final long reads = load(rids, parallelFlush, 2, outDegree, inDegree, null, null);
      assertThat(reads).isGreaterThan(0L);
      assertDegrees(rids, outDegree, inDegree);
      assertDatabaseIsConsistent();
    }

    private RID[] createVertices() {
      final RID[] rids = new RID[VERTICES];
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++)
          rids[i] = database.newVertex("V").set("i", i).save().getIdentity();
      });
      return rids;
    }

    /**
     * Loads EDGES random edges and answers the edge-list segments the batch read. With {@code outTouched}/{@code inTouched} given,
     * an endpoint is hinted the first time it is handed to the batch in that direction, as GraphImporter does.
     */
    private long load(final RID[] rids, final boolean parallelFlush, final long seed, final int[] outDegree, final int[] inDegree,
        final boolean[] outTouched, final boolean[] inTouched) {
      final Random rnd = new Random(seed);
      final GraphBatch batch = database.batch().withBatchSize(BATCH_SIZE).withBidirectional(true).withWAL(false)
          .withParallelFlush(parallelFlush).withCommitEvery(300).withMaxDeferredIncomingEdges(BATCH_SIZE * 3).build();
      try {
        for (int e = 0; e < EDGES; e++) {
          final int s = rnd.nextInt(VERTICES);
          final int d = rnd.nextInt(VERTICES);
          int hints = 0;
          if (outTouched != null && !outTouched[s]) {
            outTouched[s] = true;
            hints |= GraphBatch.SOURCE_HAS_NO_OUT_EDGES;
          }
          if (inTouched != null && !inTouched[d]) {
            inTouched[d] = true;
            hints |= GraphBatch.DESTINATION_HAS_NO_IN_EDGES;
          }
          batch.newEdgeWithHints(rids[s], "E", rids[d], hints);
          outDegree[s]++;
          inDegree[d]++;
        }
      } finally {
        batch.close();
      }
      // read after close(): the incoming side is connected there
      return batch.getSegmentHeadReads();
    }

    private void assertDegrees(final RID[] rids, final int[] outDegree, final int[] inDegree) {
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++) {
          final Vertex v = rids[i].asVertex();
          assertThat(v.countEdges(Vertex.DIRECTION.OUT, "E")).as("out edges of vertex %d", i).isEqualTo(outDegree[i]);
          assertThat(v.countEdges(Vertex.DIRECTION.IN, "E")).as("in edges of vertex %d", i).isEqualTo(inDegree[i]);
        }
      });
    }

    private void assertDatabaseIsConsistent() {
      try (final ResultSet result = database.command("sql", "CHECK DATABASE")) {
        final Result row = result.next();
        assertThat(row.<Collection<?>>getProperty("corruptedRecords")).as("corruptedRecords: %s", row.toJSON()).isEmpty();
        assertThat(((Number) row.getProperty("invalidLinks")).longValue()).as("invalidLinks: %s", row.toJSON()).isZero();
      }
    }

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    }
  }

  /**
   * Issue #9814: {@code GraphBatch.createEdgeRecordsBulk} serialized the edge records of a single-bucket flush with its own
   * template serializer and wrote them straight to the bucket, a path that never reaches {@code BinarySerializer} and so never
   * honors {@code Property.isExternal()}: an EXTERNAL edge property written through {@code GraphBatch} was stored INLINE in the
   * edge record, silently breaking the contract (small primary records, large values in the paired bucket). Reads worked, so
   * nothing errored; only a {@code REBUILD TYPE} moved the values out afterwards.
   * <p>
   * Resolving it for an INHERITED EXTERNAL property surfaced the same blind spot in the ordinary write path: the serializer looked
   * the property up in the record's own type only, so a subtype's record kept the value inline although the subtype owns a paired
   * bucket for it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchExternalEdgeProperty extends TestHelper {

    private static final String BLOB = "q".repeat(500);

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final VertexType v = database.getSchema().createVertexType("V9814", 1);
        v.createProperty("id", Type.INTEGER);
      });
    }

    private RID[] vertices(final int count) {
      final RID[] rids = new RID[count];
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          rids[i] = database.newVertex("V9814").set("id", i).save().getIdentity();
      });
      return rids;
    }

    /** Records held by the paired external buckets of {@code typeName}. */
    private long externalRecords(final String typeName) {
      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
      long total = 0;
      for (final Bucket bucket : type.getBuckets(false)) {
        final Integer externalBucketId = type.getExternalBucketIdFor(bucket.getFileId());
        if (externalBucketId != null)
          total += database.getSchema().getBucketById(externalBucketId).count();
      }
      return total;
    }

    private EdgeType edgeTypeWithExternalBlob(final String name) {
      final EdgeType type = database.getSchema().createEdgeType(name, 1);
      type.createProperty("blob", Type.STRING).setExternal(true);
      return type;
    }

    private String blobOfFirstEdge(final RID from, final String edgeType) {
      final String[] blob = new String[1];
      database.transaction(
          () -> blob[0] = from.asVertex().getEdges(Vertex.DIRECTION.OUT, edgeType).iterator().next().getString("blob"));
      return blob[0];
    }

    @Test
    void externalPropertyDeclaredOnTheEdgeTypeIsStoredInThePairedBucket() {
      edgeTypeWithExternalBlob("E9814");
      final RID[] v = vertices(2);

      try (final GraphBatch batch = database.batch().build()) {
        batch.newEdge(v[0], "E9814", v[1], "blob", BLOB);
      }

      assertThat(externalRecords("E9814")).as("the value must live in the paired external bucket").isEqualTo(1);
      assertThat(blobOfFirstEdge(v[0], "E9814")).isEqualTo(BLOB);
    }

    @Test
    void theBatchStoresTheEdgeExactlyAsTheOrdinaryApiDoes() {
      edgeTypeWithExternalBlob("EBatch9814");
      edgeTypeWithExternalBlob("EApi9814");
      final RID[] v = vertices(2);

      try (final GraphBatch batch = database.batch().build()) {
        for (int i = 0; i < 5; i++)
          batch.newEdge(v[0], "EBatch9814", v[1], "blob", BLOB + i);
      }
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          v[0].asVertex().modify().newEdge("EApi9814", v[1], "blob", BLOB + i).save();
      });

      assertThat(externalRecords("EApi9814")).isEqualTo(5);
      assertThat(externalRecords("EBatch9814")).isEqualTo(externalRecords("EApi9814"));
    }

    @Test
    void externalPropertyInheritedFromASuperTypeIsExternalizedToo() {
      edgeTypeWithExternalBlob("EParent9814");
      database.getSchema().buildEdgeType().withName("EChild9814").withSuperType("EParent9814").withTotalBuckets(1).create();
      final RID[] v = vertices(2);

      try (final GraphBatch batch = database.batch().build()) {
        batch.newEdge(v[0], "EChild9814", v[1], "blob", BLOB);
      }

      assertThat(externalRecords("EChild9814")).isEqualTo(1);
      assertThat(blobOfFirstEdge(v[0], "EChild9814")).isEqualTo(BLOB);
    }

    /**
     * The ordinary write path had the same blind spot: the serializer looked the property up in the record's own type only, so a
     * value of an EXTERNAL property inherited from a super type was stored INLINE although the subtype owns a paired bucket for it.
     */
    @Test
    void ordinarySaveExternalizesAPropertyInheritedFromASuperType() {
      database.getSchema().createDocumentType("DParent9814", 1).createProperty("blob", Type.STRING).setExternal(true);
      database.getSchema().buildDocumentType().withName("DChild9814").withSuperType("DParent9814").withTotalBuckets(1).create();
      edgeTypeWithExternalBlob("EParentApi9814");
      database.getSchema().buildEdgeType().withName("EChildApi9814").withSuperType("EParentApi9814").withTotalBuckets(1).create();
      final RID[] v = vertices(2);

      database.transaction(() -> {
        database.newDocument("DChild9814").set("blob", BLOB).save();
        v[0].asVertex().modify().newEdge("EChildApi9814", v[1], "blob", BLOB).save();
      });

      assertThat(externalRecords("DChild9814")).isEqualTo(1);
      assertThat(externalRecords("EChildApi9814")).isEqualTo(1);
      assertThat(database.query("sql", "SELECT blob FROM DChild9814").next().<String>getProperty("blob")).isEqualTo(BLOB);
    }

    /** Records written inline before the property turned EXTERNAL keep reading, and move out of the record when they are updated. */
    @Test
    void anOldInlineRecordOfASubtypeStillReadsAndMovesOutWhenUpdated() {
      final DocumentType parent = database.getSchema().createDocumentType("DParentOld9814", 1);
      parent.createProperty("blob", Type.STRING);
      database.getSchema().buildDocumentType().withName("DChildOld9814").withSuperType("DParentOld9814").withTotalBuckets(1).create();
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        rids[0] = database.newDocument("DChildOld9814").set("blob", BLOB + "0").save().getIdentity();
        rids[1] = database.newDocument("DChildOld9814").set("blob", BLOB + "1").save().getIdentity();
      });

      parent.getProperty("blob").setExternal(true);
      assertThat(externalRecords("DChildOld9814")).as("both records are still inline").isZero();

      database.transaction(() -> {
        final MutableDocument updated = rids[0].asDocument().modify();
        updated.set("blob", BLOB + "updated");
        updated.save();
      });

      assertThat(externalRecords("DChildOld9814")).as("only the updated record moved out").isEqualTo(1);
      database.transaction(() -> {
        assertThat(rids[0].asDocument().getString("blob")).isEqualTo(BLOB + "updated");
        assertThat(rids[1].asDocument().getString("blob")).isEqualTo(BLOB + "1");
      });
    }

    @Test
    void externalAndInlinePropertiesOfOneEdgeAreSplitCorrectly() {
      final EdgeType type = edgeTypeWithExternalBlob("EMixed9814");
      type.createProperty("weight", Type.INTEGER);
      final RID[] v = vertices(2);

      try (final GraphBatch batch = database.batch().build()) {
        batch.newEdge(v[0], "EMixed9814", v[1], "blob", BLOB, "weight", 7, "undeclared", "kept");
      }

      assertThat(externalRecords("EMixed9814")).isEqualTo(1);
      database.transaction(() -> {
        final Edge edge = v[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "EMixed9814").iterator().next();
        assertThat(edge.getString("blob")).isEqualTo(BLOB);
        assertThat(edge.getInteger("weight")).isEqualTo(7);
        assertThat(edge.getString("undeclared")).isEqualTo("kept");
      });
    }

    @Test
    void aUniqueIndexOnTheEdgeStillHoldsWhenTheFlushTakesTheOrdinaryPath() {
      final EdgeType type = edgeTypeWithExternalBlob("EIndexed9814");
      type.createProperty("key", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "EIndexed9814", "key");
      final RID[] v = vertices(3);

      try (final GraphBatch batch = database.batch().build()) {
        batch.newEdge(v[0], "EIndexed9814", v[1], "blob", BLOB, "key", 1);
        batch.newEdge(v[0], "EIndexed9814", v[2], "blob", BLOB, "key", 2);
      }

      assertThat(externalRecords("EIndexed9814")).isEqualTo(2);
      assertThat(database.query("sql", "SELECT FROM EIndexed9814 WHERE key = 2").stream().count()).isEqualTo(1);
    }

    @Test
    void aTypeWithoutExternalPropertiesKeepsTheBulkPathAndPairsNoBucket() {
      database.getSchema().createEdgeType("EPlain9814", 1).createProperty("blob", Type.STRING);
      final RID[] v = vertices(2);

      try (final GraphBatch batch = database.batch().build()) {
        batch.newEdge(v[0], "EPlain9814", v[1], "blob", BLOB);
      }

      assertThat(externalRecords("EPlain9814")).isZero();
      assertThat(blobOfFirstEdge(v[0], "EPlain9814")).isEqualTo(BLOB);
    }

    @Test
    void externalValuesSurviveAReopen() {
      edgeTypeWithExternalBlob("EReopen9814");
      final RID[] v = vertices(2);

      try (final GraphBatch batch = database.batch().build()) {
        batch.newEdge(v[0], "EReopen9814", v[1], "blob", BLOB);
      }
      reopenDatabase();

      assertThat(externalRecords("EReopen9814")).isEqualTo(1);
      assertThat(database.query("sql", "SELECT blob FROM EReopen9814").next().<String>getProperty("blob")).isEqualTo(BLOB);
    }
  }
}
