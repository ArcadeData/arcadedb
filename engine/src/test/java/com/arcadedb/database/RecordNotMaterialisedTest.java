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
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A record whose content is not materialised must say so, naming itself, instead of letting the caller dereference a
 * {@code null} buffer.
 * <p>
 * Every accessor of {@code ImmutableDocument} and its subclasses lazy-loads through {@code checkForLazyLoading()} and
 * then reads straight from {@code buffer}. That method can legitimately return leaving the buffer {@code null} - an
 * {@code AFTER READ} listener filtered the record away - and a concurrently {@code reload()}ed instance can have it
 * nulled underneath a caller that is between the check and the dereference. Both used to surface as
 * <p>
 * {@code Cannot invoke "com.arcadedb.database.Binary.rewind()" because "this.buffer" is null}
 * <p>
 * which names no record, no operation and no reason; the read path surfaced the same state one frame deeper, inside
 * {@code BinarySerializer}, where it was swallowed and logged as {@code "Possible corrupted record"} - a diagnosis
 * that sends the reader looking for corruption that is not there.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RecordNotMaterialisedTest extends TestHelper {
  private static final String DOC_TYPE    = "Doc";
  private static final String VERTEX_TYPE = "Vert";
  private static final String EDGE_TYPE   = "Rel";

  private final List<Runnable> unregister = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(DOC_TYPE).create();
      database.getSchema().buildVertexType().withName(VERTEX_TYPE).create();
      database.getSchema().buildEdgeType().withName(EDGE_TYPE).create();
    });
  }

  /**
   * The listeners have to go before {@code TestHelper} runs CHECK DATABASE: a filter left registered makes the
   * integrity check itself unable to read the records it is checking, and it reports the edge as an invalid link.
   */
  @Override
  protected void endTest() {
    unregister.forEach(Runnable::run);
    unregister.clear();
  }

  /**
   * A document filtered away by an after-read listener: every accessor must name the record and the reason. Before the
   * fix {@code modify()} raised a bare NPE, while {@code get()}, {@code has()} and {@code toJSON()} handed the null
   * buffer to the serializer, which swallowed the NPE and answered {@code null} / {@code false} / {@code {}} - a
   * filtered record was indistinguishable from an empty one.
   */
  @Test
  void everyAccessorOfAFilteredDocumentNamesTheRecord() {
    final RID rid = newDocument();
    filterAway(DOC_TYPE);

    database.transaction(() -> {
      final Document doc = database.lookupByRID(rid, false).asDocument();

      assertUnavailable(rid, () -> doc.get("name"));
      assertUnavailable(rid, () -> doc.has("name"));
      assertUnavailable(rid, () -> doc.getPropertyNames());
      assertUnavailable(rid, () -> doc.toJSON(true));
      assertUnavailable(rid, () -> doc.toMap(true));
      assertUnavailable(rid, () -> doc.modify());
    });
  }

  /**
   * {@code propertiesAsMap()} keeps its documented empty-map answer for the same record: callers of it (detaching,
   * {@code copyType()}, {@code toJSON(String...)}) depend on that, and it is the one accessor that already told the
   * truth about a null buffer.
   */
  @Test
  void propertiesAsMapOfAFilteredDocumentStaysEmpty() {
    final RID rid = newDocument();
    filterAway(DOC_TYPE);

    database.transaction(() -> assertThat(database.lookupByRID(rid, false).asDocument().propertiesAsMap()).isEmpty());
  }

  /**
   * The vertex path has its own {@code checkForLazyLoading()} override, which re-reads the fixed edge-pointer prefix
   * out of the buffer: the filtered record reached {@code parseEdgePointers()} and NPEd there instead.
   */
  @Test
  void filteredVertexNamesTheRecord() {
    final RID rid = newVertex();
    filterAway(VERTEX_TYPE);

    database.transaction(() -> {
      final Vertex vertex = database.lookupByRID(rid, false).asVertex();

      assertUnavailable(rid, () -> vertex.modify());
      assertUnavailable(rid, () -> vertex.get("name"));
      assertUnavailable(rid, () -> vertex.toJSON(true));
    });
  }

  /**
   * Same for an edge. {@code ImmutableEdge.modify()} has a legitimate no-buffer branch - an edge built over its two
   * endpoints carries no record content to copy - but a filtered edge has no endpoints either, so that branch used to
   * hand back a {@code MutableEdge} with {@code null} out and in, silently dropping the edge's properties on the next
   * save.
   */
  @Test
  void filteredEdgeNamesTheRecord() {
    final RID rid = newEdge();
    filterAway(EDGE_TYPE);

    database.transaction(() -> {
      final Edge edge = database.lookupByRID(rid, false).asEdge();

      assertUnavailable(rid, () -> edge.modify());
      assertUnavailable(rid, () -> edge.toJSON(true));
    });
  }

  /**
   * The state the production incident actually hit: the buffer goes away between the lazy-load and the dereference,
   * because another thread called {@code reload()} on a record instance it shares with this one. Simulated here
   * deterministically by an after-read listener that unloads the record it is handed and returns it - the same window,
   * without the timing.
   * <p>
   * Sharing a record instance between threads is not supported and this test does not make it so; it pins that the
   * failure says which record and why instead of arriving as an NPE on an unnamed field.
   */
  @Test
  void aRecordUnloadedMidFlightNamesTheRecord() {
    final RID docRid = newDocument();
    final RID vertexRid = newVertex();
    unloadOnRead(DOC_TYPE);
    unloadOnRead(VERTEX_TYPE);

    database.transaction(() -> {
      assertUnavailable(docRid, () -> database.lookupByRID(docRid, false).asDocument().get("name"));
      assertUnavailable(vertexRid, () -> database.lookupByRID(vertexRid, false).asVertex().modify());
    });
  }

  /**
   * A vertex deleted after it was loaded reports the same exception, from whichever of the two paths reaches the
   * missing content first: {@code reload()} swallows the {@link RecordNotFoundException} the bucket raised (leaving
   * the record unmaterialised), and the accessor that touches it next re-raises it.
   */
  @Test
  void aVertexDeletedAfterLoadingNamesTheRecord() {
    final RID rid = newVertex();

    final AtomicReference<Vertex> stale = new AtomicReference<>();
    database.transaction(() -> stale.set(database.lookupByRID(rid, true).asVertex()));
    database.transaction(() -> database.lookupByRID(rid, true).delete());

    database.transaction(() -> {
      stale.get().reload();
      assertThatThrownBy(() -> stale.get().get("name"))
          .isInstanceOf(RecordNotFoundException.class)
          .hasMessageContaining(rid.toString());
    });
  }

  /**
   * The real race, bounded: one thread reads a shared vertex while another reloads it. The point is the SHAPE of the
   * failure, not whether it happens - a run that never hits the window passes without asserting anything, which is why
   * the deterministic reproducer above exists too. What must never appear is a {@link NullPointerException}.
   */
  @Test
  @Timeout(60)
  void aSharedRecordNeverFailsWithANullPointerException() throws Exception {
    final RID rid = newVertex();
    final int rounds = 2_000;

    final AtomicReference<Vertex> shared = new AtomicReference<>();
    database.transaction(() -> shared.set(database.lookupByRID(rid, true).asVertex()));

    final List<Throwable> failures = new ArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);

    final Thread reader = new Thread(() -> {
      awaitQuietly(start);
      for (int i = 0; i < rounds; i++)
        database.transaction(() -> {
          try {
            shared.get().get("name");
          } catch (final RecordNotFoundException e) {
            // EXPECTED: THE OTHER THREAD UNLOADED THE RECORD WHILE THIS ONE WAS READING IT
          } catch (final Throwable t) {
            synchronized (failures) {
              failures.add(t);
            }
          }
        });
    });

    final Thread reloader = new Thread(() -> {
      awaitQuietly(start);
      for (int i = 0; i < rounds; i++)
        database.transaction(() -> {
          try {
            shared.get().reload();
          } catch (final RecordNotFoundException e) {
            // EXPECTED, SAME REASON
          } catch (final Throwable t) {
            synchronized (failures) {
              failures.add(t);
            }
          }
        });
    });

    reader.start();
    reloader.start();
    start.countDown();
    reader.join();
    reloader.join();

    assertThat(failures).as("a record shared between threads must never fail with an NPE on an unnamed field")
        .noneMatch(NullPointerException.class::isInstance);
  }

  private void assertUnavailable(final RID rid, final ThrowingCallable access) {
    assertThatThrownBy(access)
        .as("the record must name itself and say why its content is missing")
        .isInstanceOf(RecordNotFoundException.class)
        .hasMessageContaining(rid.toString())
        .hasMessageContaining("after-read");
  }

  private void filterAway(final String typeName) {
    register(typeName, record -> null);
  }

  private void unloadOnRead(final String typeName) {
    register(typeName, record -> {
      ((BaseRecord) record).setBuffer(null);
      return record;
    });
  }

  private void register(final String typeName, final AfterRecordReadListener listener) {
    final RecordEvents events = database.getSchema().getType(typeName).getEvents();
    events.registerListener(listener);
    unregister.add(() -> events.unregisterListener(listener));
  }

  private RID newDocument() {
    final AtomicReference<RID> rid = new AtomicReference<>();
    database.transaction(() -> rid.set(database.newDocument(DOC_TYPE).set("name", "Jay").save().getIdentity()));
    return rid.get();
  }

  private RID newVertex() {
    final AtomicReference<RID> rid = new AtomicReference<>();
    database.transaction(() -> rid.set(database.newVertex(VERTEX_TYPE).set("name", "Jay").save().getIdentity()));
    return rid.get();
  }

  private RID newEdge() {
    final AtomicReference<RID> rid = new AtomicReference<>();
    database.transaction(() -> {
      final MutableVertex from = database.newVertex(VERTEX_TYPE).set("name", "from").save();
      final MutableVertex to = database.newVertex(VERTEX_TYPE).set("name", "to").save();
      rid.set(from.newEdge(EDGE_TYPE, to, "since", 2020).getIdentity());
    });
    return rid.get();
  }

  private static void awaitQuietly(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
