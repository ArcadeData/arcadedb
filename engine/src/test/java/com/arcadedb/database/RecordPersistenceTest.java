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
import com.arcadedb.database.DocumentValidator.LinkTargetTypeDeferral;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of the database engine: RecordPersistence. Each nested class keeps the regression of one reported issue (see the
 * Javadoc of each).
 */
public class RecordPersistenceTest {
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
  @Nested
  class CreateRecordStatistic extends TestHelper {

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

  /**
   * Issue #10483: a restore that writes records in the order of a file meets a {@code LINK OF T}, {@code LIST OF T} or {@code MAP OF T}
   * before the record it names, and must be able to suspend the check of the link's target type for that write, and only that check.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DeferredLinkTargetTypeCheck extends TestHelper {
    private RID wanted;
    private RID other;

    @BeforeEach
    void schema() {
      database.command("sql", "CREATE DOCUMENT TYPE Wanted");
      database.command("sql", "CREATE DOCUMENT TYPE Other");
      database.command("sql", "CREATE DOCUMENT TYPE Holder");
      database.command("sql", "CREATE PROPERTY Holder.link LINK OF Wanted");
      database.command("sql", "CREATE PROPERTY Holder.list LIST OF Wanted");
      database.command("sql", "CREATE PROPERTY Holder.map MAP OF Wanted");
      database.command("sql", "CREATE PROPERTY Holder.name STRING");
      database.transaction(() -> {
        wanted = database.newDocument("Wanted").save().getIdentity();
        other = database.newDocument("Other").save().getIdentity();
      });
    }

    @Test
    void aLinkToTheWrongTypeIsRefusedByDefault() {
      assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class).hasMessageContaining("LINK of 'Wanted'");
      assertThatThrownBy(() -> save(holderListing(other))).isInstanceOf(ValidationException.class).hasMessageContaining("LIST of 'Wanted'");
      assertThatThrownBy(() -> save(holderMapping(other))).isInstanceOf(ValidationException.class).hasMessageContaining("MAP of");
    }

    @Test
    void aSuspendedCheckLetsTheWrongTypeThrough() {
      try (final LinkTargetTypeDeferral ignored = DocumentValidator.deferLinkTargetTypeChecks()) {
        save(holderLinking(other));
        save(holderListing(other));
        save(holderMapping(other));
      }
      assertThat(database.countType("Holder", false)).isEqualTo(3L);
    }

    /** The suspension is of the target's type only: the other constraints are checked as always. */
    @Test
    void aSuspendedCheckStillChecksEverythingElse() {
      database.command("sql", "CREATE PROPERTY Holder.must STRING");
      database.command("sql", "ALTER PROPERTY Holder.must MANDATORY true");
      try (final LinkTargetTypeDeferral ignored = DocumentValidator.deferLinkTargetTypeChecks()) {
        assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class).hasMessageContaining("Holder.must");
      }
    }

    @Test
    void theCheckIsBackWhenTheScopeEndsWhateverHappenedInIt() {
      try (final LinkTargetTypeDeferral ignored = DocumentValidator.deferLinkTargetTypeChecks()) {
        throw new IllegalStateException("the restore failed");
      } catch (final IllegalStateException expected) {
        // the scope was closed on the way out
      }
      assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class);
    }

    @Test
    void scopesNest() {
      try (final LinkTargetTypeDeferral outer = DocumentValidator.deferLinkTargetTypeChecks()) {
        try (final LinkTargetTypeDeferral inner = DocumentValidator.deferLinkTargetTypeChecks()) {
          save(holderLinking(other));
        }
        // the inner scope's end does not end the outer one
        save(holderLinking(other));
      }
      assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class);
      assertThat(database.countType("Holder", false)).isEqualTo(2L);
    }

    /** A second close ends nothing: it cannot lift the deferral of an enclosing scope. */
    @Test
    void closingAScopeTwiceEndsItOnce() {
      try (final LinkTargetTypeDeferral outer = DocumentValidator.deferLinkTargetTypeChecks()) {
        final LinkTargetTypeDeferral inner = DocumentValidator.deferLinkTargetTypeChecks();
        inner.close();
        inner.close();
        // the outer scope is still open
        save(holderLinking(other));
      }
      assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class);
      assertThat(database.countType("Holder", false)).isEqualTo(1L);
    }

    /** A scope closed from another thread is a mistake that is reported, and the thread that opened it keeps its deferral. */
    @Test
    void aScopeClosedFromAnotherThreadIsRefused() throws Exception {
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      try (final LinkTargetTypeDeferral scope = DocumentValidator.deferLinkTargetTypeChecks()) {
        final Thread thread = new Thread(() -> {
          try {
            scope.close();
          } catch (final Throwable t) {
            failure.set(t);
          }
        });
        thread.start();
        thread.join();
        // still deferred here
        save(holderLinking(other));
      }
      assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("thread that opened it");
      assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class);
    }

    @Test
    void aScopeBelongsToItsThread() throws Exception {
      final AtomicReference<Throwable> otherThreadFailure = new AtomicReference<>();
      try (final LinkTargetTypeDeferral ignored = DocumentValidator.deferLinkTargetTypeChecks()) {
        final Thread thread = new Thread(() -> {
          try {
            save(holderLinking(other));
          } catch (final Throwable t) {
            otherThreadFailure.set(t);
          }
        });
        thread.start();
        thread.join();
      }
      assertThat(otherThreadFailure.get()).isInstanceOf(ValidationException.class);
    }

    /** A link into a bucket no type of this database owns is a refusal that names the property, not a NullPointerException. */
    @Test
    void aLinkIntoNoBucketIsAValidationFailure() {
      final RID nowhere = new RID(9_999, 0L);
      assertThatThrownBy(() -> save(holderLinking(nowhere))).isInstanceOf(ValidationException.class)
          .hasMessageContaining("Holder.link");
      assertThatThrownBy(() -> save(holderListing(nowhere))).isInstanceOf(ValidationException.class)
          .hasMessageContaining("Holder.list");
    }

    @Test
    void aLinkToTheRightTypeIsAcceptedWithOrWithoutTheScope() {
      save(holderLinking(wanted));
      try (final LinkTargetTypeDeferral ignored = DocumentValidator.deferLinkTargetTypeChecks()) {
        save(holderLinking(wanted));
      }
      assertThat(database.countType("Holder", false)).isEqualTo(2L);
    }

    private void save(final MutableDocument holder) {
      database.transaction(holder::save);
    }

    private MutableDocument holderLinking(final RID target) {
      return database.newDocument("Holder").set("link", target);
    }

    private MutableDocument holderListing(final RID target) {
      final List<Object> list = new ArrayList<>();
      list.add(target);
      return database.newDocument("Holder").set("list", list);
    }

    private MutableDocument holderMapping(final RID target) {
      final LinkedHashMap<String, Object> map = new LinkedHashMap<>();
      map.put("k", target);
      return database.newDocument("Holder").set("map", map);
    }
  }

  /**
   * #10955: an UPDATE of a record one of whose properties cannot be decoded must not save it back without that property,
   * which turns a read-time corruption into permanent data loss. The update is refused with an error naming the record and
   * the property, and the stored bytes are left untouched.
   */
  @Nested
  class UpdateUndecodableProperty extends TestHelper {
    private static final byte UNKNOWN_TYPE = 120;

    /** These tests deliberately corrupt a record, so the blanket end-of-test check would always fire. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final var type = database.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("body", Type.STRING);
        type.createProperty("other", Type.STRING);
      });
    }

    @Test
    void anUpdateOfAnotherPropertyIsRefusedAndTheStoredBytesStay() {
      final RID rid = createCorruptedRecord();
      final byte[] before = rawBytes(rid);

      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
        doc.set("other", "changed");
        doc.save();
      })).hasMessageContaining(rid.toString()).hasMessageContaining("body");

      assertThat(rawBytes(rid)).isEqualTo(before);
    }

    @Test
    void aSqlUpdateOfAnotherPropertyIsRefusedAndTheStoredBytesStay() {
      final RID rid = createCorruptedRecord();
      final byte[] before = rawBytes(rid);

      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE " + rid + " SET other = 'changed'")))
          .hasMessageContaining("body");

      assertThat(rawBytes(rid)).isEqualTo(before);
    }

    @Test
    void aVertexSaveIsRefusedToo() {
      database.transaction(() -> {
        final var type = database.getSchema().createVertexType("V");
        type.createProperty("body", Type.STRING);
      });
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newVertex("V").set("body", "x".repeat(200)).set("other", "o").save().getIdentity());
      // A VERTEX RECORD CARRIES THE TWO EDGE-LIST HEAD RIDS (2 x 12 BYTES) AFTER THE TYPE BYTE
      corruptBody(rid[0], 25);
      final byte[] before = rawBytes(rid[0]);

      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableVertex vertex = database.lookupByRID(rid[0], true).asVertex().modify();
        vertex.set("other", "changed");
        vertex.save();
      })).hasMessageContaining("body");

      assertThat(rawBytes(rid[0])).isEqualTo(before);
    }

    @Test
    void anUpdateOfAHealthyRecordStillWorks() {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Doc").set("id", 1).set("body", "b").set("other", "o").save().getIdentity());

      database.transaction(() -> {
        final MutableDocument doc = database.lookupByRID(rid[0], true).asDocument().modify();
        doc.set("other", "changed");
        doc.save();
      });

      final Document reloaded = database.lookupByRID(rid[0], true).asDocument();
      assertThat(reloaded.getString("other")).isEqualTo("changed");
      assertThat(reloaded.getString("body")).isEqualTo("b");
    }

    @Test
    void anExplicitFullOverwriteOfTheContentIsAllowed() {
      final RID rid = createCorruptedRecord();

      database.transaction(() -> {
        final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
        doc.fromMap(Map.of("id", 2, "body", "fresh", "other", "o2"));
        doc.save();
      });

      final Document reloaded = database.lookupByRID(rid, true).asDocument();
      assertThat(reloaded.getString("body")).isEqualTo("fresh");
    }

    private RID createCorruptedRecord() {
      final RID[] rid = new RID[1];
      database.transaction(
          () -> rid[0] = database.newDocument("Doc").set("id", 1).set("body", "x".repeat(200)).set("other", "o").save().getIdentity());

      corruptBody(rid[0], 1);

      // PRECONDITION: THE LENIENT READ NOW LOSES THE PROPERTY, SO THE TEST REALLY EXERCISES THE UNDECODABLE SHAPE
      final Document read = database.lookupByRID(rid[0], true).asDocument();
      assertThat(read.toMap(false)).doesNotContainKey("body");
      return rid[0];
    }

    private void corruptBody(final RID rid, final int propertiesStart) {
      final int bodyId = ((DatabaseInternal) database).getSchema().getDictionary().getIdByName("body", false);
      onRecordPage(rid, (page, recordOffset) -> {
        final Binary record = ((LocalBucket) ((DatabaseInternal) database).getSchema().getBucketById(rid.getBucketId())).getRecord(rid);
        final int[] slotByNameId = new int[bodyId + 1];
        Arrays.fill(slotByNameId, -1);
        slotByNameId[bodyId] = 0;
        final int[] positions = new int[1];
        record.position(propertiesStart);
        assertThat(((DatabaseInternal) database).getSerializer().locateProperties(record, slotByNameId, positions, 1)).isTrue();
        assertThat(positions[0]).isGreaterThan(0);
        final long[] recordSize = page.readNumberAndSize(recordOffset);
        page.writeByte((int) (recordOffset + recordSize[1] + positions[0]), UNKNOWN_TYPE);
      });
    }

    private byte[] rawBytes(final RID rid) {
      final Binary record = ((LocalBucket) ((DatabaseInternal) database).getSchema().getBucketById(rid.getBucketId())).getRecord(rid);
      return Arrays.copyOf(record.getContent(), record.size());
    }

    private void onRecordPage(final RID rid, final BiConsumer<MutablePage, Integer> mutation) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int fileId = rid.getBucketId();
      final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(fileId);
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
      final int maxRecordsInPage = bucket.getMaxRecordsInPage();

      final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
      final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

      db.transaction(() -> {
        try {
          final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, pageId), pageSize, false);
          final int slotOffset = Binary.SHORT_SERIALIZED_SIZE + (positionInPage * Binary.INT_SERIALIZED_SIZE);
          final int recordOffset = (int) page.readUnsignedInt(slotOffset);
          assertThat(recordOffset).as("the record must still occupy its slot").isGreaterThan(0);
          mutation.accept(page, recordOffset);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  /**
   * Issue #10856: the bulk float and double reads take their element count from the record, so a corrupt count must be
   * refused without moving the position, both when the backing array ends with the content and when it has spare capacity
   * past it, whose bytes are not content.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BinaryBulkArrayBounds {

    @Test
    void aFloatCountPastTheContentIsRefusedWithSpareCapacity() {
      final Binary buffer = new Binary(1024);
      buffer.putFloatArray(new float[] { 1f, 2f, 3f });
      buffer.position(0);

      assertThatThrownBy(() -> buffer.getFloatArray(4)).isInstanceOf(BufferUnderflowException.class);
      assertThat(buffer.position()).isZero();
      assertThat(buffer.getFloatArray(3)).containsExactly(1f, 2f, 3f);
    }

    @Test
    void aDoubleCountPastTheContentIsRefusedWithSpareCapacity() {
      final Binary buffer = new Binary(1024);
      buffer.putDoubleArray(new double[] { 1d, 2d });
      buffer.position(0);

      assertThatThrownBy(() -> buffer.getDoubleArray(3)).isInstanceOf(BufferUnderflowException.class);
      assertThat(buffer.position()).isZero();
      assertThat(buffer.getDoubleArray(2)).containsExactly(1d, 2d);
    }

    @Test
    void aCountPastAnExactBackingArrayIsRefused() {
      final Binary written = new Binary(64);
      written.putFloatArray(new float[] { 1f, 2f });
      final Binary buffer = new Binary(Arrays.copyOf(written.getContent(), written.size()));

      assertThatThrownBy(() -> buffer.getFloatArray(3)).isInstanceOf(BufferUnderflowException.class);
      assertThat(buffer.position()).isZero();
      assertThatThrownBy(() -> buffer.getDoubleArray(2)).isInstanceOf(BufferUnderflowException.class);
      assertThat(buffer.position()).isZero();
    }

    @Test
    void aNegativeOrOverflowingCountIsRefused() {
      final Binary buffer = new Binary(64);
      buffer.putFloatArray(new float[] { 1f });
      buffer.position(0);

      assertThatThrownBy(() -> buffer.getFloatArray(-1)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> buffer.getDoubleArray(Integer.MAX_VALUE)).isInstanceOf(IllegalArgumentException.class);
      assertThat(buffer.position()).isZero();
    }
  }

  /**
   * Reproduces issue #6818: {@code MutableDocument.getPropertyNames()} used to hand out the record's own
   * {@code map.keySet()}, so a {@code remove()}/{@code clear()} on the returned set structurally modified the record
   * behind the back of {@link MutableDocument#remove(String)} - no {@code dirty} flag, no validation - while the
   * read-only sibling {@code ImmutableDocument.getPropertyNames()} returned a snapshot. Same {@code Document} call,
   * two incompatible contracts.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PropertyNamesSnapshot extends TestHelper {

    @Test
    void mutableDocumentPropertyNamesCannotBeMutated() {
      database.getSchema().createDocumentType("Doc6818");

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc6818").set("a", 1).set("b", 2);

        final Set<String> names = doc.getPropertyNames();
        assertThatThrownBy(names::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> names.remove("a")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> names.add("c")).isInstanceOf(UnsupportedOperationException.class);

        assertThat(doc.has("a")).isTrue();
        assertThat(doc.has("b")).isTrue();
      });
    }

    @Test
    void propertyNamesAreASnapshotSoIterateAndPruneWorks() {
      database.getSchema().createDocumentType("Doc6818b");

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc6818b").set("keep", 1).set("tmp1", 2).set("tmp2", 3);

        // The natural iterate-and-prune loop: it used to throw ConcurrentModificationException.
        for (final String p : doc.getPropertyNames())
          if (p.startsWith("tmp"))
            doc.remove(p);

        assertThat(doc.getPropertyNames()).containsExactly("keep");
      });
    }

    @Test
    void snapshotDoesNotSeeLaterChanges() {
      database.getSchema().createDocumentType("Doc6818c");

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc6818c").set("a", 1);

        final Set<String> names = doc.getPropertyNames();
        doc.set("b", 2);

        assertThat(names).containsExactly("a");
        assertThat(doc.getPropertyNames()).containsExactly("a", "b");
      });
    }

    @Test
    void propertyNamesKeepInsertionOrder() {
      database.getSchema().createDocumentType("Doc6818d");

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Doc6818d").set("z", 1).set("m", 2).set("a", 3);
        assertThat(doc.getPropertyNames()).containsExactly("z", "m", "a");
      });
    }

    /**
     * The most common path of the three: a plain {@code select} result is an {@link ImmutableDocument} until
     * {@code modify()} or {@code detach()} is called. It was already a snapshot - the names are decoded out of the
     * serialized buffer - but the serializer hands back a plain {@code LinkedHashSet}, so without the wrapper a
     * {@code remove()} silently succeeded against a copy the caller could believe was the record.
     */
    @Test
    void immutableDocumentPropertyNamesCannotBeMutatedEither() {
      database.getSchema().createDocumentType("Doc6818f");

      database.transaction(() -> database.newDocument("Doc6818f").set("a", 1).set("b", 2).save());

      database.transaction(() -> {
        final Document immutable = database.query("sql", "select from Doc6818f").next().getElement().get();
        assertThat(immutable).isInstanceOf(ImmutableDocument.class);

        final Set<String> names = immutable.getPropertyNames();
        assertThat(names).containsExactly("a", "b");
        assertThatThrownBy(names::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> names.remove("a")).isInstanceOf(UnsupportedOperationException.class);

        assertThat(immutable.getPropertyNames()).containsExactly("a", "b");
      });
    }

    @Test
    void detachedDocumentPropertyNamesCannotBeMutatedEither() {
      database.getSchema().createDocumentType("Doc6818e");

      database.transaction(() -> database.newDocument("Doc6818e").set("a", 1).set("b", 2).save());

      final Document detached = database.query("sql", "select from Doc6818e").next().getElement().get().detach();

      final Set<String> names = detached.getPropertyNames();
      assertThat(names).containsExactly("a", "b");
      assertThatThrownBy(names::clear).isInstanceOf(UnsupportedOperationException.class);
      assertThat(detached.has("a")).isTrue();
    }
  }

  /**
   * Issue #8613: {@code MutableDocument.newEmbeddedDocument(type, property)} on a property the schema does not declare
   * stores the first embedded document as a single value, so the second call replaces it. That behavior
   * is a documented contract, not a bug (the scratch-property idiom relies on it), and a declared {@code LIST}
   * or a caller-created collection is the way to keep several embedded documents.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NewEmbeddedDocumentOverwrite extends TestHelper {

    @Test
    void secondEmbeddedDocumentOnAnUndeclaredPropertyReplacesTheFirstAsDocumented() {
      database.getSchema().createDocumentType("Item8613");
      database.getSchema().createDocumentType("Undeclared8613");

      database.transaction(() -> {
        final MutableDocument d = database.newDocument("Undeclared8613");
        d.newEmbeddedDocument("Item8613", "items").set("n", 1);
        d.newEmbeddedDocument("Item8613", "items").set("n", 2);

        // single-value contract, pinned: only the last one is kept, and it is not a collection
        assertThat(d.get("items")).isInstanceOf(EmbeddedDocument.class);
        assertThat(((EmbeddedDocument) d.get("items")).getInteger("n")).isEqualTo(2);
      });
    }

    @Test
    void collectionCreatedByTheCallerStillReceivesEveryEmbeddedDocument() {
      database.getSchema().createDocumentType("Item8613b");
      database.getSchema().createDocumentType("Holder8613b");

      database.transaction(() -> {
        final MutableDocument d = database.newDocument("Holder8613b").set("items", new ArrayList<>());
        d.newEmbeddedDocument("Item8613b", "items").set("n", 1);
        d.newEmbeddedDocument("Item8613b", "items").set("n", 2);
        assertThat(d.getList("items")).hasSize(2);
      });
    }

    @Test
    void declaredListStillAppends() {
      database.getSchema().createDocumentType("Item8613c");
      final DocumentType holder = database.getSchema().createDocumentType("Holder8613c");
      holder.createProperty("items", Type.LIST);

      database.transaction(() -> {
        final MutableDocument d = database.newDocument("Holder8613c");
        d.newEmbeddedDocument("Item8613c", "items").set("n", 1);
        d.newEmbeddedDocument("Item8613c", "items").set("n", 2);
        assertThat((List<?>) d.getList("items")).hasSize(2);
      });
    }

    @Test
    void declaredEmbeddedPropertyKeepsReplacingItsSingleValue() {
      database.getSchema().createDocumentType("Item8613d");
      final DocumentType holder = database.getSchema().createDocumentType("Holder8613d");
      holder.createProperty("item", Type.EMBEDDED).setOfType("Item8613d");

      database.transaction(() -> {
        final MutableDocument d = database.newDocument("Holder8613d");
        d.newEmbeddedDocument("Item8613d", "item").set("n", 1);
        d.newEmbeddedDocument("Item8613d", "item").set("n", 2);
        assertThat(((EmbeddedDocument) d.get("item")).getInteger("n")).isEqualTo(2);
      });
    }
  }

  /**
   * Issue #8712: the map-keyed {@code newEmbeddedDocument} overloads trusted the stored container, so a property set to
   * an immutable {@code Map.of(...)} threw {@code UnsupportedOperationException} where the collection arm heals (#7777).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NewEmbeddedDocumentImmutableMap extends TestHelper {

    @Test
    void stringKeyedOverloadHealsAnImmutableMap() {
      database.getSchema().createDocumentType("Item8712");
      database.getSchema().createDocumentType("Holder8712");

      database.transaction(() -> {
        final MutableDocument seed = database.newDocument("Item8712").set("n", 0);
        final MutableDocument d = database.newDocument("Holder8712");
        d.set("items", Map.of("a", seed));
        d.newEmbeddedDocument("Item8712", "items", "b").set("n", 2);

        final Map<String, Object> items = d.getMap("items");
        assertThat(items).containsOnlyKeys("a", "b");
        assertThat(((EmbeddedDocument) items.get("b")).getInteger("n")).isEqualTo(2);
      });
    }

    @Test
    void objectKeyedOverloadHealsAnImmutableMap() {
      database.getSchema().createDocumentType("Item8712b");
      database.getSchema().createDocumentType("Holder8712b");

      database.transaction(() -> {
        final MutableDocument seed = database.newDocument("Item8712b").set("n", 0);
        final MutableDocument d = database.newDocument("Holder8712b");
        d.set("items", Map.of("a", seed));
        d.newEmbeddedDocument("Item8712b", "items", (Object) "b").set("n", 2);

        assertThat(d.getMap("items")).containsOnlyKeys("a", "b");
      });
    }

    @Test
    void declaredMapPropertyKeepsTheReturnedEmbeddedInstanceStored() {
      database.getSchema().createDocumentType("Item8712c");
      database.getSchema().createDocumentType("Holder8712c").createProperty("items", Type.MAP);

      database.transaction(() -> {
        final MutableDocument seed = database.newDocument("Item8712c").set("n", 0);
        final MutableDocument d = database.newDocument("Holder8712c");
        d.set("items", Map.of("a", seed));
        final MutableEmbeddedDocument emb = d.newEmbeddedDocument("Item8712c", "items", "b");
        emb.set("n", 2);

        // the instance handed back must be the one stored, or later writes to it are lost
        assertThat(d.getMap("items").get("b")).isSameAs(emb);
      });
    }
  }

  /**
   * Issue #9799: a reserved metadata name ({@code @rid}, {@code @type}, {@code @cat}, ...) is never stored as a property,
   * which no reader could return: the positional setters refuse it, the map-based ones (fed {@code toMap(true)} output
   * by design, {@code merge(Map)} with them) skip it. On an edge, {@code @in} and {@code @out} re-point the edge through
   * every setter, not only through {@code set(String, Object)}. Any other {@code @}-prefixed name stays a property.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReservedPropertyNames extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("D1");
        database.getSchema().createVertexType("V1");
        database.getSchema().createEdgeType("E1");
      });
    }

    @Test
    void positionalSettersRefuseMetadataNamesOnADocument() {
      database.transaction(() -> {
        final MutableDocument d = database.newDocument("D1");
        assertThatThrownBy(() -> d.set("@type", "HACKED")).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("@type");
        assertThatThrownBy(() -> d.set("a", 1, "@type", "HACKED")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> d.set("a", 1, "b", 2, "@rid", "#1:1")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> d.set(new Object[] { "a", 1, "@cat", 2 })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> d.set("@props", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> d.set("@this", "x")).isInstanceOf(IllegalArgumentException.class);

        // A name that is not one: refused with a message rather than a NullPointerException/ClassCastException
        assertThatThrownBy(() -> d.set((String) null, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> d.set(new Object[] { "a", 1, 2, 3 })).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be a String");

        // A refused call assigns nothing, not even the names that precede the reserved one
        assertThat(d.getPropertyNames()).isEmpty();
        assertThat(d.isDirty()).isFalse();

        final Map<String, Object> nullKey = new HashMap<>();
        nullKey.put(null, 1);
        assertThatThrownBy(() -> d.set(nullKey)).isInstanceOf(IllegalArgumentException.class);

        d.set("q", 1);
        d.save();
      });

      database.transaction(() -> {
        final Document reloaded = database.iterateType("D1", false).next().asDocument();
        assertThat(reloaded.getPropertyNames()).containsExactly("q");
        assertThat(reloaded.toMap(false)).containsOnlyKeys("q");
      });
    }

    @Test
    void aNonReservedAtNameStaysAReadableProperty() {
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        rids[0] = database.newDocument("D1").set("@special", "v").save().getIdentity();

        final MutableVertex v1 = database.newVertex("V1").save();
        final MutableVertex v2 = database.newVertex("V1").save();
        final MutableVertex v3 = database.newVertex("V1").save();
        final MutableEdge e = v1.newEdge("E1", v2).set("@special", "e").save();
        // The move rebuilds the record from its properties: an `@special` one must come along
        e.set("@in", v3.getIdentity()).save();
        rids[1] = e.getIdentity();
      });

      database.transaction(() -> {
        final Document doc = rids[0].asDocument();
        assertThat(doc.get("@special")).isEqualTo("v");
        assertThat(doc.toJSON(true).getString("@special")).isEqualTo("v");
        assertThat(rids[1].asEdge().get("@special")).isEqualTo("e");
      });
    }

    @Test
    void mapSettersSkipMetadataNamesOnADocument() {
      database.transaction(() -> {
        final MutableDocument d = database.newDocument("D1");
        final Map<String, Object> props = new LinkedHashMap<>();
        props.put("@type", "HACKED");
        props.put("@foo", "skipped");
        props.put("q", 1);
        d.set(props);
        d.save();

        final MutableDocument d2 = database.newDocument("D1");
        d2.fromMap(props);
        d2.save();

        final MutableDocument d4 = database.newDocument("D1");
        d4.merge(props);
        d4.save();

        // The round trip that motivates the skip: a record's own metadata-bearing map
        final MutableDocument d3 = database.newDocument("D1");
        d3.fromMap(d.toMap(true));
        d3.save();
      });

      database.transaction(() -> {
        database.iterateType("D1", false).forEachRemaining(r -> assertThat(r.asDocument().getPropertyNames()).containsExactly("q"));
      });
    }

    @Test
    void sqlUpdateOfAMetadataNameFailsInsteadOfReportingAWrite() {
      database.transaction(() -> database.newDocument("D1").set("q", 1).save());

      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE D1 SET `@type` = 'HACKED'")))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("@type");

      database.transaction(() -> {
        final Document reloaded = database.iterateType("D1", false).next().asDocument();
        assertThat(reloaded.getPropertyNames()).containsExactly("q");
        try (final ResultSet rs = database.query("sql", "SELECT `@type` AS t FROM D1")) {
          assertThat((String) rs.next().getProperty("t")).isEqualTo("D1");
        }
      });
    }

    @Test
    void everyEdgeSetterRePointsTheInEndpoint() {
      assertEdgeSetterRePoints((e, target) -> e.set("@in", target));
      assertEdgeSetterRePoints((e, target) -> e.set("@in", target, "k", "x"));
      assertEdgeSetterRePoints((e, target) -> e.set("k", "x", "@in", target));
      assertEdgeSetterRePoints((e, target) -> e.set("k", "x", "j", 1, "@in", target));
      assertEdgeSetterRePoints((e, target) -> e.set(new Object[] { "@in", target, "k", "x" }));
      assertEdgeSetterRePoints((e, target) -> e.set(mapOf("@in", target, "k", "x")));
      assertEdgeSetterRePoints((e, target) -> e.merge(mapOf("@in", target, "k", "x")));
      assertEdgeSetterRePoints((e, target) -> e.fromMap(mapOf("@in", target, "k", "x")));
      // The textual form a JSON payload carries
      assertEdgeSetterRePoints((e, target) -> e.fromMap(mapOf("@in", target.toString(), "k", "x")));
    }

    @Test
    void everyEdgeSetterRePointsTheOutEndpoint() {
      final RID[] rids = new RID[3];
      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("V1").set("n", 1).save();
        final MutableVertex v2 = database.newVertex("V1").set("n", 2).save();
        final MutableVertex v3 = database.newVertex("V1").set("n", 3).save();
        rids[0] = v3.getIdentity();
        rids[1] = v2.getIdentity();
        final MutableEdge e = v1.newEdge("E1", v2).save();
        e.set("k", "x", "@out", v3.getIdentity());
        e.save();
        rids[2] = e.getIdentity();
      });

      database.transaction(() -> {
        final Edge reloaded = rids[2].asEdge();
        assertThat(reloaded.getOut()).isEqualTo(rids[0]);
        assertThat(reloaded.getIn()).isEqualTo(rids[1]);
        assertThat(reloaded.getPropertyNames()).containsExactly("k");
        assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(1);
      });
    }

    @Test
    void oneMapMovingBothEndpointsLeavesOneEdge() {
      final RID[] rids = new RID[5];
      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("V1").save();
        final MutableVertex v2 = database.newVertex("V1").save();
        final MutableVertex v3 = database.newVertex("V1").save();
        final MutableVertex v4 = database.newVertex("V1").save();
        final MutableEdge e = v1.newEdge("E1", v2).set("k", "x").save();
        final Map<String, Object> both = new LinkedHashMap<>();
        both.put("@in", v3.getIdentity());
        both.put("@out", v4.getIdentity());
        e.set(both);
        e.save();
        rids[0] = v1.getIdentity();
        rids[1] = v2.getIdentity();
        rids[2] = v3.getIdentity();
        rids[3] = v4.getIdentity();
        rids[4] = e.getIdentity();
      });

      database.transaction(() -> {
        final Edge reloaded = rids[4].asEdge();
        assertThat(reloaded.getOut()).isEqualTo(rids[3]);
        assertThat(reloaded.getIn()).isEqualTo(rids[2]);
        assertThat(reloaded.get("k")).isEqualTo("x");
        assertThat(database.countType("E1", false)).isEqualTo(1L);
        assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isZero();
        assertThat(rids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E1")).isZero();
        assertThat(rids[3].asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(1);
        assertThat(rids[2].asVertex().countEdges(Vertex.DIRECTION.IN, "E1")).isEqualTo(1);
      });
    }

    @Test
    void edgeRefusesOtherMetadataNamesAndSkipsThemInMaps() {
      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("V1").save();
        final MutableVertex v2 = database.newVertex("V1").save();
        final MutableEdge e = v1.newEdge("E1", v2).save();
        assertThatThrownBy(() -> e.set("@type", "X")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> e.set("k", "x", "@rid", "#1:1")).isInstanceOf(IllegalArgumentException.class);
        assertThat(e.has("k")).isFalse();

        // toMap(true) carries the edge's own endpoints and metadata: feeding it back is a no-op on the endpoints
        e.set("k", "x");
        e.fromMap(e.toMap(true));
        e.save();
        assertThat(e.getIn()).isEqualTo(v2.getIdentity());
        assertThat(e.getPropertyNames()).containsExactly("k");
      });
    }

    private void assertEdgeSetterRePoints(final BiConsumer<MutableEdge, RID> setter) {
      final RID[] rids = new RID[4];
      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("V1").set("n", 1).save();
        final MutableVertex v2 = database.newVertex("V1").set("n", 2).save();
        final MutableVertex v3 = database.newVertex("V1").set("n", 3).save();
        final MutableEdge e = v1.newEdge("E1", v2).save();
        setter.accept(e, v3.getIdentity());
        e.save();
        rids[0] = v1.getIdentity();
        rids[1] = v2.getIdentity();
        rids[2] = v3.getIdentity();
        rids[3] = e.getIdentity();
      });

      database.transaction(() -> {
        final Edge reloaded = rids[3].asEdge();
        assertThat(reloaded.getOut()).isEqualTo(rids[0]);
        assertThat(reloaded.getIn()).isEqualTo(rids[2]);
        assertThat(reloaded.get("@in")).isEqualTo(rids[2]);
        assertThat(reloaded.getPropertyNames()).doesNotContain("@in");
        assertThat(reloaded.toMap(false)).doesNotContainKey("@in");
        assertThat(rids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E1")).isZero();
        assertThat(rids[2].asVertex().countEdges(Vertex.DIRECTION.IN, "E1")).isEqualTo(1);
      });
    }

    private static Map<String, Object> mapOf(final String k1, final Object v1, final String k2, final Object v2) {
      final Map<String, Object> map = new HashMap<>();
      map.put(k1, v1);
      map.put(k2, v2);
      return map;
    }
  }
}
