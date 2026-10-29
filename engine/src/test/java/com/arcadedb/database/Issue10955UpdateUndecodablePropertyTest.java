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
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #10955: an UPDATE of a record one of whose properties cannot be decoded must not save it back without that property,
 * which turns a read-time corruption into permanent data loss. The update is refused with an error naming the record and
 * the property, and the stored bytes are left untouched.
 */
class Issue10955UpdateUndecodablePropertyTest extends TestHelper {
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

    final int bodyId = ((DatabaseInternal) database).getSchema().getDictionary().getIdByName("body", false);
    onRecordPage(rid[0], (page, recordOffset) -> {
      final Binary record = ((LocalBucket) ((DatabaseInternal) database).getSchema().getBucketById(rid[0].getBucketId())).getRecord(rid[0]);
      final int[] slotByNameId = new int[bodyId + 1];
      Arrays.fill(slotByNameId, -1);
      slotByNameId[bodyId] = 0;
      final int[] positions = new int[1];
      record.position(1);
      assertThat(((DatabaseInternal) database).getSerializer().locateProperties(record, slotByNameId, positions, 1)).isTrue();
      assertThat(positions[0]).isGreaterThan(0);
      final long[] recordSize = page.readNumberAndSize(recordOffset);
      page.writeByte((int) (recordOffset + recordSize[1] + positions[0]), UNKNOWN_TYPE);
    });

    // PRECONDITION: THE LENIENT READ NOW LOSES THE PROPERTY, SO THE TEST REALLY EXERCISES THE UNDECODABLE SHAPE
    final Document read = database.lookupByRID(rid[0], true).asDocument();
    assertThat(read.toMap(false)).doesNotContainKey("body");
    return rid[0];
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
