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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Follow-up of #10944 (the flaky "Cannot find record type '120'" of {@code Issue10944ScanSkipsDeletedChunkedRecordTest}): a read
 * of a multi-page record that has to retry re-reads the head slot from the newest committed page. When the record was deleted
 * under the read and its slot taken by a CONTINUATION chunk of another record, the retry walked that chunk as if it were the
 * head and handed out the other record's content bytes as a record of their own.
 */
class Issue10944ChunkHeadSlotReusedDuringReadTest extends TestHelper {
  private static final String TYPE         = "Chunked";
  private static final int    PAYLOAD_SIZE = 200_000; // SEVERAL 64KB PAGES: STORED AS A CHUNK CHAIN

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void aRetriedReadDoesNotWalkAContinuationChunkTakenIntoTheHeadSlot() throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("payload", Type.STRING));

    final RID[] small = new RID[1];
    final RID[] deleted = new RID[1];
    final List<RID> growing = new ArrayList<>();
    database.transaction(() -> {
      // POSITION 0 OF THE BUCKET IS NEVER A CONTINUATION CHUNK (#10701): THE RECORD TO DELETE TAKES A LATER SLOT
      database.newDocument(TYPE).set("payload", "first").save();
      final MutableDocument s = database.newDocument(TYPE).set("payload", "s");
      s.save();
      small[0] = s.getIdentity();
      final MutableDocument r = database.newDocument(TYPE).set("payload", "r");
      r.save();
      deleted[0] = r.getIdentity();
    });
    // GROWN IN PLACE, THE LAST RECORD OF THE PAGE KEEPS ITS SLOT AND ITS HEAD CHUNK TAKES THE WHOLE FREE TAIL OF THE PAGE
    database.transaction(() -> deleted[0].asDocument().modify().set("payload", "r".repeat(PAYLOAD_SIZE)).save());
    database.transaction(() -> {
      for (int i = 0; i < 4; i++) {
        final MutableDocument x = database.newDocument(TYPE).set("payload", "x".repeat(PAYLOAD_SIZE));
        x.save();
        growing.add(x.getIdentity());
      }
    });

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(deleted[0].getBucketId());
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();
    assertThat(small[0].getPosition() / maxRecordsInPage).as("small record %s and chunked record %s share a page", small[0],
        deleted[0]).isEqualTo(deleted[0].getPosition() / maxRecordsInPage);
    assertThat(committedSlotMarker(bucket, deleted[0])).isEqualTo(LocalBucket.FIRST_CHUNK);

    database.begin();
    try {
      // THE TRANSACTION NOW HOLDS THE HEAD PAGE: ITS READ STARTS FROM THIS IMAGE, OLDER THAN WHAT THE OTHER THREAD COMMITS
      small[0].asDocument().modify().set("payload", "t").save();

      final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          database.transaction(() -> database.deleteRecord(deleted[0].asDocument()));
          // THE GROWTH OF THE OTHER RECORDS PUTS A NEW CONTINUATION CHUNK IN THE ROOM, AND THE SLOT, THE DELETE FREED
          database.transaction(() -> {
            for (final RID rid : growing)
              rid.asDocument().modify().set("payload", "x".repeat(PAYLOAD_SIZE + 70_000)).save();
            for (int i = 0; i < 4; i++)
              database.newDocument(TYPE).set("payload", "x".repeat(PAYLOAD_SIZE)).save();
          });
        } catch (final Throwable e) {
          writerFailure.set(e);
        }
      });
      writer.start();
      writer.join();
      assertThat(writerFailure.get()).isNull();
      // THE SHAPE THE TEST IS ABOUT: THE HEAD SLOT OF THE DELETED RECORD NOW HOLDS ANOTHER RECORD'S CONTINUATION CHUNK
      assertThat(committedSlotMarker(bucket, deleted[0])).isEqualTo(LocalBucket.NEXT_CHUNK);

      // ONLY THE DELETED RECORD IS READ: THE TRANSACTION HOLDS AN IMAGE OF THE PAGE THAT PREDATES THE GROWTH OF THE OTHER
      // RECORDS, WHOSE CHUNKS ALSO LIVE THERE, SO READING THOSE IS A GENUINE CONFLICT THAT NO RETRY RESOLVES
      assertThatThrownBy(() -> database.lookupByRID(deleted[0], true)).isInstanceOf(RecordNotFoundException.class);
    } finally {
      database.rollback();
    }
  }

  private long committedSlotMarker(final LocalBucket bucket, final RID rid) throws IOException {
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();
    final DatabaseInternal db = (DatabaseInternal) database;
    final BasePage page = db.getPageManager()
        .getImmutablePage(new PageId(db, bucket.getFileId(), (int) (rid.getPosition() / maxRecordsInPage)), bucket.getPageSize(), false,
            true);
    final int recordPositionInPage = (int) page.readUnsignedInt(
        LocalBucket.PAGE_RECORD_TABLE_OFFSET + (int) (rid.getPosition() % maxRecordsInPage) * Binary.INT_SERIALIZED_SIZE);
    return recordPositionInPage == 0 ? 0 : page.readNumber(recordPositionInPage);
  }
}
