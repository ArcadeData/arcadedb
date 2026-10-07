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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9306: an interrupt landing on ANY thread doing I/O on a data file closes that file's channel for
 * every other thread (ClosedByInterruptException). The two readers of the point-in-time snapshot - the pre-image capture
 * a commit runs before overwriting a page, and the snapshot's own input stream - used to answer that closed channel as
 * a hard failure: the capture invalidated every open window, and the stream killed the transfer mid-archive. Both now
 * reopen the channel and retry, so the window stays ACTIVE and keeps serving its t0 image.
 */
class Issue9306SnapshotSurvivesInterruptClosedChannelTest extends TestHelper {

  private static final String TYPE = "Doc";

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("id", Integer.class);
    type.createProperty("payload", String.class);

    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument(TYPE).set("id", i).set("payload", "initial").save();
    });
  }

  @Test
  void preImageCaptureReopensAChannelAnInterruptClosedInsteadOfInvalidatingTheWindow() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      final Map<Integer, Long> t0Checksums = checksums(snapshot);

      // THE FLUSH IS HELD BACK WHILE THE REWRITE COMMITS, SO THE CHANNELS CAN BE CLOSED AFTER THE COMMIT READ ITS PAGES
      // (A READ REOPENS THE CHANNEL ON ITS OWN) AND BEFORE THE FLUSH CAPTURES THE PRE-IMAGES OF THE PAGES IT OVERWRITES
      pageManager.suspendFlushAndExecute(db, () -> {
        database.transaction(() -> database.iterateType(TYPE, false).forEachRemaining(record -> {
          final MutableDocument doc = record.asDocument().modify();
          // THE SAME LENGTH AS "initial": EVERY RECORD IS REWRITTEN IN PLACE, SO EVERY PAGE THE FLUSH WRITES EXISTED AT t0
          // AND NEEDS A PRE-IMAGE. A GROWING RECORD WOULD ALSO WRITE A NEW PAGE, AND IF THAT ONE WERE FLUSHED FIRST ITS
          // OWN WRITE WOULD REOPEN THE CHANNEL BEFORE ANY CAPTURE RAN
          doc.set("payload", "INITIAL");
          doc.save();
        }));
        // ANOTHER THREAD'S INTERRUPT CLOSED EVERY CHANNEL: THE FLUSH CAPTURES ITS PRE-IMAGES THROUGH A CLOSED CHANNEL
        closeEveryChannel(snapshot);
      });
      pageManager.waitAllPagesOfDatabaseAreFlushed(db);

      assertThat(snapshot.getShadowedPages()).as("the rewrite must have captured pre-images, or this test proves nothing")
          .isPositive();
      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
      assertThat(checksums(snapshot)).isEqualTo(t0Checksums);
    }
  }

  @Test
  void snapshotStreamReopensAChannelAnInterruptClosedInsteadOfFailingTheTransfer() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
      final Map<Integer, Long> t0Checksums = checksums(snapshot);

      closeEveryChannel(snapshot);

      // EVERY FILE IS READ BACK THROUGH THE SNAPSHOT'S INPUT STREAM, EACH ONE STARTING ON A CLOSED CHANNEL
      assertThat(checksums(snapshot)).isEqualTo(t0Checksums);
      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
    }
  }

  private static Map<Integer, Long> checksums(final PageSnapshot snapshot) throws IOException {
    final Map<Integer, Long> result = new HashMap<>();
    for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
      result.put(file.fileId(), snapshot.calculateChecksum(file.fileId()));
    return result;
  }

  /** What a ClosedByInterruptException does to the file: the channel is closed, while the file itself stays open. */
  private static void closeEveryChannel(final PageSnapshot snapshot) throws ReflectiveOperationException, IOException {
    final Field field = PaginatedComponentFile.class.getDeclaredField("channel");
    field.setAccessible(true);
    for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
      ((FileChannel) field.get(file.file())).close();
  }
}
