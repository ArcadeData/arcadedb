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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6399 (item 1): {@link PaginatedComponent}'s constructor derives its page count as {@code fileSize / pageSize},
 * which FLOORS. Nothing anywhere asked whether there was a remainder, so a file killed part way through writing a page
 * kept a partial tail that no component counted, no check reported and nothing repaired - simply rounded away at every
 * open, forever.
 * <p>
 * This is the natural third sibling of the file-id (#6283) and page-size (#6314) guards that live ten lines above it
 * in the same constructor, but unlike them it is NOT a tripwire: those two fire on states that are programming errors
 * and cannot exist in a file a correct build wrote, while a torn tail is exactly what a power cut produces, and a
 * database a previous build opened happily must keep opening. The fix is a WARNING logged at open instead, and for a
 * bucket or index (as opposed to the Dictionary, which #6355 fixed separately because it reads file content, not just
 * a count) the torn bytes cost nothing beyond disk space: the next page appended to the file lands at
 * {@code pageCount * pageSize} and overwrites them wholesale.
 * <p>
 * In package {@code com.arcadedb.engine} on purpose: reaching a component's {@link File}/{@link PaginatedComponentFile}
 * needs package access.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6399PartialTailPageTest extends TestHelper {

  @Test
  void aBucketFileWithATornTailPageWarnsButStillOpensAndStaysFullyUsable() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.getSchema().createDocumentType("Issue6399Type", 1);
    db.transaction(() -> db.newDocument("Issue6399Type").set("id", 1).save());

    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Issue6399Type").getBuckets(false).getFirst();
    final int pageSize = bucket.getPageSize();
    final int wholePages = bucket.getTotalPages();
    assertThat(wholePages).as("premise: at least the header page is committed").isPositive();

    final File bucketFile = bucket.getOSFile();
    final String path = database.getDatabasePath();

    assertThat(db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db)).isTrue();
    db.close();

    // TORN TAIL: a whole page plus a stub that is not one, exactly what a kill mid-write leaves behind.
    final long tornLength = (long) wholePages * pageSize + 37;
    try (final RandomAccessFile raw = new RandomAccessFile(bucketFile, "rw")) {
      raw.setLength(tornLength);
    }

    final LogCapture capture = attach(LocalBucket.class);
    final Database reopened = new DatabaseFactory(path).open();
    try {
      // THE WARNING FIRED AT OPEN, NOT A THROWN EXCEPTION: this is a report, not a tripwire.
      assertThat(capture.handler.snapshot().stream()
          .anyMatch(m -> m.contains("not a multiple of its page size") && m.contains("37")))
          .as("expected a WARNING about the torn tail; got: %s", capture.handler.snapshot())
          .isTrue();

      final DatabaseInternal reopenedInternal = (DatabaseInternal) reopened;
      final LocalBucket reopenedBucket =
          (LocalBucket) reopenedInternal.getSchema().getType("Issue6399Type").getBuckets(false).getFirst();

      // THE TORN BYTES WERE ROUNDED AWAY, NOT COUNTED AS AN EXTRA PAGE
      assertThat(reopenedBucket.getTotalPages()).isEqualTo(wholePages);

      // THE DATABASE IS FULLY USABLE: THE PRE-EXISTING RECORD SURVIVED AND MORE CAN STILL BE WRITTEN
      assertThat(reopenedInternal.countType("Issue6399Type", false)).isEqualTo(1);

      // ENOUGH RECORDS TO FORCE A REAL PAGE 1 TO BE ALLOCATED AND WRITTEN - THE SAME PAGE NUMBER THE TORN TAIL
      // STUBBED. ITS WRITE LANDS AT `pageNumber * pageSize` REGARDLESS OF WHAT WAS THERE (PaginatedComponentFile#write),
      // SO IT OVERWRITES THE TORN BYTES WHOLESALE RATHER THAN READING THEM: THE FIX'S WARNING PROMISES EXACTLY THIS.
      reopenedInternal.transaction(() -> {
        for (int i = 0; i < 5_000; i++)
          reopenedInternal.newDocument("Issue6399Type").set("id", i, "payload", "x".repeat(100)).save();
      });
      assertThat(reopenedInternal.getPageManager().waitAllPagesOfDatabaseAreFlushed(reopenedInternal)).isTrue();

      assertThat(reopenedBucket.getTotalPages()).as("a real page 1 must have been allocated").isGreaterThan(wholePages);
      assertThat(reopenedBucket.getComponentFile().getSize() % pageSize)
          .as("the torn stub must be gone, overwritten by the real page 1").isZero();
    } finally {
      detach(capture);
      reopened.drop();
    }
  }

  private static LogCapture attach(final Class<?> loggerOwner) {
    final CapturingHandler handler = new CapturingHandler();
    handler.setLevel(Level.ALL);
    final Logger logger = Logger.getLogger(loggerOwner.getName());
    logger.addHandler(handler);
    logger.setLevel(Level.ALL);
    return new LogCapture(logger, handler);
  }

  private static void detach(final LogCapture capture) {
    capture.logger.removeHandler(capture.handler);
  }

  /**
   * Bundles the {@link Logger} together with its {@link CapturingHandler} so the caller keeps a strong reference to
   * the logger for the whole capture window: {@code java.util.logging.LogManager} holds registered loggers via
   * {@code WeakReference}, so a logger obtained and discarded inside a helper method could be collected - taking its
   * handler with it - before the code under test ever logs anything.
   */
  private record LogCapture(Logger logger, CapturingHandler handler) {
  }

  private static final class CapturingHandler extends Handler {
    private final List<String> records = new CopyOnWriteArrayList<>();

    @Override
    public void publish(final LogRecord record) {
      if (record == null || record.getLevel().intValue() < Level.WARNING.intValue())
        return;
      String msg = record.getMessage();
      if (msg != null && record.getParameters() != null && record.getParameters().length > 0) {
        try {
          msg = msg.formatted(record.getParameters());
        } catch (final Exception ignored) {
        }
      }
      if (msg != null)
        records.add(msg);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }

    List<String> snapshot() {
      return records;
    }
  }
}
