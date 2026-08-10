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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.schema.DocumentType;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Follow-up on #6015's code review: {@code BucketIterator.getSkippedRecordCount()} was added but had no
 * production consumer, which both review rounds flagged as a risk of becoming permanently-dead API. Wires it
 * into the two SQL scan steps that hold a raw {@code BucketIterator} - {@link FetchFromClusterExecutionStep}
 * (a plain, unfiltered scan) and {@link ScanWithFilterStep} (a scan fused with a {@code WHERE} clause) - so a
 * corrupted/skipped record surfaces as a one-time WARNING log instead of silently returning fewer rows than the
 * bucket actually has.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BucketIteratorSkippedRecordWarningTest extends TestHelper {

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
    return false;
  }

  @Test
  void plainTypeScanWarnsOnSkippedCorruptedRecord() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("SkipWarnPlainScan", 1);

    database.begin();
    database.newDocument("SkipWarnPlainScan").set("id", 1).save();
    database.newDocument("SkipWarnPlainScan").set("id", 2).save();
    database.commit();

    corruptSecondSlot(type);

    // The Logger local variable MUST stay alive (a strong reference held by this method) for as long as the
    // handler needs to be attached: java.util.logging.LogManager holds registered loggers via WeakReference, so
    // a Logger obtained and discarded inside a helper method can be garbage-collected - taking its handler with
    // it - before the query underneath ever logs anything, making this assertion flake open-endedly.
    final LogCapture capture = attach(FetchFromClusterExecutionStep.class);
    try {
      final ResultSet result = database.query("sql", "SELECT FROM SkipWarnPlainScan");
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).as("the healthy record must still be returned, the scan must not abort").isEqualTo(1);
    } finally {
      detach(capture);
    }

    assertThat(capture.handler.snapshot().stream().anyMatch(m -> m.contains("skipped") && m.contains("could not be read")))
        .as("the scan must warn that a record was skipped instead of silently returning fewer rows; got: %s",
            capture.handler.snapshot())
        .isTrue();
  }

  @Test
  void filteredTypeScanWarnsOnSkippedCorruptedRecord() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("SkipWarnFilteredScan", 1);

    database.begin();
    database.newDocument("SkipWarnFilteredScan").set("id", 1).save();
    database.newDocument("SkipWarnFilteredScan").set("id", 2).save();
    database.commit();

    corruptSecondSlot(type);

    final LogCapture capture = attach(ScanWithFilterStep.class);
    try {
      final ResultSet result = database.query("sql", "SELECT FROM SkipWarnFilteredScan WHERE id >= 0");
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).as("the healthy record must still be returned, the scan must not abort").isEqualTo(1);
    } finally {
      detach(capture);
    }

    assertThat(capture.handler.snapshot().stream().anyMatch(m -> m.contains("skipped") && m.contains("could not be read")))
        .as("the scan must warn that a record was skipped instead of silently returning fewer rows; got: %s",
            capture.handler.snapshot())
        .isTrue();
  }

  /**
   * Corrupts the second record's slot-table entry the same way {@code BucketIteratorCorruptedSlotTest} does:
   * point it at a wildly out-of-range page position, so {@code BasePage.readNumberAndSize()} throws and
   * {@code BucketIterator} counts it as a skipped record instead of returning it.
   */
  private void corruptSecondSlot(final DocumentType type) throws Exception {
    final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
    assertThat(bucket.getTotalPages()).as("both records must fit on a single page").isEqualTo(1);

    database.begin();
    final MutablePage page = ((DatabaseInternal) database).getTransaction()
        .getPageToModify(new PageId((DatabaseInternal) database, bucket.getFileId(), 0), bucket.getPageSize(), false);
    // PAGE_RECORD_TABLE_OFFSET == PAGE_RECORD_COUNT_IN_PAGE_OFFSET(0) + SHORT_SERIALIZED_SIZE; slot 1 (the second
    // record) sits INT_SERIALIZED_SIZE bytes further in. Mirrors BucketIteratorCorruptedSlotTest/
    // BrokenMultiPageRecordDeleteTest's technique, adapted for this package's lack of protected-field access.
    page.writeUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE, (long) bucket.getPageSize() * 1000);
    database.commit();
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
   * Bundles the {@link Logger} together with its {@link CapturingHandler} so the caller keeps a strong reference
   * to the logger for the whole capture window - see the comment where this is used.
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
