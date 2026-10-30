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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Final review A-C1: an apply pass draining the first columnar mark of a commit before its pages are published, and so before the page
 * counts of the pages it allocated are raised. The pass must not complete a page it cannot see yet as an empty page: it retries it, and
 * the commit's second mark, once the pages and their counts are visible, brings the rows in.
 */
class ColumnarCommitVisibilityTest extends TestHelper {
  /** Two of these fill a 64KB page: the third one allocates the next page. */
  private static final int RECORD_BYTES = 25_000;

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  @Override
  protected void endTest() {
    TransactionContext.afterFirstColumnarMarkForTesting = null;
    executor.shutdownNow();
  }

  @Test
  void aPassBeforeTheNewPrimaryPageIsVisibleRetriesThePage() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    final int bucketId = bucketId("Asset");
    final ColumnarProjection projection = manager().projection(bucketId);
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(bucketId);
    final String blob = "x".repeat(RECORD_BYTES);
    database.transaction(() -> {
      for (int i = 0; i < 2; i++)
        database.newDocument("Asset").set("brand", "a").set("blob", blob).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final int pagesBefore = bucket.getCommittedPageCount();

    final AtomicInteger retriesAfterPass = new AtomicInteger(-1);
    final AtomicReference<Throwable> passFailure = new AtomicReference<>();
    try (final AutoCloseable hold = manager().suspendApply()) {
      TransactionContext.afterFirstColumnarMarkForTesting = () -> {
        TransactionContext.afterFirstColumnarMarkForTesting = null;
        passOnAnotherThread(projection, passFailure);
        retriesAfterPass.set(projection.unreadablePages().sizeForTesting());
      };
      database.transaction(() -> database.newDocument("Asset").set("brand", "b").set("blob", blob).save());
    }

    assertThat(passFailure.get()).isNull();
    assertThat(bucket.getCommittedPageCount()).as("the fixture must allocate a primary page").isEqualTo(pagesBefore + 1);
    assertThat(retriesAfterPass).as("a page past the committed count is retried, not completed as an empty page").hasValue(1);
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);
    assertThat(FacetOracle.projectionCounts(database, "Asset", "brand", null, false)).containsEntry("b", 1L);
    assertThat(projection.unreadablePages().sizeForTesting()).as("the second mark ended the retry").isZero();
    assertThat(manager().getStats().unreadableCount("Asset")).as("a page not visible yet is no unreadable record").isZero();
  }

  @Test
  void aPassBeforeTheNewExternalPageIsVisibleDoesNotLoseTheRecord() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("Blob", 1);
    type.createProperty("payload", Type.STRING).setExternal(true);
    database.command("sql", "CREATE INDEX ON Blob (payload) COLUMNAR");
    final int bucketId = bucketId("Blob");
    final ColumnarProjection projection = manager().projection(bucketId);
    final LocalBucket external = (LocalBucket) database.getSchema()
        .getBucketById(((LocalDocumentType) type).getExternalBucketIdFor(bucketId));
    // two values fill an external page, whatever its size: the third one allocates the next page
    final int valueBytes = external.getPageSize() * 2 / 5;
    final Random random = new Random(42);
    final String[] values = new String[3];
    for (int i = 0; i < values.length; i++)
      values[i] = randomText(random, valueBytes);
    database.transaction(() -> {
      for (int i = 0; i < 2; i++)
        database.newDocument("Blob").set("payload", values[i]).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final int externalPagesBefore = external.getCommittedPageCount();

    final AtomicReference<Throwable> passFailure = new AtomicReference<>();
    try (final AutoCloseable hold = manager().suspendApply()) {
      TransactionContext.afterFirstColumnarMarkForTesting = () -> {
        TransactionContext.afterFirstColumnarMarkForTesting = null;
        passOnAnotherThread(projection, passFailure);
      };
      database.transaction(() -> database.newDocument("Blob").set("payload", values[2]).save());
    }

    assertThat(passFailure.get()).isNull();
    assertThat(external.getCommittedPageCount()).as("the fixture must allocate an external page").isEqualTo(externalPagesBefore + 1);
    FacetOracle.assertEquivalent(database, "Blob", "payload", null);
    assertThat(FacetOracle.projectionCounts(database, "Blob", "payload", null, false)).containsEntry(values[2], 1L);
    assertThat(projection.unreadablePages().sizeForTesting()).as("no retry is left behind").isZero();
  }

  /** Runs one apply pass of the projection on the executor while the committing thread waits for it. */
  private void passOnAnotherThread(final ColumnarProjection projection, final AtomicReference<Throwable> failure) {
    try {
      executor.submit(() -> ColumnarApplier.runPass(manager(), projection)).get(30, TimeUnit.SECONDS);
    } catch (final Exception e) {
      failure.set(e);
    }
  }

  /** Letters that do not compress, so a value takes its length on the page. */
  private static String randomText(final Random random, final int length) {
    final StringBuilder text = new StringBuilder(length);
    for (int i = 0; i < length; i++)
      text.append((char) ('a' + random.nextInt(26)));
    return text.toString();
  }

  private int bucketId(final String type) {
    return database.getSchema().getType(type).getBuckets(false).stream().mapToInt(Bucket::getFileId).findFirst().orElseThrow();
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }
}
