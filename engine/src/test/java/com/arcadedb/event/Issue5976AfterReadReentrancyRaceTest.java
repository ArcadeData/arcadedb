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
package com.arcadedb.event;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5976.
 * <p>
 * {@code RecordEncryptionTest.encryption()} intermittently threw {@code NoSuchElementException} out of
 * {@code MultiIterator.next()} on CI, immediately after a {@code REPEATABLE_READ} transaction's very first read of
 * a just-committed record. The real failure was hidden: {@code BucketIterator.fetchNext()} swallows any exception
 * raised while materializing a record (logs it at {@code SEVERE} and skips the slot), so a record that failed to
 * load looked identical to an empty bucket to the caller.
 * <p>
 * Root cause chain:
 * <ol>
 *   <li>{@code TransactionContext.getPage()} decided whether a page was "new" (and therefore not worth caching in
 *   {@code immutablePages} for the {@code REPEATABLE_READ} snapshot) using {@code PaginatedComponentFile.getTotalPages()}
 *   - the PHYSICAL on-disk file size. That lags a commit: {@code PageManager.writePages()} schedules the page for the
 *   async flush thread and only the in-memory {@code PaginatedComponent.pageCount} is bumped synchronously.</li>
 *   <li>A just-committed, not-yet-physically-flushed page was therefore treated as "new" and left out of
 *   {@code immutablePages}.</li>
 *   <li>{@code ImmutableVertex.modify()} uses {@code hasPageForRecord()} (which reads {@code immutablePages}) to
 *   decide whether the record needs a defensive {@code reload()}. Missing from the cache, it force-reloaded -
 *   re-invoking every {@code AfterRecordReadListener} on the same record a second time, re-entrantly, before the
 *   first invocation had returned.</li>
 *   <li>The encryption listener pattern - decrypting a record's ciphertext in place inside {@code onAfterRead()},
 *   which calls {@code record.asVertex().modify()} as its first step - is not safe against that re-entrant second
 *   call: the inner (nested) invocation decrypts the ciphertext first, so the outer invocation then tries to
 *   Base64-decode the now-plaintext value and throws {@code IllegalArgumentException}, which {@code BucketIterator}
 *   swallows.</li>
 * </ol>
 * The fix makes {@code TransactionContext.getPage()} use the component's own (synchronously updated) page count
 * instead of the physical file size, so a committed page is cached for {@code REPEATABLE_READ} as soon as it is
 * committed, regardless of async flush timing.
 * <p>
 * The race only shows up when the async flush thread has not yet caught up with a commit by the time the very next
 * transaction reads the same page, which is a narrow window under normal load - hence "intermittent, CI-only,
 * never reproduced locally" in the original report. This test recreates the exact shape (fresh single-page bucket,
 * one record, immediately read back under {@code REPEATABLE_READ}, in a listener that calls {@code modify()} on
 * itself) in a tight loop with the page cache forced to evict aggressively ({@code MAX_PAGE_RAM=0}), which widens
 * the window enough to fail on effectively every run before the fix (500+ failures out of 1000 iterations) and
 * zero after it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
public class Issue5976AfterReadReentrancyRaceTest extends TestHelper
    implements BeforeRecordCreateListener, AfterRecordReadListener, BeforeRecordUpdateListener {

  private static final String       PASSWORD      = "JustAPassword";
  private static final String       ALGORITHM     = "AES/CBC/PKCS5Padding";
  private static final String       SECRET        = "Nobody must know John and Zuck are brothers";
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final int          ITERATIONS    = 1500;

  private SecretKey key;

  @Test
  void insertThenRepeatableReadImmediatelyAfterNeverThrowsNoSuchElementException() throws Exception {
    key = RecordEncryptionTest.getKeyFromPassword(PASSWORD, "salt");

    // Force the read-cache to evict as aggressively as possible: this widens the window between a page being
    // committed (in-memory page count bumped synchronously) and it being physically flushed to disk (async, and
    // the only thing PaginatedComponentFile.getTotalPages() reflects), which is exactly the race #5976 needs.
    GlobalConfiguration.MAX_PAGE_RAM.setValue(0L);

    int failures = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      final int iterationIndex = i;
      final String typeName = "BackAccount" + i;
      final VertexType backAccount = database.getSchema().createVertexType(typeName);
      backAccount.getEvents().registerListener((BeforeRecordCreateListener) this);
      backAccount.getEvents().registerListener((AfterRecordReadListener) this);
      backAccount.getEvents().registerListener((BeforeRecordUpdateListener) this);

      database.transaction(() -> database.newVertex(typeName).set("secret", SECRET).save());

      database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        database.transaction(() -> {
          final Vertex v1 = database.iterateType(typeName, true).next().asVertex();
          assertThat(v1.getString("secret")).as("iteration %d", iterationIndex).isEqualTo(SECRET);
        });
      } catch (final Exception e) {
        failures++;
      } finally {
        database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      }
    }

    assertThat(failures).as("failed iterations out of %d", ITERATIONS).isZero();
  }

  @Override
  public Record onAfterRead(final Record record) {
    // MIRRORS RecordEncryptionTest'S LISTENER SHAPE: modify() FIRST, THEN DECRYPT IN PLACE. THIS IS WHAT MAKES THE
    // LISTENER NON-REENTRANT-SAFE - THE BUG THIS TEST GUARDS AGAINST IS modify() TRIGGERING A NESTED RE-READ.
    final MutableVertex doc = record.asVertex().modify();
    try {
      final byte[] ivBytes = Base64.getDecoder().decode(doc.getString("iv"));
      final IvParameterSpec iv = new IvParameterSpec(ivBytes);
      doc.set("secret", RecordEncryptionTest.decrypt(ALGORITHM, doc.getString("secret"), key, iv));
      return doc;
    } catch (final Exception e) {
      throw new SecurityException(e);
    }
  }

  @Override
  public boolean onBeforeCreate(final Record record) {
    final MutableVertex doc = record.asVertex().modify();
    try {
      final byte[] iv = new byte[16];
      SECURE_RANDOM.nextBytes(iv);
      final IvParameterSpec ivSpec = new IvParameterSpec(iv);
      final String encrypted = RecordEncryptionTest.encrypt(ALGORITHM, doc.getString("secret"), key, ivSpec);
      doc.set("secret", encrypted);
      doc.set("iv", Base64.getEncoder().encodeToString(iv));
    } catch (final Exception e) {
      throw new SecurityException(e);
    }
    return true;
  }

  @Override
  public boolean onBeforeUpdate(final Record record) {
    return onBeforeCreate(record);
  }
}
