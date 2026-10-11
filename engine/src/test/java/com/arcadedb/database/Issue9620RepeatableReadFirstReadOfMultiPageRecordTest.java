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

import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9620: under {@code REPEATABLE_READ} the read of a small record pins its page, and a continuation chunk of a
 * multi-page record lives on the same page. After a concurrent rewrite of the multi-page record its FIRST read in the
 * transaction was refused with a {@code ConcurrentModificationException}, and so was every query that scanned the type,
 * although the transaction had never read that record and owed no image of it. It is served the newest committed version
 * whole, and every later read of the transaction answers the same.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9620RepeatableReadFirstReadOfMultiPageRecordTest extends BucketPageLayoutTestSupport {
  private static final int SIZE = 200_000;

  private RID a;
  private RID b;
  private RID c;

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createDocumentType("T", 1));
    database.transaction(() -> a = database.newDocument("T").set("v", 0).set("s", "a").save().getIdentity());
    database.transaction(() -> b = database.newDocument("T").set("v", 0).set("s", "Q".repeat(SIZE)).save().getIdentity());
    database.transaction(() -> c = database.newDocument("T").set("v", 0).set("s", "c").save().getIdentity());
    assertThat((Long) bucketStats("T").get("totalMultiPageRecords")).isEqualTo(1L);
  }

  @Test
  void theChainOfTheLargeRecordSharesThePageOfTheSmallOne() {
    // the premise of the issue: a chunk of the large record lives on the page of the small one
    final LocalBucket bucket = bucketOf("T");
    final long pageOfA = a.getPosition() / bucket.getMaxRecordsInPage();
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      readLarge(b);
      assertThat(isPinned(bucket, (int) pageOfA)).as("reading the large record pins the page of the small one").isTrue();
      assertThat(pageOfA).isNotEqualTo(b.getPosition() / bucket.getMaxRecordsInPage());
    } finally {
      database.rollback();
    }
  }

  @Test
  void firstReadAfterANeighbourPinnedAPageOfTheChainIsServed() {
    final LocalBucket bucket = bucketOf("T");
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");

      rewriteLarge(1, 'X');

      assertThat(readLarge(b)).isEqualTo("v=1 s=" + SIZE + "X");

      // later reads answer the same, from the transaction's own copy, whatever commits next
      rewriteLarge(2, 'Z');
      ((DatabaseInternal) database).getTransaction().removeRecordFromCache(b);
      assertThat(readLarge(b)).isEqualTo("v=1 s=" + SIZE + "X");
      assertThat(readLarge(b)).isEqualTo("v=1 s=" + SIZE + "X");

      // the read of the neighbour stays repeatable: its page was neither released nor re-pinned
      assertThat(isPinned(bucket, (int) (a.getPosition() / bucket.getMaxRecordsInPage()))).isTrue();
      ((DatabaseInternal) database).getTransaction().removeRecordFromCache(a);
      assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");
    } finally {
      database.rollback();
    }

    // a new transaction reads the newest version
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(readLarge(b)).isEqualTo("v=2 s=" + SIZE + "Z");
    } finally {
      database.rollback();
    }
  }

  @Test
  void scanAfterANeighbourPinnedAPageOfTheChainSucceeds() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");
      rewriteLarge(1, 'X');

      assertThat(scan()).containsExactly("a", SIZE + "X", "c");
      // a second scan in the same transaction answers the same, after one more concurrent rewrite
      rewriteLarge(2, 'Z');
      assertThat(scan()).containsExactly("a", SIZE + "X", "c");
    } finally {
      database.rollback();
    }
  }

  @Test
  void recordReadBeforeTheRewriteIsRepeated() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");
      assertThat(readLarge(b)).isEqualTo("v=0 s=" + SIZE + "Q");

      rewriteLarge(1, 'X');

      ((DatabaseInternal) database).getTransaction().removeRecordFromCache(b);
      assertThat(readLarge(b)).isEqualTo("v=0 s=" + SIZE + "Q");
    } finally {
      database.rollback();
    }
  }

  /**
   * The served version is for reading: the page of the chain the transaction holds at its older image cannot take the
   * rewritten record, so an update is refused with the retryable conflict as soon as the record is taken for it, never
   * laid over a state the commit would have to merge blindly. A new transaction updates it.
   */
  @Test
  void updateAfterAServedFirstReadIsARetryableConflict() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");
      rewriteLarge(1, 'X');
      assertThat(readLarge(b)).isEqualTo("v=1 s=" + SIZE + "X");

      final MutableDocument toUpdate = database.lookupByRID(b, true).asDocument().modify();
      assertThatThrownBy(() -> toUpdate.set("v", 7).set("s", "W".repeat(SIZE + 1)).save())
          .isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }

    database.transaction(() -> database.lookupByRID(b, true).asDocument().modify().set("v", 7).set("s", "W".repeat(SIZE + 1)).save(),
        false, 3);
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(readLarge(b)).isEqualTo("v=7 s=" + (SIZE + 1) + "W");
    } finally {
      database.rollback();
    }
  }

  @Test
  void readCommittedReadsTheNewVersion() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    try {
      assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");
      rewriteLarge(1, 'X');
      assertThat(scan()).containsExactly("a", SIZE + "X", "c");
    } finally {
      database.rollback();
    }
  }

  /**
   * A commit puts its pages in the read cache one at a time. A read whose validation ran while one was being published could
   * find the new head chunk and the old continuation chunk both current, and accepted a record no commit ever wrote (under
   * REPEATABLE_READ it then kept it as its snapshot). Every read must be a record some commit wrote: its filler and its length
   * follow from its {@code v}.
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES) // hang detector for the writer join, not a latency bound
  void noReadAssemblesAHalfPublishedCommit() throws Exception {
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        for (int v = 1; !stop.get(); v++) {
          final int version = v;
          database.transaction(() -> database.lookupByRID(b, true).asDocument().modify().set("v", version)
              .set("s", String.valueOf(fillerOf(version)).repeat(sizeOf(version))).save(), false, 100);
        }
      } catch (final Throwable e) {
        writerFailure.set(e);
      }
    });
    writer.start();
    try {
      for (int i = 0; i < 600; i++) {
        final boolean repeatableRead = i % 2 == 0;
        database.begin(repeatableRead ?
            Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ :
            Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
        try {
          if (i % 4 == 0)
            // pins the page of the neighbour, which a chunk of the large record shares
            database.lookupByRID(a, true).asDocument().getString("s");
          final String first = readWrittenByACommit(i);
          final String second = readWrittenByACommit(i);
          if (repeatableRead && !first.equals("CME"))
            assertThat(second).as("second read of transaction " + i).isEqualTo(first);
        } finally {
          database.rollback();
        }
      }
    } finally {
      stop.set(true);
      writer.join();
    }
    assertThat(writerFailure.get()).as("the writer thread must not have failed").isNull();
  }

  private static char fillerOf(final int v) {
    return (char) ('a' + v % 26);
  }

  private static int sizeOf(final int v) {
    return SIZE - 120_000 + v % 3 * 20_000;
  }

  private String readWrittenByACommit(final int transaction) {
    final Document read;
    try {
      ((DatabaseInternal) database).getTransaction().removeRecordFromCache(b);
      read = database.lookupByRID(b, true).asDocument();
    } catch (final ConcurrentModificationException e) {
      return "CME";
    }
    final int v = read.getInteger("v");
    final String s = read.getString("s");
    final String expected = v == 0 ? "Q".repeat(SIZE) : String.valueOf(fillerOf(v)).repeat(sizeOf(v));
    assertThat(expected.equals(s)).as("transaction %d read v=%d with an s no commit wrote: %s", transaction, v,
        s == null ? "null" : s.length() + " chars from '" + s.charAt(0) + "' to '" + s.charAt(s.length() - 1) + "'").isTrue();
    return "v=" + v;
  }

  private List<String> scan() {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM T ORDER BY @rid")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final String s = row.getProperty("s");
        rows.add(s.length() == 1 ? s : s.length() + s.substring(0, 1));
      }
    }
    return rows;
  }

  private void rewriteLarge(final int v, final char filler) {
    inAnotherThread(() -> database.transaction(
        () -> database.lookupByRID(b, true).asDocument().modify().set("v", v).set("s", String.valueOf(filler).repeat(SIZE)).save()));
  }

  /** {@code v=<v> s=<length><filler>}, failing on a value of {@code s} that is not one filler repeated. */
  private String readLarge(final RID rid) {
    final Document read = database.lookupByRID(rid, true).asDocument();
    final int v = read.getInteger("v");
    final String s = read.getString("s");
    final char filler = s.charAt(0);
    assertThat(s.chars().allMatch(ch -> ch == filler)).as("s of v=" + v + " is a mix").isTrue();
    return "v=" + v + " s=" + s.length() + filler;
  }

  private boolean isPinned(final LocalBucket bucket, final int pageNumber) {
    return ((DatabaseInternal) database).getTransaction().getPinnedPage(new PageId(database, bucket.getFileId(), pageNumber)) != null;
  }
}
