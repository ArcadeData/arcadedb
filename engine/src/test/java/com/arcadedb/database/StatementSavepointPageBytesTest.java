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
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.DuplicatedKeyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10346, at the level of the bytes: the page undo log is only right if every write to a page declares its range BEFORE it changes
 * the bytes, so the log reads them as they were. A writer that touched the bytes first and declared them after would restore the
 * statement's own bytes, and nothing above the pages would see it: the records, the indexes and the counters all read back fine from
 * the wrong page, until a later write lands on it.
 * <p>
 * The test snapshots every page the transaction holds (bytes, content size, modified ranges), takes a failing statement of each kind of
 * write back, and requires every page to be byte for byte what it was and no page to have been added.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StatementSavepointPageBytesTest extends TestHelper {
  private final AtomicInteger deletes     = new AtomicInteger();
  private       int           refuseAtNth = -1;

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.command("sql", "CREATE PROPERTY T.k INTEGER").close();
    database.command("sql", "CREATE PROPERTY T.payload STRING").close();
    database.command("sql", "CREATE INDEX ON T (k) UNIQUE").close();
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE PROPERTY V.id LONG").close();
    database.command("sql", "CREATE EDGE TYPE E").close();
    database.command("sql", "CREATE PROPERTY E.w INTEGER").close();
    database.command("sql", "CREATE INDEX ON E (w) UNIQUE").close();

    final BeforeRecordDeleteListener refuseTheNth = record -> {
      if (refuseAtNth > 0 && deletes.incrementAndGet() == refuseAtNth)
        throw new IllegalStateException("refused");
      return true;
    };
    database.getSchema().getType("T").getEvents().registerListener(refuseTheNth);
    database.getSchema().getType("V").getEvents().registerListener(refuseTheNth);
  }

  /** What a page of the transaction holds. */
  private record PageImage(byte[] bytes, int contentSize, int[] ranges) {
    @Override
    public boolean equals(final Object o) {
      return o instanceof PageImage other && Arrays.equals(bytes, other.bytes) && contentSize == other.contentSize
          && Arrays.equals(ranges, other.ranges);
    }

    @Override
    public int hashCode() {
      return contentSize;
    }

    @Override
    public String toString() {
      return "PageImage(contentSize=" + contentSize + ", ranges=" + Arrays.toString(ranges) + ", length=" + bytes.length + ")";
    }
  }

  @SuppressWarnings("unchecked")
  private Map<PageId, PageImage> snapshotOfThePagesTheTransactionHolds() throws ReflectiveOperationException {
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final Map<PageId, PageImage> images = new HashMap<>();
    for (final String name : new String[] { "modifiedPages", "newPages" }) {
      final Field field = TransactionContext.class.getDeclaredField(name);
      field.setAccessible(true);
      final Map<PageId, MutablePage> pages = (Map<PageId, MutablePage>) field.get(tx);
      if (pages == null)
        continue;
      for (final Map.Entry<PageId, MutablePage> entry : pages.entrySet()) {
        final MutablePage page = entry.getValue();
        images.put(entry.getKey(), new PageImage(page.getImmutableView(0, page.getMaxContentSize()).getContent().clone(),
            page.getContentSize(), Arrays.copyOf(page.getModifiedRanges(), page.getModifiedRangeCount() * 2)));
      }
    }
    return images;
  }

  private void assertTakenBackToTheByte(final Map<PageId, PageImage> before, final String statement) throws ReflectiveOperationException {
    final Map<PageId, PageImage> after = snapshotOfThePagesTheTransactionHolds();
    assertThat(new TreeSet<>(after.keySet().stream().map(PageId::toString).toList())).as("%s: the pages of the transaction", statement)
        .isEqualTo(new TreeSet<>(before.keySet().stream().map(PageId::toString).toList()));
    for (final Map.Entry<PageId, PageImage> entry : before.entrySet()) {
      final PageImage now = after.get(entry.getKey());
      assertThat(now.contentSize()).as("%s: content size of %s", statement, entry.getKey()).isEqualTo(entry.getValue().contentSize());
      assertThat(now.ranges()).as("%s: modified ranges of %s", statement, entry.getKey()).isEqualTo(entry.getValue().ranges());
      assertThat(now.bytes()).as("%s: bytes of %s", statement, entry.getKey()).isEqualTo(entry.getValue().bytes());
    }
  }

  private String payload(final int size) {
    return "x".repeat(size);
  }

  @Test
  void everyKindOfWriteIsTakenBackToTheByte() throws Exception {
    // PAGES THE TRANSACTION MODIFIES (NOT NEW ONES): SOME COMMITTED CONTENT, THEN MORE OF IT IN THE TRANSACTION
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.command("sql", "INSERT INTO T SET id = ?, k = ?, payload = ?", i, i, payload(30 + (i % 7) * 200)).close();
      database.command("sql", "INSERT INTO V (id) VALUES (1), (2), (3), (4), (5)").close();
      database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE id = 1) TO (SELECT FROM V WHERE id = 2) SET w = 1").close();
    });

    database.begin();
    database.command("sql", "INSERT INTO T SET id = 100, k = 100, payload = ?", payload(500)).close();
    database.command("sql", "DELETE FROM T WHERE id = 3").close();
    database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE id = 2) TO (SELECT FROM V WHERE id = 3) SET w = 2").close();
    database.command("sql", "INSERT INTO V (id) VALUES (6)").close();
    Map<PageId, PageImage> before = snapshotOfThePagesTheTransactionHolds();
    assertThat(before).as("the transaction holds pages").isNotEmpty();

    // 1. ROWS APPENDED TO A PAGE THE TRANSACTION HOLDS, THE LAST ONE CLASHES
    assertThatThrownBy(() -> database.command("sql",
        "INSERT INTO T (id, k, payload) VALUES (200, 200, '" + payload(300) + "'), (201, 201, '" + payload(900) + "'), (202, 100, 'x')")
        .close()).isInstanceOf(DuplicatedKeyException.class);
    assertTakenBackToTheByte(before, "insert");

    // 2. A MULTI-PAGE RECORD: ITS CHUNKS ARE WRITTEN THROUGH THE INLINE RECORD-TABLE PATH
    assertThatThrownBy(() -> database.command("sql",
        "INSERT INTO T (id, k, payload) VALUES (300, 300, '" + payload(200_000) + "'), (301, 100, 'x')").close()).isInstanceOf(
        DuplicatedKeyException.class);
    assertTakenBackToTheByte(before, "multi-page insert");

    // 3. SLOTS FREED AND THE PAGE COMPACTED, THE THIRD DELETE IS REFUSED
    deletes.set(0);
    refuseAtNth = 3;
    assertThatThrownBy(() -> database.command("sql", "DELETE FROM T WHERE id < 20").close()).hasStackTraceContaining("refused");
    refuseAtNth = -1;
    assertTakenBackToTheByte(before, "delete");

    // 4. EDGES APPENDED TO THE EDGE LISTS OF VERTICES THAT HAVE ONE ALREADY, THE SECOND EDGE CLASHES
    assertThatThrownBy(() -> database.command("sql",
        "CREATE EDGE E FROM (SELECT FROM V WHERE id = 2) TO (SELECT FROM V WHERE id IN [4, 5]) SET w = 50").close()).isInstanceOf(
        DuplicatedKeyException.class);
    assertTakenBackToTheByte(before, "create edge");

    // 5. A VERTEX AND ITS EDGES DELETED, A LATER VERTEX IS REFUSED
    deletes.set(0);
    refuseAtNth = 2;
    assertThatThrownBy(() -> database.command("sql", "DELETE VERTEX FROM V WHERE id IN [1, 2, 3]").close()).hasStackTraceContaining(
        "refused");
    refuseAtNth = -1;
    assertTakenBackToTheByte(before, "delete vertex");

    // THE TRANSACTION STILL COMMITS WHAT IT HAD
    database.commit();
    try (final var rs = database.query("sql", "SELECT count(*) AS c FROM T")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(40L);
    }
    try (final var rs = database.command("sql", "CHECK DATABASE")) {
      assertThat(rs.next().<Number>getProperty("totalErrors").intValue()).isZero();
    }
  }
}
