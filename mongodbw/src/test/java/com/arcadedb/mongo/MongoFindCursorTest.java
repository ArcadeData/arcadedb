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
package com.arcadedb.mongo;

import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a {@link MongoFindCursor} reads and converts per batch, and what it releases when. The rows are counted as they are pulled,
 * so the laziness is asserted by a counter, not by a clock.
 */
class MongoFindCursorTest {

  /** Rows {@code 0..size-1} as documents, counting how many were pulled and how many were closed. */
  private static final class Source implements Iterator<Object>, AutoCloseable {
    final AtomicInteger pulled = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    private final int   size;
    private final int   failAt;

    Source(final int size) {
      this(size, -1);
    }

    Source(final int size, final int failAt) {
      this.size = size;
      this.failAt = failAt;
    }

    @Override
    public boolean hasNext() {
      return pulled.get() < size;
    }

    @Override
    public Object next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final int row = pulled.getAndIncrement();
      if (row == failAt)
        throw new IllegalStateException("row " + row + " is unreadable");
      return new Document("n", row);
    }

    @Override
    public void close() {
      closed.incrementAndGet();
    }
  }

  private static List<Object> values(final List<Document> documents) {
    return documents.stream().map(d -> d.get("n")).toList();
  }

  @Test
  void aBatchPullsAndConvertsNoMoreRowsThanItHolds() {
    final Source source = new Source(100_000);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 0, 0, null);

    final List<Document> first = cursor.takeBatch(MongoFindCursor.DEFAULT_FIRST_BATCH, MongoFindCursor.MAX_BATCH_BYTES);
    assertThat(first).hasSize(101);
    assertThat(source.pulled.get()).as("rows pulled by the first batch").isEqualTo(101);
    assertThat(cursor.isEmpty()).isFalse();
    assertThat(source.pulled.get()).as("looking for a next row pulls none").isEqualTo(101);

    assertThat(values(cursor.takeDocuments(10))).containsExactly(101, 102, 103, 104, 105, 106, 107, 108, 109, 110);
    assertThat(source.pulled.get()).isEqualTo(111);
    assertThat(source.closed.get()).as("not exhausted: still open").isZero();
  }

  @Test
  void skippedRowsAreNotConvertedAndTheLimitStopsTheReading() {
    final Source source = new Source(1_000);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 20, 7, null);

    assertThat(values(cursor.takeDocuments(3))).containsExactly(20, 21, 22);
    assertThat(values(cursor.takeDocuments(100))).containsExactly(23, 24, 25, 26);
    assertThat(source.pulled.get()).as("the rows past the limit are never pulled").isEqualTo(27);
    assertThat(cursor.isEmpty()).isTrue();
    assertThat(source.closed.get()).as("the limit is reached: released").isEqualTo(1);
  }

  @Test
  void aBatchThatEndsAtItsSizeDoesNotLookForARowAndTheNextOneFindsTheEnd() {
    final Source source = new Source(101);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 0, 0, null);

    assertThat(cursor.takeBatch(101, MongoFindCursor.MAX_BATCH_BYTES)).hasSize(101);
    assertThat(cursor.isDrained()).as("a full batch does not know whether a row follows").isFalse();
    assertThat(source.closed.get()).isZero();

    assertThat(cursor.takeBatch(101, MongoFindCursor.MAX_BATCH_BYTES)).isEmpty();
    assertThat(cursor.isDrained()).isTrue();
    assertThat(source.closed.get()).as("released by the batch that found the end").isEqualTo(1);
  }

  @Test
  void aBatchThatReachesTheLimitKnowsItWithoutReadingARowMore() {
    final Source source = new Source(1_000);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 0, 7, null);

    assertThat(cursor.takeBatch(7, MongoFindCursor.MAX_BATCH_BYTES)).hasSize(7);
    assertThat(cursor.isDrained()).isTrue();
    assertThat(source.pulled.get()).isEqualTo(7);
    assertThat(source.closed.get()).isEqualTo(1);
  }

  @Test
  void exhaustingTheRowsReleasesTheSourceOnceAndCloseIsIdempotent() {
    final Source source = new Source(5);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 0, 0, null);

    assertThat(cursor.takeDocuments(5)).hasSize(5);
    assertThat(source.closed.get()).as("a full batch is not yet the end").isZero();
    assertThat(cursor.isEmpty()).isTrue();
    assertThat(source.closed.get()).isEqualTo(1);

    cursor.close();
    cursor.close();
    assertThat(source.closed.get()).isEqualTo(1);
    assertThat(cursor.takeDocuments(5)).as("a closed cursor answers nothing").isEmpty();
  }

  @Test
  void skippingPastTheEndLeavesNothing() {
    final Source source = new Source(5);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 50, 0, null);
    assertThat(cursor.isEmpty()).isTrue();
    assertThat(source.closed.get()).isEqualTo(1);
  }

  @Test
  void aKilledCursorReleasesTheSourceWithRowsLeft() {
    final Source source = new Source(1_000);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 0, 0, null);
    cursor.takeDocuments(10);
    cursor.close();
    assertThat(source.closed.get()).isEqualTo(1);
    assertThat(source.pulled.get()).isEqualTo(10);
    assertThat(cursor.isEmpty()).isTrue();
  }

  @Test
  void aRowThatCannotBeReadClosesTheCursorAndTheFailureReachesTheCaller() {
    final Source source = new Source(100, 12);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 0, 0, null);
    assertThat(cursor.takeDocuments(10)).hasSize(10);
    assertThatThrownBy(() -> cursor.takeDocuments(10)).isInstanceOf(IllegalStateException.class).hasMessageContaining("row 12");
    assertThat(source.closed.get()).as("a half-served cursor is not kept").isEqualTo(1);
  }

  @Test
  void aBatchEndsOnceItsSizeReachesTheBudgetButAlwaysHoldsOneDocument() {
    final String text = "x".repeat(1_000);
    final Iterator<Object> rows = IntStream.range(0, 50).<Object>mapToObj(i -> new Document("t", text)).iterator();
    final MongoFindCursor cursor = new MongoFindCursor(rows, null, 0, 0, null);

    final List<Document> batch = cursor.takeBatch(Integer.MAX_VALUE, 10_000L);
    assertThat(batch.size()).isBetween(9, 10);

    final MongoFindCursor tiny = new MongoFindCursor(IntStream.range(0, 5).<Object>mapToObj(i -> new Document("t", text))
        .iterator(), null, 0, 0, null);
    assertThat(tiny.takeBatch(Integer.MAX_VALUE, 1L)).as("the budget is smaller than one document").hasSize(1);
  }

  /** A string is answered as its UTF-8 bytes: three for a CJK character, and four for the pair of chars of an emoji. */
  @Test
  void aStringCountsTheBytesOfItsUtf8Text() {
    assertThat(MongoFindCursor.estimateSize(new Document("t", "x".repeat(1_000)))).isBetween(1_000L, 1_030L);
    assertThat(MongoFindCursor.estimateSize(new Document("t", "中".repeat(1_000)))).isBetween(3_000L, 3_030L);
    assertThat(MongoFindCursor.estimateSize(new Document("t", "😀".repeat(1_000)))).isBetween(4_000L, 4_030L);
    assertThat(MongoFindCursor.estimateSize(new Document("中".repeat(100), 1))).as("a field name too").isGreaterThan(300L);
  }

  /** The budget is checked before a document joins a batch: the batch is never filled past it, and the document is not lost. */
  @Test
  void aDocumentThatDoesNotFitWaitsForTheNextBatchWithoutBeingLostOrCountedTwice() {
    final String text = "中".repeat(2_000);
    final Source source = new Source(5);
    final Iterator<Object> rows = new Iterator<>() {
      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public Object next() {
        return new Document("n", ((Document) source.next()).get("n")).append("t", text);
      }
    };
    final MongoFindCursor cursor = new MongoFindCursor(rows, source, 0, 4, null);

    // 6 KB a document, a budget of 10 KB: a batch holds one
    for (int expected = 0; expected < 4; expected++) {
      final List<Document> batch = cursor.takeBatch(Integer.MAX_VALUE, 10_000L);
      assertThat(values(batch)).containsExactly(expected);
      assertThat(cursor.isEmpty()).as("a retained document keeps the cursor from being empty, until the limit").isEqualTo(expected == 3);
    }
    assertThat(source.pulled.get()).as("the limit stops the reading: the fifth row is never pulled").isEqualTo(4);
    assertThat(source.closed.get()).isEqualTo(1);
    assertThat(cursor.takeBatch(Integer.MAX_VALUE, 10_000L)).isEmpty();
  }

  /** A byte[] property is answered as a BinData: its bytes, not the 16 of a number, are what the reply holds. */
  @Test
  void aBinaryValueCountsItsBytesAndEndsTheBatchAtTheBudget() {
    final byte[] megabyte = new byte[1_000_000];
    assertThat(MongoFindCursor.estimateSize(new Document("b", new BinData(megabyte)))).isGreaterThan(1_000_000L);
    assertThat(MongoFindCursor.estimateSize(new Document("b", List.of(new BinData(megabyte), new BinData(megabyte))))).isGreaterThan(
        2_000_000L);
    assertThat(MongoFindCursor.estimateSize(new Document("r", new BsonRegularExpression("x".repeat(1_000), "i")))).isGreaterThan(1_000L);

    final Iterator<Object> rows = IntStream.range(0, 50).<Object>mapToObj(i -> new Document("b", new BinData(megabyte))).iterator();
    final MongoFindCursor cursor = new MongoFindCursor(rows, null, 0, 0, null);
    assertThat(cursor.takeBatch(Integer.MAX_VALUE, 10_000_000L).size()).as("10 MB of budget, 1 MB a document").isBetween(9, 10);
  }

  @Test
  void theDocumentsAreReadWhenAskedForAndTheCursorIsReleasedWithTheLastOne() {
    final Source source = new Source(1_000);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 10, 0, null);

    final Iterator<Document> documents = cursor.documents();
    for (int expected = 10; expected < 15; expected++) {
      assertThat(documents.hasNext()).isTrue();
      assertThat(documents.next().get("n")).isEqualTo(expected);
    }
    assertThat(source.pulled.get()).as("5 documents asked for: the skipped rows and those 5, no row ahead").isEqualTo(15);
    assertThat(source.closed.get()).isZero();

    int count = 5;
    while (documents.hasNext()) {
      documents.next();
      ++count;
    }
    assertThat(count).isEqualTo(990);
    assertThat(source.closed.get()).as("released with the last document").isEqualTo(1);
    assertThatThrownBy(documents::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void aFailureReadingADocumentSurfacesWhenItIsAskedForAndReleasesTheCursor() {
    final Source source = new Source(100, 3);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 0, 0, null);

    final Iterator<Document> documents = cursor.documents();
    for (int expected = 0; expected < 3; expected++) {
      assertThat(documents.hasNext()).isTrue();
      assertThat(documents.next().get("n")).isEqualTo(expected);
    }
    assertThat(source.closed.get()).as("nothing failed yet").isZero();

    assertThatThrownBy(documents::hasNext).isInstanceOf(IllegalStateException.class).hasMessageContaining("row 3");
    assertThat(source.closed.get()).as("the failed cursor is released").isEqualTo(1);
    assertThat(cursor.isClosed()).isTrue();
    assertThat(documents.hasNext()).isFalse();
  }

  @Test
  void aStreamOfTheCursorStopsReadingWhereItIsStoppedAndReleasesTheCursorWhenClosed() {
    final Source source = new Source(100_000);
    final MongoFindCursor cursor = new MongoFindCursor(source, source, 0, 0, null);

    try (final Stream<Document> stream = cursor.stream()) {
      assertThat(stream.limit(3).map(d -> d.get("n")).toList()).containsExactly(0, 1, 2);
      assertThat(source.pulled.get()).as("a limit stops the reading at the documents it takes").isBetween(3, 4);
      assertThat(source.closed.get()).isZero();
    }
    assertThat(source.closed.get()).as("closing the stream releases the rows").isEqualTo(1);
    assertThat(cursor.isClosed()).isTrue();
    assertThat(cursor.documents().hasNext()).isFalse();
  }

  @Test
  void theSizeEstimateCoversStringsNestedDocumentsAndArrays() {
    final Document small = new Document("a", 1);
    final Document nested = new Document("a", new Document("b", "y".repeat(500))).append("c", List.of("z".repeat(500), 2));
    assertThat(MongoFindCursor.estimateSize(small)).isLessThan(50L);
    assertThat(MongoFindCursor.estimateSize(nested)).isGreaterThan(1_000L);
  }
}
