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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * A round trip of the page format of every quantization: a vector index of NONE, INT8, BINARY and PRODUCT entries is written, the
 * pages are parsed back entry by entry (and the BINARY and INT8 entries must be skipped exactly: a wrong width would shift every
 * entry after it), the database is reopened so the locations are loaded from the pages, and a search answers every live record at
 * the distance to its current vector, as a brute force over the records does.
 */
class LSMVectorIndexPageParserQuantizationTest extends TestHelper {
  private static final int DIM = 13; // not a multiple of 8: the binary code is ceil(13 / 8) = 2 bytes

  private final Map<RID, float[]> live = new HashMap<>();

  private LSMVectorIndex subIndex() {
    final Index typeIndex = database.getSchema().getIndexByName("Doc[vector]");
    return (LSMVectorIndex) ((TypeIndex) typeIndex).getIndexesOnBuckets()[0];
  }

  private float[] randomVector(final Random rnd) {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++)
      v[i] = (float) rnd.nextGaussian();
    return v;
  }

  private void load(final String quantization, final Random rnd) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {\"dimensions\": " + DIM
          + ", \"similarity\": \"COSINE\", \"quantization\": \"" + quantization + "\"}");
    });
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 150; i++) {
        final float[] v = randomVector(rnd);
        final MutableDocument d = database.newVertex("Doc").set("vector", v).save();
        rids.add(d.getIdentity());
        live.put(d.getIdentity(), v);
      }
    });
    // ten records change their vector (one entry superseded, one written) and ten are deleted (a tombstone each)
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final float[] v = randomVector(rnd);
        database.lookupByRID(rids.get(i), true).asVertex().modify().set("vector", v).save();
        live.put(rids.get(i), v);
      }
      for (int i = 10; i < 20; i++) {
        database.lookupByRID(rids.get(i), true).asVertex().delete();
        live.remove(rids.get(i));
      }
    });
  }

  private List<LSMVectorIndexPageParser.VectorEntry> parse(final boolean strict) {
    final LSMVectorIndex index = subIndex();
    final List<LSMVectorIndexPageParser.VectorEntry> entries = new ArrayList<>();
    final int[] cutShort = new int[1];
    final int read = LSMVectorIndexPageParser.parsePages((DatabaseInternal) database, index.getFileId(), index.getTotalPages(),
        index.getPageSize(), false, strict, entries::add, cutShort);
    assertThat(read).isEqualTo(entries.size());
    assertThat(cutShort[0]).as("no page cut short").isZero();
    return entries;
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8", "BINARY", "PRODUCT" })
  void everyEntryOfEveryQuantizationIsParsedBackWithItsRecordAndLength(final String quantization) {
    load(quantization, new Random(5));
    final List<LSMVectorIndexPageParser.VectorEntry> entries = parse(true);

    // 150 inserted + 10 rewritten (each also tombstones the entry it supersedes) + 10 deleted (one tombstone each)
    assertThat(entries).hasSize(180);
    assertThat(entries.stream().filter(e -> e.deleted).count()).isEqualTo(20);
    assertThat(entries.stream().filter(e -> !e.isCompacted).count()).isEqualTo(180);

    // entries are written back to back: the offset of an entry plus its length is the offset of the next one (on one page)
    int adjacent = 0;
    for (int i = 1; i < entries.size(); i++) {
      final LSMVectorIndexPageParser.VectorEntry previous = entries.get(i - 1);
      final LSMVectorIndexPageParser.VectorEntry current = entries.get(i);
      if (current.absoluteFileOffset > previous.absoluteFileOffset
          && current.absoluteFileOffset - previous.absoluteFileOffset == previous.entryLength)
        adjacent++;
    }
    assertThat(adjacent).as("entries of a page are contiguous").isEqualTo(179);

    // a quantized entry is wider than a NONE or PRODUCT one by exactly the code it carries
    final int quantizedWidth = LSMVectorIndexPageParser.calculateQuantizedDataSize(DIM, VectorQuantizationType.valueOf(quantization));
    final int expectedQuantizedWidth = switch (quantization) {
      case "INT8" -> DIM + 8;
      case "BINARY" -> 2 + 4;
      default -> 0;
    };
    assertThat(quantizedWidth).isEqualTo(expectedQuantizedWidth);
    final int widest = entries.stream().filter(e -> !e.deleted).mapToInt(e -> e.entryLength).max().orElseThrow();
    final int narrowest = entries.stream().filter(e -> !e.deleted).mapToInt(e -> e.entryLength).min().orElseThrow();
    assertThat(widest - narrowest).as("entry lengths differ only by the varint widths of the ids").isLessThanOrEqualTo(12);
    // id + bucket + position varints (at least 1 byte each) + deleted byte + quantization byte + the quantized part
    // (the 4-byte array length of a quantized entry included)
    assertThat(narrowest).isGreaterThanOrEqualTo(3 + 1 + 1 + (quantizedWidth > 0 ? 4 + quantizedWidth : 0));

    // the records of the live entries are exactly the live records plus the ones superseded or deleted since
    final Set<RID> parsedRids = new HashSet<>();
    for (final LSMVectorIndexPageParser.VectorEntry e : entries)
      if (!e.deleted)
        parsedRids.add(e.rid);
    assertThat(parsedRids).containsAll(live.keySet());
    assertThat(parsedRids).hasSize(150);
  }

  @ParameterizedTest
  @CsvSource({ "NONE,0.0002", "INT8,0.05", "BINARY,0.0002", "PRODUCT,0.0002" })
  void aReopenedIndexAnswersEveryLiveRecordAtItsCurrentDistance(final String quantization, final double tolerance) {
    final Random rnd = new Random(9);
    load(quantization, rnd);
    final float[] query = randomVector(rnd);

    final Map<RID, Float> before = exhaustive(query);
    assertThat(before.keySet()).isEqualTo(live.keySet());

    reopenDatabase();
    final Map<RID, Float> after = exhaustive(query);
    assertThat(after.keySet()).as("every live record answers after the reopen, nothing deleted or superseded does").isEqualTo(live.keySet());
    for (final Map.Entry<RID, Float> e : after.entrySet())
      assertThat((double) e.getValue()).as("distance of %s", e.getKey()).isCloseTo(cosineDistance(query, live.get(e.getKey())), within(tolerance));
    assertThat(before).isEqualTo(after);
  }

  @ParameterizedTest
  @ValueSource(strings = { "INT8", "BINARY" })
  void aDamagedEntryCountIsRefusedByAStrictParseAndReportedByALenientOne(final String quantization) throws IOException {
    load(quantization, new Random(3));
    final LSMVectorIndex index = subIndex();
    final PageId pageId = new PageId(database, index.getFileId(), 0);
    final int intact = ((DatabaseInternal) database).getPageManager().getImmutablePage(pageId, index.getPageSize(), false, false)
        .readInt(LSMVectorIndex.OFFSET_NUM_ENTRIES);
    assertThat(intact).isEqualTo(180);

    writeEntryCount(pageId, index, -1);
    try {
      assertThatThrownBy(() -> parse(true)).isInstanceOf(IndexException.class).hasMessageContaining("Invalid number of entries -1");

      // the lenient parse logs the page, counts it, and moves on: nothing of it is in the result
      final int[] cutShort = new int[1];
      final List<LSMVectorIndexPageParser.VectorEntry> entries = new ArrayList<>();
      LSMVectorIndexPageParser.parsePages((DatabaseInternal) database, index.getFileId(), index.getTotalPages(), index.getPageSize(), false, false,
          entries::add, cutShort);
      assertThat(cutShort[0]).isEqualTo(1);
      assertThat(entries).isEmpty();
    } finally {
      writeEntryCount(pageId, index, intact);
    }
    assertThat(parse(true)).hasSize(180);
  }

  private void writeEntryCount(final PageId pageId, final LSMVectorIndex index, final int count) {
    database.transaction(() -> {
      try {
        final MutablePage page = ((DatabaseInternal) database).getTransaction().getPageToModify(pageId, index.getPageSize(), false);
        page.writeInt(LSMVectorIndex.OFFSET_NUM_ENTRIES, count);
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }

  @Test
  void anInterruptionIsToldFromAnUnreadablePage() {
    assertThat(LSMVectorIndexPageParser.isInterruption(new ClosedChannelException())).isTrue();
    assertThat(LSMVectorIndexPageParser.isInterruption(new IndexException("x", new InterruptedIOException()))).isTrue();
    assertThat(LSMVectorIndexPageParser.isInterruption(new IndexException("x", new DatabaseIsClosedException("db")))).isTrue();
    assertThat(LSMVectorIndexPageParser.isInterruption(new IndexException("x", new InterruptedException()))).isTrue();
    assertThat(LSMVectorIndexPageParser.isInterruption(new IndexException("damaged page"))).isFalse();
    assertThat(LSMVectorIndexPageParser.isInterruption(new IllegalStateException("x", new IllegalArgumentException()))).isFalse();
  }

  private Map<RID, Float> exhaustive(final float[] query) {
    final Map<RID, Float> answer = new HashMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, distance FROM (SELECT expand(vector.neighbors('Doc[vector]', ?, 1000, {efSearch: 2000})))", (Object) query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        answer.put(((Identifiable) r.getProperty("rid")).getIdentity(), r.<Number>getProperty("distance").floatValue());
      }
    }
    return answer;
  }

  private static double cosineDistance(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return 1.0 - dot / Math.sqrt(na * nb);
  }
}
