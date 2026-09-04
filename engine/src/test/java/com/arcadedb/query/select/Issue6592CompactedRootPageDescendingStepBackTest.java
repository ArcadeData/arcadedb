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
package com.arcadedb.query.select;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Third follow-up regression test for https://github.com/ArcadeData/arcadedb/issues/6592: after the two earlier rounds
 * (#6635, #6692) and the root-page direction fix of #6694, {@code ORDER BY <trailing property> DESC} over a composite
 * index PREFIX still returned ZERO rows once the index had been compacted, while the very same query ASC returned all
 * of them - the asymmetry the reporter kept describing.
 * <p>
 * The remaining defect was in how {@code LSMTreeIndexCompacted.searchInCurrentPage()} read the ROOT-page probe result.
 * A compacted series' root page holds the MINIMUM key of each of its data pages followed by one trailing sentinel
 * entry carrying the series' MAXIMUM key, so root entry {@code i} addresses data page {@code i}. #6694 made a
 * descending PARTIAL-key probe use {@code purpose=3}, but {@code purpose=3}'s binary search returns the entry AT OR
 * BELOW the search key, whereas {@code purpose=1}/{@code 2} return the INSERTION POINT above it - and the shared
 * not-found branch stepped back one page unconditionally, which is right only for an insertion point. The descending
 * scan therefore started on a data page entirely BELOW the matching group, never visited the page holding it, and the
 * whole series contributed nothing.
 * <p>
 * It only shows up when the root-page probe does NOT land on an exact prefix match, i.e. when no data page of the
 * series happens to BEGIN with a row of the matched group - the ordinary case for a group small enough to live inside
 * a page that starts with a lower key, which is exactly the reporter's shape and the one shape
 * {@link Issue6592CompactedCompositePrefixDescTest} and {@link Issue6694CompactedRootPageDescendingPurposeTest} both
 * missed (they use groups large enough to start a page of their own).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6592CompactedRootPageDescendingStepBackTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;

  private record Row(String key1, String key2, long orderedAt) {
  }

  @Override
  protected void beginTest() {
    // Compaction on a dataset this small needs the RAM budget squeezed down; same technique as Issue5214MultiSeriesRangeTest.
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);
  }

  /**
   * The whole matched group has to come back, in descending order, whatever the page size, bucket count and group size
   * - none of which the reporter's query knows anything about, and any of which decides whether a data page happens to
   * begin on a group row.
   */
  @ParameterizedTest(name = "buckets={0} pageSize={1} noiseGroups={2} noiseRows={3} groupSize={4}")
  @CsvSource({ //
      "1,  4096,  6,  100,  1", //
      "1,  4096,  6,  100,  3", //
      "1,  4096,  6,  100, 30", //
      "1, 65536,  6,  500,  3", //
      "1, 65536, 12, 2500,  5", //
      "4,  4096,  6,  100,  3", //
      "8, 65536, 12, 2500,  5", //
      "1,  4096, 40,   50,  7", //
  })
  void descendingPrefixScanAfterCompactionReturnsTheWholeGroup(final int buckets, final int pageSize, final int noiseGroups,
      final int noiseRows, final int groupSize) {
    final String pad = "x".repeat(64);

    final VertexType supplier = database.getSchema().createVertexType("Supplier", buckets);
    supplier.createProperty("key1", Type.STRING);
    supplier.createProperty("key2", Type.STRING);
    supplier.createProperty("orderedAt", Type.LONG);
    supplier.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, new String[] { "key1", "key2", "orderedAt" }, pageSize);

    database.transaction(() -> {
      // Noise both alphabetically BEFORE and AFTER the target "a"/"b" group, so the group sits in the middle of a
      // series rather than on one of its boundaries (where the lookup takes a different branch entirely).
      for (int g = 0; g < noiseGroups; g++) {
        final String key1 = (g % 2 == 0 ? "0" : "z") + pad + g;
        for (int i = 0; i < noiseRows; i++)
          database.newVertex("Supplier").set("key1", key1, "key2", "x", "orderedAt", (long) i).save();
      }
      for (int i = 0; i < groupSize; i++)
        database.newVertex("Supplier").set("key1", "a", "key2", "b", "orderedAt", (long) i).save();
    });

    compact();

    database.transaction(() -> {
      final List<Long> descending = new ArrayList<>();
      for (final Vertex v : database.select().fromType("Supplier")//
          .where().property("key1").eq().value("a")//
          .and().property("key2").eq().value("b")//
          .orderBy("orderedAt", false)//
          .compile().vertices().toList()) {
        assertThat(v.getString("key1")).as("row outside the matched group").isEqualTo("a");
        assertThat(v.getString("key2")).as("row outside the matched group").isEqualTo("b");
        descending.add(v.getLong("orderedAt"));
      }

      assertThat(descending).as("descending prefix scan").hasSize(groupSize);
      for (int i = 0; i < groupSize; i++)
        assertThat(descending.get(i)).as("descending row " + i).isEqualTo((long) (groupSize - 1 - i));

      // The ascending scan has always worked: keep it here so a future fix cannot trade one direction for the other.
      final List<Long> ascending = new ArrayList<>();
      for (final Vertex v : database.select().fromType("Supplier")//
          .where().property("key1").eq().value("a")//
          .and().property("key2").eq().value("b")//
          .orderBy("orderedAt", true)//
          .compile().vertices().toList())
        ascending.add(v.getLong("orderedAt"));

      assertThat(ascending).as("ascending prefix scan").isEqualTo(reversedCopy(descending));
    });
  }

  /**
   * The reporter's query, verbatim: a DATETIME trailing property, bound parameters on a compiled query, and
   * {@code LIMIT 1} - which is what turns a dropped series into a {@code null} answer rather than a short result.
   */
  @Test
  void orderByDescLimitOneReturnsTheNewestRowOfTheGroup() {
    final String pad = "x".repeat(64);
    final int groupSize = 4;

    final VertexType supplier = database.getSchema().createVertexType("Supplier");
    supplier.createProperty("key1", Type.STRING);
    supplier.createProperty("key2", Type.STRING);
    supplier.createProperty("ordered_at", Type.DATETIME);
    supplier.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, new String[] { "key1", "key2", "ordered_at" }, 4096);

    database.transaction(() -> {
      for (int g = 0; g < 6; g++) {
        final String key1 = (g % 2 == 0 ? "0" : "z") + pad + g;
        for (int i = 0; i < 100; i++)
          database.newVertex("Supplier").set("key1", key1, "key2", "x", "ordered_at", new Date(BASE_TS + i * 1000L)).save();
      }
      for (int i = 0; i < groupSize; i++)
        database.newVertex("Supplier").set("key1", "a", "key2", "b", "ordered_at", new Date(BASE_TS + i * 1000L)).save();
    });

    compact();

    database.transaction(() -> {
      final SelectCompiled query = database.select().fromType("Supplier")//
          .where().property("key1").eq().parameter("key1")//
          .and().property("key2").eq().parameter("key2")//
          .limit(1)//
          .orderBy("ordered_at", false)//
          .compile();

      final Vertex newest = query.parameter("key1", "a").parameter("key2", "b").vertices().nextOrNull();

      assertThat(newest).as("newest row of the group").isNotNull();
      assertThat(newest.getString("key1")).isEqualTo("a");
      assertThat(newest.getString("key2")).isEqualTo("b");
      assertThat(epochMillis(newest)).isEqualTo(BASE_TS + (groupSize - 1) * 1000L);
    });
  }

  /**
   * A seeded - therefore fully deterministic - cross-check of every prefix scan against an in-memory oracle, over
   * randomly shaped indexes and over data split across the compacted AND the mutable side of the LSM tree. This is
   * what keeps the fix honest for the layouts no hand-written shape thought of.
   */
  @ParameterizedTest(name = "seed={0}")
  @ValueSource(ints = { 1, 2, 5, 6, 8, 9, 10, 14, 16 })
  void everyPrefixScanMatchesAnInMemoryOracle(final int seed) {
    final Random rnd = new Random(seed);
    final int buckets = 1 + rnd.nextInt(4);
    final int pageSize = new int[] { 4096, 8192, 65536 }[rnd.nextInt(3)];
    final int keyCount = 3 + rnd.nextInt(12);
    final int totalRows = 300 + rnd.nextInt(3000);
    final String pad = "q".repeat(rnd.nextInt(80));

    final VertexType supplier = database.getSchema().createVertexType("Supplier", buckets);
    supplier.createProperty("key1", Type.STRING);
    supplier.createProperty("key2", Type.STRING);
    supplier.createProperty("orderedAt", Type.LONG);
    supplier.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, new String[] { "key1", "key2", "orderedAt" }, pageSize);

    final List<Row> oracle = new ArrayList<>();
    final int firstBatch = totalRows / 2;

    database.transaction(() -> {
      for (int i = 0; i < firstBatch; i++)
        oracle.add(insertRandomRow(rnd, keyCount, pad));
    });

    compact();

    // The second half stays in the mutable pages, so every scan has to merge both sides.
    database.transaction(() -> {
      for (int i = firstBatch; i < totalRows; i++)
        oracle.add(insertRandomRow(rnd, keyCount, pad));
    });

    if (rnd.nextBoolean())
      compact();

    for (int probe = 0; probe < 12; probe++) {
      final String key1 = "k" + pad + rnd.nextInt(keyCount);
      final String key2 = "g" + pad + rnd.nextInt(3);

      final List<Long> expected = oracle.stream()//
          .filter(r -> r.key1().equals(key1) && r.key2().equals(key2))//
          .map(Row::orderedAt)//
          .sorted(Comparator.reverseOrder())//
          .toList();

      database.transaction(() -> {
        final List<Long> descending = new ArrayList<>();
        for (final Vertex v : database.select().fromType("Supplier")//
            .where().property("key1").eq().value(key1)//
            .and().property("key2").eq().value(key2)//
            .orderBy("orderedAt", false)//
            .compile().vertices().toList()) {
          assertThat(v.getString("key1")).as("row outside the matched group").isEqualTo(key1);
          assertThat(v.getString("key2")).as("row outside the matched group").isEqualTo(key2);
          descending.add(v.getLong("orderedAt"));
        }
        assertThat(descending).as("DESC on " + key1 + "/" + key2).isEqualTo(expected);

        final Vertex newest = database.select().fromType("Supplier")//
            .where().property("key1").eq().value(key1)//
            .and().property("key2").eq().value(key2)//
            .orderBy("orderedAt", false).limit(1)//
            .compile().vertices().nextOrNull();
        if (expected.isEmpty())
          assertThat(newest).as("LIMIT 1 on an empty group").isNull();
        else
          assertThat(newest.getLong("orderedAt")).as("LIMIT 1 on " + key1 + "/" + key2).isEqualTo(expected.get(0));

        final List<Long> ascending = new ArrayList<>();
        for (final Vertex v : database.select().fromType("Supplier")//
            .where().property("key1").eq().value(key1)//
            .and().property("key2").eq().value(key2)//
            .orderBy("orderedAt", true)//
            .compile().vertices().toList())
          ascending.add(v.getLong("orderedAt"));
        assertThat(ascending).as("ASC on " + key1 + "/" + key2).isEqualTo(reversedCopy(expected));
      });
    }
  }

  private Row insertRandomRow(final Random rnd, final int keyCount, final String pad) {
    final Row row = new Row("k" + pad + rnd.nextInt(keyCount), "g" + pad + rnd.nextInt(3), rnd.nextInt(100_000));
    database.newVertex("Supplier").set("key1", row.key1(), "key2", row.key2(), "orderedAt", row.orderedAt()).save();
    return row;
  }

  private void compact() {
    final TypeIndex typeIndex = database.getSchema().getType("Supplier").getAllIndexes(false).iterator().next();
    try {
      assertThat(((IndexInternal) typeIndex).scheduleCompaction()).as("compaction scheduled").isTrue();
      assertThat(((IndexInternal) typeIndex).compact()).as("compaction executed").isTrue();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static long epochMillis(final Vertex vertex) {
    final Object value = vertex.get("ordered_at");
    return value instanceof LocalDateTime dateTime ? dateTime.toInstant(ZoneOffset.UTC).toEpochMilli() : ((Date) value).getTime();
  }

  /**
   * {@code List.reversed()} arrived with {@code SequencedCollection} in JDK 21; on this branch the reversal is an
   * explicit copy, which is what the assertion wanted anyway - an independent list, not a view.
   */
  private static List<Long> reversedCopy(final List<Long> source) {
    final List<Long> reversed = new ArrayList<>(source);
    Collections.reverse(reversed);
    return reversed;
  }

}
