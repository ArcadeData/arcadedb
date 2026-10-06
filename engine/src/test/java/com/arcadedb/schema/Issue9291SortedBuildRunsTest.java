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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #9291 on the multi-run paths of the sorted build: a low-cardinality prefix grows the run past the
 * default limit, a high-cardinality tail then overflows the shared-key table (the run falls back to comparing keys), and a run of
 * mostly distinct keys never shares at all. The sorted index must hold what the default build holds, in the same key order, and the
 * {@code buildMode} directive of SQL must really select the sorted build.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9291SortedBuildRunsTest extends TestHelper {
  private static final int LOW_CARDINALITY_ROWS = 100_000;
  private static final int DISTINCT_ROWS        = 100_000;

  @Test
  void spilledRunsWithSharingThatEndsMidRunMatchTheDefaultBuild() {
    for (final String type : new String[] { "Sorted", "Plain" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".k STRING");
      database.begin();
      for (int i = 0; i < LOW_CARDINALITY_ROWS + DISTINCT_ROWS; i++) {
        final String key = i < LOW_CARDINALITY_ROWS ? "low-%02d".formatted(i % 37) : "u-%08d".formatted(Math.floorMod(i * 7919, 1_000_003));
        database.newDocument(type).set("k", key).save();
        if (i % 20_000 == 19_999) {
          database.commit();
          database.begin();
        }
      }
      database.commit();
    }

    final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
    TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
    try {
      // 128 MB lets a run hold 43,690 entries, so the 32,768-entry probe is reached before the first spill
      database.getSchema().buildTypeIndex("Sorted", new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED).withBuildMemoryBudget(128L << 20).withUnique(false).create();
    } finally {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }
    database.command("sql", "CREATE INDEX ON Plain (k) NOTUNIQUE");

    assertThat(captured.get()).as("the sorted build ran").isNotNull();
    assertThat(captured.get().initialRuns()).as("the build spilled").isGreaterThan(0);
    assertThat(captured.get().logicalEntries()).isEqualTo(LOW_CARDINALITY_ROWS + DISTINCT_ROWS);

    final List<String> sorted = entries(database.getSchema().getType("Sorted").getPolymorphicIndexByProperties("k"));
    final List<String> plain = entries(database.getSchema().getType("Plain").getPolymorphicIndexByProperties("k"));
    assertThat(sorted).hasSize(LOW_CARDINALITY_ROWS + DISTINCT_ROWS);
    assertThat(keysOnly(sorted)).as("key order").isEqualTo(keysOnly(plain));
    assertThat(sorted.stream().sorted().toList()).as("entries").isEqualTo(plain.stream().sorted().toList());
  }

  @Test
  void defaultBuildModeDoesNotUseTheSortedBuild() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.k STRING");
    database.transaction(() -> database.newDocument("D").set("k", "a").save());
    final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
    TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
    try {
      database.command("sql", "CREATE INDEX ON D (k) NOTUNIQUE METADATA {\"buildMode\": \"DEFAULT\"}");
    } finally {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }
    assertThat(captured.get()).isNull();
  }

  @Test
  void sortedBuildModeDirectiveSelectsTheSortedBuild() {
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.k STRING");
    database.transaction(() -> database.newDocument("S").set("k", "a").save());
    final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
    TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
    try {
      database.command("sql", "CREATE INDEX ON S (k) NOTUNIQUE METADATA {\"buildMode\": \"SORTED\"}");
    } finally {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }
    assertThat(captured.get()).isNotNull();
  }

  private static List<String> keysOnly(final List<String> entries) {
    return entries.stream().map(e -> e.substring(0, e.indexOf('@'))).toList();
  }

  private static List<String> entries(final TypeIndex index) {
    final List<String> out = new ArrayList<>();
    final IndexCursor cursor = index.iterator(true);
    while (cursor.hasNext()) {
      final RID rid = cursor.next().getIdentity();
      out.add(Arrays.toString(cursor.getKeys()) + "@" + rid.getPosition());
    }
    return out;
  }
}
