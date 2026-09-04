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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Follow-up regression test for https://github.com/ArcadeData/arcadedb/issues/6592: once the composite (multi-property)
 * index in {@link SelectCompositeIndexTest} is actually used by the native {@link Select} query builder, a genuine bug
 * surfaces on an index that has gone through LSM compaction (any long-running database) when the query does an
 * {@code ORDER BY <trailing property> DESC} on a composite-index prefix match.
 * <p>
 * Two independent defects in {@link com.arcadedb.index.lsm.LSMTreeIndexCompacted}, both only reachable through a
 * PARTIAL (prefix) key - i.e. a composite index where not every property is bound by equality - combined to produce
 * this:
 * <ol>
 * <li>{@code compareKey()} never adjusted the binary-search {@code mid} to the boundary of the matching-prefix run
 * (unlike {@code LSMTreeIndexMutable.compareKey()}, which does), so a descending partial-key scan could start
 * anywhere inside the group instead of at its highest entry;</li>
 * <li>{@code searchInCurrentPage()}'s "the search key is outside this data page" fallback always stepped to
 * {@code firstPageNumber + 1}, which is only correct for an ASCENDING scan (where "outside" only ever means the page
 * sorts below the search key). For a DESCENDING scan "outside" only ever means the page sorts ABOVE the search key,
 * so the fallback needs to step to {@code firstPageNumber - 1} instead - stepping the wrong way walked the scan into
 * an unrelated, higher-keyed page (possibly belonging to a different compacted series entirely) and returned
 * whatever it found there as if it matched the WHERE clause.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue6592CompactedCompositePrefixDescTest extends TestHelper {

  private static final int    NOISE_GROUPS = 12;
  private static final int    NOISE_ROWS   = 2_500;
  private static final int    GROUP_SIZE   = 5;
  private static final String PAD          = "x".repeat(64);

  @Override
  protected void beginTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);

    final VertexType supplier = database.getSchema().createVertexType("Supplier", 1);
    supplier.createProperty("key1", Type.STRING);
    supplier.createProperty("key2", Type.STRING);
    supplier.createProperty("orderedAt", Type.LONG);
    supplier.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "key1", "key2", "orderedAt");

    database.transaction(() -> {
      // Noise groups both alphabetically BEFORE and AFTER the target "a"/"b" group, spread across enough data to
      // force several compacted series (see buildMultiSeriesIndex() in Issue5214MultiSeriesRangeTest for the same
      // RAM-bounded-compaction technique). Groups before "a" exercise the descending "outside" fallback (#6592
      // follow-up); groups after it exercise the pre-existing ascending fallback, which must stay correct.
      for (int g = 0; g < NOISE_GROUPS; g++) {
        final String key1 = (g % 2 == 0 ? "0" : "z") + PAD + g;
        for (int i = 0; i < NOISE_ROWS; i++)
          database.newVertex("Supplier").set("key1", key1, "key2", "x", "orderedAt", (long) i).save();
      }
      for (int i = 0; i < GROUP_SIZE; i++)
        database.newVertex("Supplier").set("key1", "a", "key2", "b", "orderedAt", (long) i).save();
    });

    final TypeIndex typeIndex = database.getSchema().getType("Supplier").getIndexesByProperties("key1", "key2", "orderedAt")
        .get(0);
    try {
      assertThat(((IndexInternal) typeIndex).scheduleCompaction()).as("compaction scheduled").isTrue();
      assertThat(((IndexInternal) typeIndex).compact()).as("compaction executed").isTrue();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void orderByDescLimitOneReturnsTheMatchingGroupAfterCompaction() {
    database.transaction(() -> {
      final SelectCompiled select = database.select().fromType("Supplier")//
          .where().property("key1").eq().value("a")//
          .and().property("key2").eq().value("b")//
          .orderBy("orderedAt", false)//
          .limit(1)//
          .compile();

      final Vertex first = select.vertices().nextOrNull();

      assertThat(first).isNotNull();
      assertThat(first.getString("key1")).isEqualTo("a");
      assertThat(first.getString("key2")).isEqualTo("b");
      assertThat(first.getLong("orderedAt")).isEqualTo(GROUP_SIZE - 1);
    });
  }

  @Test
  void orderByAscLimitOneStillReturnsTheMatchingGroupAfterCompaction() {
    database.transaction(() -> {
      final SelectCompiled select = database.select().fromType("Supplier")//
          .where().property("key1").eq().value("a")//
          .and().property("key2").eq().value("b")//
          .orderBy("orderedAt", true)//
          .limit(1)//
          .compile();

      final Vertex first = select.vertices().nextOrNull();

      assertThat(first).isNotNull();
      assertThat(first.getString("key1")).isEqualTo("a");
      assertThat(first.getString("key2")).isEqualTo("b");
      assertThat(first.getLong("orderedAt")).isZero();
    });
  }

  @Test
  void descendingScanReturnsExactlyTheMatchingGroupInOrder() {
    database.transaction(() -> {
      final SelectCompiled select = database.select().fromType("Supplier")//
          .where().property("key1").eq().value("a")//
          .and().property("key2").eq().value("b")//
          .orderBy("orderedAt", false)//
          .compile();

      final List<Vertex> list = select.vertices().toList();

      assertThat(list).hasSize(GROUP_SIZE);
      for (int i = 0; i < GROUP_SIZE; i++) {
        assertThat(list.get(i).getString("key1")).isEqualTo("a");
        assertThat(list.get(i).getString("key2")).isEqualTo("b");
        assertThat(list.get(i).getLong("orderedAt")).isEqualTo(GROUP_SIZE - 1 - i);
      }
    });
  }
}
