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
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/6694: {@code LSMTreeIndexCompacted.newIterators()}'s
 * ROOT-PAGE lookup for a partial (composite-index prefix) key always used purpose=2 (ascending), regardless of the
 * requested scan direction. #6692 fixed the analogous asymmetry at the DATA-page level ({@code searchInCurrentPage()},
 * which already used {@code ascendingOrder ? 2 : 3}), but left the coarser root-page probe that PICKS which data page
 * to start {@code searchInCurrentPage()} from always ascending.
 * <p>
 * This only surfaces when the composite-index prefix match ({@code key1='a' AND key2='b'}) itself spans MULTIPLE data
 * pages within one compacted series - i.e. a group large enough that the root page holds a RUN of several
 * page-boundary entries that all compare equal under a partial-key comparison. {@code compareKey()}'s "PARTIAL
 * MATCHING" walk resolves an ambiguous binary-search landing point to the FIRST entry of that run for purpose=2 and
 * the LAST entry for purpose=3; using purpose=2 unconditionally means a DESCENDING scan's root-page probe always
 * lands on the FIRST (lowest-keyed) data page of the group instead of the LAST (highest-keyed) one, so the scan
 * starts multiple pages too low and returns the wrong (or incomplete) rows.
 * <p>
 * The existing #6692 regression test ({@code Issue6592CompactedCompositePrefixDescTest}) used a target group small
 * enough to live in a single data page, so it never exercised this specific branch - confirmed by the automated code
 * review comment that filed this issue.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue6694CompactedRootPageDescendingPurposeTest extends TestHelper {

  private static final int GROUP_SIZE = 4_000;
  private static final int PAGE_SIZE  = 8 * 1024;

  @Override
  protected void beginTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);

    final VertexType supplier = database.getSchema().createVertexType("Supplier", 1);
    supplier.createProperty("key1", Type.STRING);
    supplier.createProperty("key2", Type.STRING);
    supplier.createProperty("orderedAt", Type.LONG);
    database.getSchema()
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Supplier", new String[] { "key1", "key2", "orderedAt" }, PAGE_SIZE);

    database.transaction(() -> {
      // A single prefix group large enough to span several data pages within one compacted series - the root page
      // then holds a run of multiple page-boundary entries that all compare equal under the partial key (key1,key2).
      for (int i = 0; i < GROUP_SIZE; i++)
        database.newVertex("Supplier").set("key1", "a", "key2", "b", "orderedAt", (long) i).save();
    });

    final TypeIndex typeIndex = database.getSchema().getType("Supplier").getIndexesByProperties("key1", "key2", "orderedAt")
        .getFirst();
    try {
      assertThat(((IndexInternal) typeIndex).scheduleCompaction()).as("compaction scheduled").isTrue();
      assertThat(((IndexInternal) typeIndex).compact()).as("compaction executed").isTrue();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void orderByDescLimitOneReturnsTheHighestValueAfterCompaction() {
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

  @Test
  void orderByAscLimitOneStillReturnsTheLowestValueAfterCompaction() {
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
}
