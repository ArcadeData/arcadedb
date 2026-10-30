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
package com.arcadedb.graph.olap;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11011: the page cache is on the heap, and a view build read every page of the graph into it, so after the build
 * the database held the graph twice, once as the view and once as cached pages nobody read again. The pages a build loads
 * leave the cache when it ends, the pages the cache held before it stay, and the build sees every hit it saw before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11011GavBuildReleasesPagesTest extends TestHelper {
  private static final int  VERTICES  = 20_000;
  private static final int  EDGES     = 120_000;
  private static final int  HOT       = 200;
  // A few pages of slack for what the build's own transaction and the schema read on the way
  private static final long TOLERANCE = 4L * 64 * 1024;

  @Test
  void theBuildLeavesThePageCacheAsItFoundIt() {
    final List<RID> vertices = createGraph();
    reopenDatabase();
    readVertices(vertices.subList(0, HOT));

    final long before = PageManager.INSTANCE.getReadCacheRAM();
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("pages").withVertexTypes("V")
        .withEdgeTypes("E").build();
    try {
      assertThat(view.getEdgeCount()).isEqualTo(EDGES);
      assertThat(PageManager.INSTANCE.getReadCacheRAM() - before).isLessThan(TOLERANCE);

      // The pages that were hot before the build are still in the cache
      final long missesBefore = PageManager.INSTANCE.getStats().cacheMiss;
      readVertices(vertices.subList(0, HOT));
      assertThat(PageManager.INSTANCE.getStats().cacheMiss - missesBefore).isZero();
    } finally {
      view.drop();
    }
  }

  @Test
  void withTheSettingOffTheBuildKeepsThePagesItRead() {
    final List<RID> vertices = createGraph();
    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.GAV_BUILD_RELEASES_PAGES, false);
    readVertices(vertices.subList(0, HOT));

    final long before = PageManager.INSTANCE.getReadCacheRAM();
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("pages").withVertexTypes("V")
        .withEdgeTypes("E").build();
    try {
      assertThat(view.getEdgeCount()).isEqualTo(EDGES);
      assertThat(PageManager.INSTANCE.getReadCacheRAM() - before).isGreaterThan(8 * TOLERANCE);
    } finally {
      view.drop();
    }
  }

  @Test
  void aPageAnotherReaderUsesDuringTheScanStaysCached() throws Exception {
    final List<RID> vertices = createGraph();
    reopenDatabase();

    final long before = PageManager.INSTANCE.getReadCacheRAM();
    final PageManager.BulkScan scan = PageManager.INSTANCE.beginBulkScan();
    try {
      readVertices(vertices);
      // Another thread reads the first vertices while the scan is open: their pages are now wanted by someone else
      final Thread reader = new Thread(() -> readVertices(vertices.subList(0, HOT)));
      reader.start();
      reader.join();
    } finally {
      scan.close();
    }

    assertThat(scan.getReleasedPages()).isPositive();
    // What stayed is the pages the other reader used, a small part of what the scan read
    final long kept = PageManager.INSTANCE.getReadCacheRAM() - before;
    assertThat(kept).isPositive().isLessThan(scan.getReleasedBytes());
    final long missesBefore = PageManager.INSTANCE.getStats().cacheMiss;
    readVertices(vertices.subList(0, HOT));
    assertThat(PageManager.INSTANCE.getStats().cacheMiss - missesBefore).isZero();
  }

  private List<RID> createGraph() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    final List<RID> vertices = new ArrayList<>(VERTICES);
    final Random random = new Random(11011);
    database.transaction(() -> {
      final List<MutableVertex> created = new ArrayList<>(VERTICES);
      for (int i = 0; i < VERTICES; i++) {
        final MutableVertex v = database.newVertex("V").set("id", i, "name", "vertex-" + i).save();
        created.add(v);
        vertices.add(v.getIdentity());
      }
      for (int e = 0; e < EDGES; e++)
        created.get(random.nextInt(VERTICES)).newEdge("E", created.get(random.nextInt(VERTICES)));
    });
    return vertices;
  }

  private void readVertices(final List<RID> rids) {
    database.transaction(() -> {
      // Looked up through the reopened database: a RID keeps the instance it was created with
      for (final RID rid : rids)
        assertThat(database.lookupByRID(rid, true).asVertex().getInteger("id")).isNotNull();
    });
  }
}
