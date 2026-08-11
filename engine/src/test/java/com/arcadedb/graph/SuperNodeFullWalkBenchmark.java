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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.PageManager;
import com.arcadedb.log.LogManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6048: measures the full-walk cost {@code GRAPH_SUPERNODE_INTERLEAVE_ROUNDS} trades off. Interleaving the
 * stripe chains of a promoted super-node (#6044) keeps one resident chunk page PER STRIPE instead of one, and
 * hops between {@code stripes} files every {@code stripes} entries - free when the page cache holds the whole
 * super-node, but a real eviction cost once it does not: an interleaved walk's working set is {@code stripes}
 * pages wide, so it evicts (and later re-faults) at {@code stripes}x the rate a one-cursor-at-a-time walk does
 * under the same cache budget.
 * <p>
 * The primary metric is {@link PageManager.PPageManagerStats#cacheMiss} and {@code pagesEvicted}, not wall
 * time: on modern NVMe storage backed by a generous OS page cache, a dataset small enough to run in a unit test
 * fits comfortably in the OS cache regardless of ArcadeDB's own {@code MAX_PAGE_RAM} budget, so wall-clock
 * differences at this scale are largely noise - the effect this benchmark is chasing only shows up at a scale
 * where {@code MAX_PAGE_RAM} actually has to evict, which is exactly what the cache-miss counters measure
 * directly and deterministically instead of hoping the underlying storage is slow enough to notice. Wall time
 * is reported alongside anyway, and does move the same direction as the counters at this fixture's size.
 * <p>
 * {@code MAX_PAGE_RAM} is forced down to 1 MB - its smallest granularity, and, at {@code BUCKET_DEFAULT_PAGE_SIZE}'s
 * 64 KB default, on the order of the {@code stripes} count itself - specifically to land in the regime where a
 * concatenated walk's steady-state working set fits and an interleaved one's does not; the {@code default}
 * profile's 4 GB would fit the whole fixture in RAM and produce zero misses regardless of configuration. The
 * database is reopened before every timed walk so each one starts from a cold ArcadeDB-level page cache rather
 * than one left warm by a previous config's walk or by insertion.
 * <p>
 * Sample run (200,000 edges, 16 stripes, 1 MB cache, 3 repeats averaged): unbounded interleaving (the #6044/#6047
 * behaviour with no #6048 degrade) cost 14 evictions and 41 cache misses per walk; the {@code rounds=64} default
 * this change introduces cost 0 evictions and 34 misses - the theoretical floor, since 34 is the number of
 * distinct pages the data occupies. Wall time went from ~28 ms to ~16 ms accordingly. At only single-digit-to-tens
 * absolute counts and 3 repeats this is a small sample, not a precision measurement, but the direction and the
 * "34 misses = no re-reads" floor are unambiguous. Whoever tunes {@code supernodeInterleaveRounds} against a
 * production degree distribution and real storage should extend this harness rather than trust these numbers as
 * exact.
 * <p>
 * Tagged {@code benchmark} so it is skipped from regular CI builds. Run explicitly with:
 * {@code mvn -pl engine test -Dtest=SuperNodeFullWalkBenchmark -DexcludedGroups=}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class SuperNodeFullWalkBenchmark extends TestHelper {
  private static final int  STRIPES       = 16;
  private static final int  THRESHOLD     = 100;
  private static final int  EDGES         = 200_000;
  // MAX_PAGE_RAM is denominated in MB, not bytes (PageManager multiplies by 1024*1024 itself) - 1 is the
  // smallest granularity available and, at BUCKET_DEFAULT_PAGE_SIZE's default (64 KB), holds ~16 pages: enough
  // for a concatenated walk's steady-state working set (one bucket's current chunk plus a handful of
  // schema/directory pages) but NOT an interleaved walk's (one resident chunk PER STRIPE plus the same
  // schema/directory overhead) - exactly the regime where the #6048 locality argument produces an observable
  // cache-miss delta.
  private static final long PAGE_CACHE_MB  = 1L;

  @Override
  protected String getPerformanceProfile() {
    // The 'default' profile's page cache (4 GB) would fit this whole fixture and hide the eviction cost this
    // benchmark exists to measure; 'low-ram' is the existing profile that constrains MAX_PAGE_RAM, further
    // tightened below.
    return "low-ram";
  }

  @Test
  void fullWalkAtVariousInterleaveRoundBudgets() throws IOException {
    GlobalConfiguration.MAX_PAGE_RAM.setValue(PAGE_CACHE_MB);
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);

    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 16);
      database.getSchema().createEdgeType("LINK", 16);
    });

    final MutableVertex[] hubHolder = new MutableVertex[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub");
      hub.save();
      hubHolder[0] = hub;
    });
    final RID hubRID = hubHolder[0].getIdentity();

    // One big transaction: insertion speed is not what this benchmark measures.
    database.transaction(() -> {
      for (int i = 0; i < EDGES; i++) {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        src.newEdge("LINK", hubRID);
      }
    });

    assertThat(loadHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final StringBuilder report = new StringBuilder();
    report.append("======== Super-node full-walk benchmark (interleave degrade) ========\n");
    report.append("edges                : %d%n".formatted(EDGES));
    report.append("stripes              : %d%n".formatted(STRIPES));
    report.append("page cache           : %d MB%n".formatted(PAGE_CACHE_MB));

    report.append(("%-10s %10s %10s %10s %10s%n").formatted("rounds", "elapsed ms", "cacheMiss", "evicted", "pagesRead"));

    // "always interleave" (the #6044/#6047 behaviour with no #6048 degrade), the new default, and "never
    // interleave" (immediate concatenation, the pre-#6044 order): one cold run each, averaged over a few
    // repeats. Cold means reopened right before the walk, so every run starts from the same empty
    // ArcadeDB-level page cache rather than one left warm by the previous config's walk or by insertion.
    for (final int rounds : new int[] { Integer.MAX_VALUE, 64, 0 }) {
      GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(rounds);

      final int repeats = 3;
      long totalMs = 0, totalCacheMiss = 0, totalEvicted = 0, totalPagesRead = 0, visited = 0;
      for (int run = 0; run < repeats; run++) {
        reopenDatabase();
        final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
        final PageManager.PPageManagerStats before = pageManager.getStats();

        // A RID captured before reopenDatabase() still refers to the CLOSED pre-reopen Database instance
        // (DatabaseIsClosedException on asVertex()); rebuild a plain RID bound to the current one.
        final RID hub = new RID(hubRID.getBucketId(), hubRID.getPosition());
        final long begin = System.currentTimeMillis();
        long count = 0;
        for (final Iterator<Vertex> it = hub.asVertex(true).getVertices(Vertex.DIRECTION.IN, "LINK").iterator(); it.hasNext(); it.next())
          count++;
        totalMs += System.currentTimeMillis() - begin;

        final PageManager.PPageManagerStats after = pageManager.getStats();
        totalCacheMiss += after.cacheMiss - before.cacheMiss;
        totalEvicted += after.pagesEvicted - before.pagesEvicted;
        totalPagesRead += after.pagesRead - before.pagesRead;
        visited = count;
      }
      assertThat(visited).isEqualTo(EDGES);

      report.append(("rounds=%-3s %10.1f %10d %10d %10d%n").formatted(
          rounds == Integer.MAX_VALUE ? "inf" : String.valueOf(rounds), totalMs / (double) repeats, totalCacheMiss / repeats,
          totalEvicted / repeats, totalPagesRead / repeats));
    }
    report.append("=======================================================================");

    LogManager.instance().log(this, Level.INFO, report.toString());
    Files.writeString(new File("./target/supernode-fullwalk-benchmark.txt").toPath(), report + System.lineSeparator(),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  private Record loadHead(final RID hubRID) {
    final Record[] head = new Record[1];
    database.transaction(() -> {
      final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
      head[0] = database.lookupByRID(headRID, true);
    });
    return head[0];
  }
}
