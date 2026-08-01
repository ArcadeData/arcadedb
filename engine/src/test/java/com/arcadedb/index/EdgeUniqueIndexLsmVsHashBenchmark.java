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
package com.arcadedb.index;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compares the two index implementations available for the guard a loader puts on edge creation: a UNIQUE index
 * on the edge type, which lets the engine reject a duplicate instead of every insert having to query for one.
 * <p>
 * The interesting axis is write cost, not lookup cost. Both answer a dedup probe with a full-key equality lookup,
 * which is all this key shape is ever asked; but they pay very differently to accept an insert.
 * {@code LSMTreeIndexMutable} keeps a sorted pointer array per page and memmoves it on every insert, and that array
 * grows with the page, so each inserted key rewrites a slice of the page and the WAL carries it. Extendible hashing
 * has no ordering to maintain, and {@code HashIndex.compact()} is a no-op because there is nothing to merge.
 * <p>
 * The key is {@code (@out, @in)}, the endpoint pair a de-duplication guard actually wants. HASH could not index
 * that until #5677 taught it to store a LINK key compressed.
 * <p>
 * The numbers that matter are WAL bytes per edge and ingest rate. The read side is deliberately not measured:
 * neither implementation is ever asked for a range scan on this key.
 * <p>
 * <b>Measured answer: keep LSM for this guard.</b> At 200k edges in creation order LSM runs it in ~1.8 s against
 * HASH's ~4.0 s, and the shuffle flag says why: shuffling costs LSM ~2.5x (1.8 s -> 4.5 s) while barely moving
 * HASH (~4.0 s -> ~5.9 s, and its WAL is flat at ~2.9 KB/edge either way, since hashing scatters regardless).
 * The keys here are monotonic - one hub, destinations created in order - so the LSM insertion point sits at the
 * tail and the sorted pointer array hardly shifts. That is the case where the per-insert memmove is cheapest, not
 * costliest, which is the opposite of what the raw structure of the code suggests. HASH loses even shuffled.
 * <p>
 * The WAL figure is the total for the database's whole life, not a delta over the edge phase. {@code bytesWritten}
 * only accumulates into the long-lived counter when a WAL file is <i>retired</i>, so a windowed delta jumps by a
 * whole file whenever the cleaner happens to run inside the window - which made an earlier version of this
 * benchmark report values that were bimodal and swapped between the two implementations from run to run. Both
 * databases do identical work apart from the index type, so comparing totals is sound where comparing deltas
 * was not. It is still lumpy enough across runs that only its order of magnitude should be read - the elapsed
 * time is the stable metric, and both agree on the direction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class EdgeUniqueIndexLsmVsHashBenchmark {
  /** Distinct destination vertices, and therefore distinct edges: one per (hub, leaf) pair, each with a unique ref. */
  private static final int EDGES         = Integer.parseInt(System.getProperty("edges", "50000"));
  /** Edges per transaction, so the WAL cost is amortised the way a batching loader would amortise it. */
  private static final int EDGES_PER_TX  = Integer.parseInt(System.getProperty("edgesPerTx", "100"));
  /**
   * Whether the destination vertices are linked in creation order or shuffled. This is the axis that decides the
   * comparison: a hub's edges keyed on {@code (@out, @in)} have a constant first column and a second that climbs
   * with the RID, so in creation order the key is effectively monotonic - LSM's best case, since the insertion
   * point is at the tail and the sorted pointer array barely shifts, and hash's worst case, since consecutive keys
   * scatter across the whole directory. Shuffling removes that advantage and shows how much of the gap it was.
   */
  private static final boolean SHUFFLE   = Boolean.parseBoolean(System.getProperty("shuffle", "false"));
  /** Page size for the HASH index; 0 leaves its own default (65_536) against LSM's 262_144. */
  private static final int     HASH_PAGE = Integer.parseInt(System.getProperty("hashPageSize", "0"));
  /** Dedup probes replayed after the load, to measure the lookup side the guard actually performs. */
  private static final int     PROBES    = Integer.parseInt(System.getProperty("probes", "100000"));

  @Test
  void comparesTheWriteCostOfLsmAndHashForTheDedupGuard() {
    final Result lsm = measure(Schema.INDEX_TYPE.LSM_TREE);
    final Result hash = measure(Schema.INDEX_TYPE.HASH);

    // System.out, not LogManager: a benchmark's numbers ARE its output, and LogManager INFO does not
    // surface in a surefire run, which makes the measurement invisible.
    System.out.printf("edges=%d perTx=%d shuffle=%b hashPage=%d%n  %-9s insert %5d ms  probe %5d ms  %8.1f B/edge  %5.2f pages/edge%n  %-9s insert %5d ms  probe %5d ms  %8.1f B/edge  %5.2f pages/edge%n",
        EDGES, EDGES_PER_TX, SHUFFLE, HASH_PAGE,
        "LSM_TREE", lsm.elapsedMs, lsm.probeMs, lsm.bytesPerEdge(), lsm.pagesPerEdge(),
        "HASH", hash.elapsedMs, hash.probeMs, hash.bytesPerEdge(), hash.pagesPerEdge());

    // Both must have actually indexed every edge - a cheaper index that dropped entries would be no bargain.
    assertThat(lsm.indexed).isEqualTo(EDGES);
    assertThat(hash.indexed).isEqualTo(EDGES);
    // ...and both must still reject a duplicate, which is the whole point of the guard.
    assertThat(lsm.rejectedDuplicate).isTrue();
    assertThat(hash.rejectedDuplicate).isTrue();
  }

  private Result measure(final Schema.INDEX_TYPE indexType) {
    final String path = "./target/databases/edgeUniqueIdx" + indexType;
    final DatabaseFactory factory = new DatabaseFactory(path);
    if (factory.exists())
      factory.open().drop();

    final Database database = factory.create();
    try {
      final List<Vertex> leaves = new ArrayList<>(EDGES);
      final Vertex[] hub = new Vertex[1];

      database.transaction(() -> {
        database.getSchema().createVertexType("Account");
        database.getSchema().createEdgeType("INITIATED");
        final var builder = database.getSchema().buildTypeIndex("INITIATED", new String[] { "@out", "@in" })
            .withType(indexType).withUnique(true);
        if (HASH_PAGE > 0 && indexType == Schema.INDEX_TYPE.HASH)
          builder.withPageSize(HASH_PAGE);
        builder.create();
      });

      // Vertices first and in their own transactions: only the edge phase is measured, so the cost of
      // creating the endpoints must not land in the numbers.
      database.transaction(() -> hub[0] = database.newVertex("Account").set("code", "HUB").save());
      for (int i = 0; i < EDGES; i += EDGES_PER_TX) {
        final int from = i;
        database.transaction(() -> {
          for (int j = from; j < Math.min(from + EDGES_PER_TX, EDGES); j++)
            leaves.add(database.newVertex("Account").set("code", "L" + j).save());
        });
      }

      if (SHUFFLE)
        java.util.Collections.shuffle(leaves, new java.util.Random(42));

      final long begin = System.nanoTime();

      for (int i = 0; i < EDGES; i += EDGES_PER_TX) {
        final int from = i;
        database.transaction(() -> {
          final MutableVertex source = hub[0].modify();
          for (int j = from; j < Math.min(from + EDGES_PER_TX, EDGES); j++)
            source.newEdge("INITIATED", leaves.get(j)).save();
        });
      }

      final long elapsedMs = (System.nanoTime() - begin) / 1_000_000;

      // Read side: the dedup probe itself. Page size trades insert cost against lookup cost, so measuring
      // only the write half would recommend a default on half the evidence.
      final java.util.Random rnd = new java.util.Random(7);
      final var index = database.getSchema().getType("INITIATED").getAllIndexes(true).iterator().next();
      final long probeBegin = System.nanoTime();
      long hits = 0;
      for (int i = 0; i < PROBES; i++) {
        final var cursor = index.get(new Object[] { hub[0].getIdentity(), leaves.get(rnd.nextInt(EDGES)).getIdentity() });
        if (cursor.hasNext()) {
          cursor.next();
          hits++;
        }
      }
      final long probeMs = (System.nanoTime() - probeBegin) / 1_000_000;
      assertThat(hits).isEqualTo(PROBES);
      final long walTotal = walBytes(database);
      final long pageTotal = walPages(database);

      final long indexed = database.getSchema().getType("INITIATED").getAllIndexes(true).stream()
          .mapToLong(IndexInternal::countEntries).sum();

      // The guard has to still fire: re-inserting an existing pair must be refused.
      boolean rejected = false;
      try {
        database.transaction(() -> hub[0].modify().newEdge("INITIATED", leaves.getFirst()).save());
      } catch (final DuplicatedKeyException e) {
        rejected = true;
      }

      return new Result(elapsedMs, probeMs, walTotal, pageTotal, indexed, rejected);
    } finally {
      database.drop();
    }
  }

  private static long walBytes(final Database database) {
    return (Long) ((DatabaseInternal) database).getTransactionManager().getStats().get("bytesWritten");
  }

  /** Pages the WAL carried. Separates "dirties many pages" from "writes a big range in few pages". */
  private static long walPages(final Database database) {
    return (Long) ((DatabaseInternal) database).getTransactionManager().getStats().get("pagesWritten");
  }

  private record Result(long elapsedMs, long probeMs, long walBytes, long walPages, long indexed, boolean rejectedDuplicate) {
    double bytesPerEdge() {
      return walBytes / (double) EDGES;
    }

    double pagesPerEdge() {
      return walPages / (double) EDGES;
    }
  }
}
