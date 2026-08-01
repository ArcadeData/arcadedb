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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sweeps the design space of the HASH index against LSM, to decide two things for issue #5712: whether
 * {@code HashIndexBucket.DEF_PAGE_SIZE} (65_536) is the right default, and how much of the insert cost is the
 * sorted slot directory that the proposed restructure would remove.
 * <p>
 * The axes exist because a single measurement misled this investigation once already. An earlier round compared
 * only one key shape in one order and produced a recommendation that reversed as soon as the order changed, so
 * every dimension that could plausibly flip the answer is swept rather than assumed:
 * <ul>
 *   <li><b>Key shape</b> - a long, a string, and a composite. Key width drives entries-per-page, which is what the
 *       slot-shift cost scales with, so it should move the page-size optimum.</li>
 *   <li><b>Distribution</b> - sequential or random. Sequential is LSM's best case (insertion point at the tail,
 *       almost no array shift); hashing scatters either way, so this isolates how much of LSM's lead is locality
 *       rather than structure.</li>
 *   <li><b>Page size</b> - the dial under test.</li>
 *   <li><b>Probe outcome</b> - hit and <b>miss</b> measured separately. A de-duplication guard mostly misses, and a
 *       miss is the case a bloom filter or a tag scan changes most, so reporting only hits would measure the wrong
 *       operation.</li>
 *   <li><b>Uniqueness</b> - unique and non-unique. Non-unique takes {@code addRIDToExistingEntry}, a different
 *       write path that rewrites an entry in place rather than inserting a slot.</li>
 * </ul>
 * Elapsed times are the trustworthy figures. WAL bytes come from {@code TransactionManager.getStats()}, which only
 * folds a file's counters into the total when that file is retired, so the byte column is lumpy run to run and
 * only its order of magnitude should be read.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class HashIndexDesignSpaceBenchmark {
  private static final int     ENTRIES     = Integer.parseInt(System.getProperty("entries", "200000"));
  private static final int     PER_TX      = Integer.parseInt(System.getProperty("perTx", "100"));
  private static final int     PROBES      = Integer.parseInt(System.getProperty("probes", "100000"));
  /**
   * Comma-separated page sizes for HASH; LSM always runs at its own default. 0 means "the index's own default".
   * <p>
   * The ceiling is 65_536 and not a choice: every offset inside a bucket page is a 16-bit field, so a larger page
   * truncates its slot offsets and corrupts the index (#5713). 262_144 is also useless to pass, because the factory
   * reads that exact value as "unset" and substitutes the 64K default.
   */
  private static final String  PAGE_SIZES  = System.getProperty("pageSizes", "1024,4096,16384,65536");
  private static final boolean NON_UNIQUE  = Boolean.parseBoolean(System.getProperty("nonUnique", "false"));

  private enum KeyShape {
    /** Narrow fixed-width key: the most entries per page, so the longest slot directory. */
    LONG,
    /** Wider variable-width key, closer to a business reference. */
    STRING,
    /** Two columns, the shape an edge de-duplication guard uses. */
    COMPOSITE
  }

  @Test
  void sweepsPageSizeAcrossKeyShapesAndDistributions() {
    System.out.printf("entries=%d perTx=%d probes=%d nonUnique=%b%n", ENTRIES, PER_TX, PROBES, NON_UNIQUE);
    System.out.printf("%-10s %-10s %-9s %8s | %7s %8s %9s %11s %8s%n",
        "keyShape", "dist", "index", "pageSize", "insert", "probeHit", "probeMiss", "WALbytes/e", "pages/e");

    for (final KeyShape shape : KeyShape.values())
      for (final boolean sequential : new boolean[] { true, false }) {
        run(shape, sequential, Schema.INDEX_TYPE.LSM_TREE, 0);
        for (final String pageSize : PAGE_SIZES.split(","))
          run(shape, sequential, Schema.INDEX_TYPE.HASH, Integer.parseInt(pageSize.trim()));
      }
  }

  private void run(final KeyShape shape, final boolean sequential, final Schema.INDEX_TYPE indexType,
      final int pageSize) {
    final String path = "./target/databases/hashDesign_" + shape + "_" + (sequential ? "seq" : "rnd") + "_"
        + indexType + "_" + pageSize;
    final DatabaseFactory factory = new DatabaseFactory(path);
    if (factory.exists())
      factory.open().drop();

    final Database database = factory.create();
    try {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Entry");
        final String[] properties = switch (shape) {
          case LONG -> {
            type.createProperty("k", Type.LONG);
            yield new String[] { "k" };
          }
          case STRING -> {
            type.createProperty("k", Type.STRING);
            yield new String[] { "k" };
          }
          case COMPOSITE -> {
            type.createProperty("a", Type.LONG);
            type.createProperty("b", Type.LONG);
            yield new String[] { "a", "b" };
          }
        };
        final IndexBuilderAccessor builder = new IndexBuilderAccessor(database, properties, indexType, pageSize);
        builder.create();
      });

      // The key order the loader presents. Sequential is the interesting case: it is LSM's best and the one a
      // graph loader actually produces, since RIDs climb.
      final List<Integer> order = new ArrayList<>(ENTRIES);
      for (int i = 0; i < ENTRIES; i++)
        order.add(i);
      if (!sequential)
        Collections.shuffle(order, new Random(42));

      final long insertBegin = System.nanoTime();
      for (int i = 0; i < ENTRIES; i += PER_TX) {
        final int from = i;
        database.transaction(() -> {
          for (int j = from; j < Math.min(from + PER_TX, ENTRIES); j++)
            newEntry(database, shape, order.get(j)).save();
        });
      }
      final long insertMs = (System.nanoTime() - insertBegin) / 1_000_000;

      final long walBytes = walStat(database, "bytesWritten");
      final long walPages = walStat(database, "pagesWritten");

      final IndexInternal index = (IndexInternal) database.getSchema().getType("Entry").getAllIndexes(true).iterator().next();
      final Random rnd = new Random(7);

      // Hits: keys that are present.
      final long hitBegin = System.nanoTime();
      long hits = 0;
      for (int i = 0; i < PROBES; i++)
        if (index.get(key(shape, rnd.nextInt(ENTRIES))).hasNext())
          hits++;
      final long hitMs = (System.nanoTime() - hitBegin) / 1_000_000;

      // Misses: keys beyond the loaded range. This is what a de-duplication guard asks almost every time.
      final long missBegin = System.nanoTime();
      long misses = 0;
      for (int i = 0; i < PROBES; i++)
        if (!index.get(key(shape, ENTRIES + rnd.nextInt(ENTRIES))).hasNext())
          misses++;
      final long missMs = (System.nanoTime() - missBegin) / 1_000_000;

      assertThat(hits).isEqualTo(PROBES);
      assertThat(misses).isEqualTo(PROBES);

      System.out.printf("%-10s %-10s %-9s %8s | %5d ms %6d ms %8d ms %11.1f %8.2f%n",
          shape, sequential ? "sequential" : "random", indexType, pageSize == 0 ? "default" : pageSize,
          insertMs, hitMs, missMs, walBytes / (double) ENTRIES, walPages / (double) ENTRIES);
    } finally {
      database.drop();
    }
  }

  private static com.arcadedb.database.MutableDocument newEntry(final Database database, final KeyShape shape,
      final int i) {
    final com.arcadedb.database.MutableDocument doc = database.newDocument("Entry");
    return switch (shape) {
      case LONG -> doc.set("k", (long) i);
      case STRING -> doc.set("k", "key-with-some-width-" + i);
      case COMPOSITE -> doc.set("a", (long) (i % 1000)).set("b", (long) i);
    };
  }

  private static Object[] key(final KeyShape shape, final int i) {
    return switch (shape) {
      case LONG -> new Object[] { (long) i };
      case STRING -> new Object[] { "key-with-some-width-" + i };
      case COMPOSITE -> new Object[] { (long) (i % 1000), (long) i };
    };
  }

  private static long walStat(final Database database, final String name) {
    return (Long) ((DatabaseInternal) database).getTransactionManager().getStats().get(name);
  }

  /** Small shim so the page-size override is applied only where it is meaningful. */
  private record IndexBuilderAccessor(Database database, String[] properties, Schema.INDEX_TYPE indexType,
                                      int pageSize) {
    void create() {
      final var builder = database.getSchema().buildTypeIndex("Entry", properties)
          .withType(indexType).withUnique(!NON_UNIQUE);
      if (pageSize > 0)
        builder.withPageSize(pageSize);
      builder.create();
    }
  }
}
