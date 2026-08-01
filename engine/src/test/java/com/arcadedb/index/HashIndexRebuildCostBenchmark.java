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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Prices the migration strategy for #5712.
 * <p>
 * Changing the bucket layout leaves existing HASH indexes on disk in the old format. One option is to carry both
 * layouts in the read path forever; the other is to detect the old format when the database opens and rebuild
 * those indexes, which retires the old format immediately but makes someone wait. That choice should be made on
 * the actual cost of the rebuild, not on a guess, and the cost is what this measures - per index size, and against
 * the time the same database takes to open without one.
 * <p>
 * What is measured is {@code REBUILD INDEX}, the mechanism an open-time migration would drive. It is an upper
 * bound in one respect: a migration rebuilds from data already in the page cache of a just-opened database,
 * whereas here the database is reopened cold first, which is the honest comparison for a restart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class HashIndexRebuildCostBenchmark {
  private static final String SIZES  = System.getProperty("sizes", "10000,100000,500000");
  private static final int    PER_TX = Integer.parseInt(System.getProperty("perTx", "1000"));

  @Test
  void pricesAnOpenTimeRebuildPerIndexSize() {
    System.out.printf("%-10s %-9s | %9s %9s %11s%n", "entries", "index", "openMs", "rebuildMs", "perEntryUs");

    for (final String size : SIZES.split(",")) {
      final int entries = Integer.parseInt(size.trim());
      measure(entries, Schema.INDEX_TYPE.HASH);
      measure(entries, Schema.INDEX_TYPE.LSM_TREE);
    }
  }

  private void measure(final int entries, final Schema.INDEX_TYPE indexType) {
    final String path = "./target/databases/hashRebuild_" + entries + "_" + indexType;
    final DatabaseFactory factory = new DatabaseFactory(path);
    if (factory.exists())
      factory.open().drop();

    String indexName;
    try (final Database database = factory.create()) {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Entry").createProperty("k", Type.LONG);
        database.getSchema().buildTypeIndex("Entry", new String[] { "k" })
            .withType(indexType).withUnique(true).create();
      });

      for (int i = 0; i < entries; i += PER_TX) {
        final int from = i;
        database.transaction(() -> {
          for (int j = from; j < Math.min(from + PER_TX, entries); j++)
            database.newDocument("Entry").set("k", (long) j).save();
        });
      }
      indexName = database.getSchema().getType("Entry").getAllIndexes(true).iterator().next().getName();
    }

    // Cold open, so the number reflects a restart rather than a warm cache.
    final long openBegin = System.nanoTime();
    final Database reopened = factory.open();
    final long openMs = (System.nanoTime() - openBegin) / 1_000_000;

    try {
      final long rebuildBegin = System.nanoTime();
      reopened.command("sql", "REBUILD INDEX `" + indexName + "`");
      final long rebuildMs = (System.nanoTime() - rebuildBegin) / 1_000_000;

      // A rebuild that lost entries would make the migration unusable however fast it was.
      final long indexed = reopened.getSchema().getType("Entry").getAllIndexes(true).stream()
          .mapToLong(IndexInternal::countEntries).sum();
      assertThat(indexed).isEqualTo(entries);

      System.out.printf("%-10d %-9s | %7d ms %7d ms %9.2f us%n",
          entries, indexType, openMs, rebuildMs, rebuildMs * 1000.0 / entries);
    } finally {
      reopened.drop();
    }
  }
}
