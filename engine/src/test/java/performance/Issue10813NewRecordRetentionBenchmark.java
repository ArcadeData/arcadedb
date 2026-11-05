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
package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Locale;

/**
 * Issue #10813: what a document saved in a transaction costs until the commit, with read-your-writes on (the transaction holds the
 * document, its property map and its serialized buffer) and off (the transaction keeps only what a rollback needs to reset a record
 * the caller still holds). Measures the heap a transaction of new documents retains, and the elapsed time and GC time of the
 * #10661 load: documents {@code {id, grp, cat, val, txt}} in transactions of 10,000 with a UNIQUE index on {@code id}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue10813NewRecordRetentionBenchmark {
  private static final String PATH          = "target/databases/Issue10813NewRecordRetentionBenchmark";
  private static final int    RETAINED_DOCS = 200_000;
  private static final int    LOAD_DOCS     = 2_000_000;
  private static final int    TX_SIZE       = 10_000;

  @Test
  void retainedHeapAndLoadCost() {
    for (final boolean readYourWrites : new boolean[] { true, false })
      load(readYourWrites, LOAD_DOCS / 4); // warm up

    for (final boolean readYourWrites : new boolean[] { true, false })
      retainedHeap(readYourWrites);
    for (int round = 0; round < 2; round++)
      for (final boolean readYourWrites : new boolean[] { true, false })
        load(readYourWrites, LOAD_DOCS);
  }

  private void retainedHeap(final boolean readYourWrites) {
    try (final DatabaseFactory factory = new DatabaseFactory(PATH)) {
      if (factory.exists())
        factory.open().drop();
      final Database database = factory.create();
      try {
        createSchema(database);
        database.setReadYourWrites(readYourWrites);

        database.begin();
        insert(database, 0, 1_000);
        final long before = usedHeapAfterGC();
        insert(database, 1_000, RETAINED_DOCS);
        final long after = usedHeapAfterGC();
        database.commit();

        System.out.printf(Locale.ENGLISH, "readYourWrites=%-5s retained by an open transaction: %,d bytes per new document%n",
            readYourWrites, (after - before) / (RETAINED_DOCS - 1_000));
      } finally {
        database.drop();
      }
    }
  }

  private void load(final boolean readYourWrites, final int documents) {
    try (final DatabaseFactory factory = new DatabaseFactory(PATH)) {
      if (factory.exists())
        factory.open().drop();
      final Database database = factory.create();
      try {
        createSchema(database);
        database.setReadYourWrites(readYourWrites);
        usedHeapAfterGC();

        final long gcBefore = gcMillis();
        final long gcCountBefore = gcCount();
        final long begin = System.nanoTime();
        for (int i = 0; i < documents; i += TX_SIZE) {
          final int from = i;
          database.transaction(() -> insert(database, from, Math.min(documents, from + TX_SIZE)));
        }
        final long elapsed = (System.nanoTime() - begin) / 1_000_000;

        System.out.printf(Locale.ENGLISH, "readYourWrites=%-5s %,d documents: %,d ms, %,d docs/s, GC %,d ms in %,d collections%n",
            readYourWrites, documents, elapsed, documents * 1000L / Math.max(1, elapsed), gcMillis() - gcBefore,
            gcCount() - gcCountBefore);
      } finally {
        database.drop();
      }
    }
  }

  private static void createSchema(final Database database) {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("id", Type.LONG);
    type.createProperty("grp", Type.INTEGER);
    type.createProperty("cat", Type.STRING);
    type.createProperty("val", Type.DOUBLE);
    type.createProperty("txt", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "id");
  }

  private static void insert(final Database database, final int from, final int to) {
    for (int i = from; i < to; i++)
      database.newDocument("Doc").set("id", (long) i, "grp", i % 100, "cat", "c" + (i % 17), "val", i * 0.5, "txt",
          "text of document " + i).save();
  }

  private static long usedHeapAfterGC() {
    for (int i = 0; i < 3; i++) {
      System.gc();
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    final Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  private static long gcMillis() {
    long total = 0;
    for (final GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans())
      total += Math.max(0, gc.getCollectionTime());
    return total;
  }

  private static long gcCount() {
    long total = 0;
    for (final GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans())
      total += Math.max(0, gc.getCollectionCount());
    return total;
  }
}
