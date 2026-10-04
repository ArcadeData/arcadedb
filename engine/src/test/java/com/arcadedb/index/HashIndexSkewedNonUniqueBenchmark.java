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
import com.arcadedb.schema.TypeIndexBuilder;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

/**
 * Low-cardinality NOTUNIQUE_HASH against LSM, to check the page size default of issue #5712 where one key holds thousands
 * of RIDs: a bigger share of the entries then lives in overflow chains, which a smaller page makes longer. Prints insert
 * time and lookup time of hot keys per page size.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class HashIndexSkewedNonUniqueBenchmark {
  private static final int ENTRIES = Integer.parseInt(System.getProperty("entries", "100000"));
  private static final int PER_TX  = 100;

  @Test
  void run() {
    for (final int keys : new int[] { 10, 200, 5_000 })
      for (final int pageSize : new int[] { 0, 4_096, 16_384, 65_536 })
        measure(keys, pageSize);
  }

  private void measure(final int keys, final int pageSize) {
    final String path = "target/databases/HashIndexSkewedNonUniqueBenchmark";
    final DatabaseFactory factory = new DatabaseFactory(path);
    if (factory.exists())
      factory.open().drop();
    final Database database = factory.create();
    try {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Entry").createProperty("k", Type.LONG);
        final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Entry", new String[] { "k" });
        if (pageSize == 0)
          builder.withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
        else
          builder.withType(Schema.INDEX_TYPE.HASH).withUnique(false).withPageSize(pageSize).create();
      });

      final Random random = new Random(42);
      final long insertBegin = System.nanoTime();
      for (int i = 0; i < ENTRIES; i += PER_TX)
        database.transaction(() -> {
          for (int j = 0; j < PER_TX; j++)
            database.newDocument("Entry").set("k", (long) random.nextInt(keys)).save();
        });
      final long insertMs = (System.nanoTime() - insertBegin) / 1_000_000;

      final long readBegin = System.nanoTime();
      long rids = 0;
      for (int i = 0; i < 200; i++)
        rids += database.query("sql", "SELECT count(*) AS c FROM Entry WHERE k = ?", (long) random.nextInt(keys))
            .next().<Long>getProperty("c");
      final long readMs = (System.nanoTime() - readBegin) / 1_000_000;

      System.out.printf("keys=%-6d %-9s %8s | insert %6d ms | 200 key reads %6d ms (%d rids)%n", keys,
          pageSize == 0 ? "LSM_TREE" : "HASH", pageSize == 0 ? "default" : pageSize, insertMs, readMs, rids);
    } finally {
      database.drop();
    }
  }
}
