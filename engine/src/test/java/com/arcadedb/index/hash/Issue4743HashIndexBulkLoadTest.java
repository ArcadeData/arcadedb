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
package com.arcadedb.index.hash;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bulk-load scenario of issue #4743: a large number of vertices keyed by a random UUID indexed with a HASH index,
 * plus the reporter's UPSERT variant on a type with subtypes. Sizes can be raised from the command line, e.g.
 * {@code -Dissue4743.total=1000000}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
public class Issue4743HashIndexBulkLoadTest {
  private static final String DB_PATH = "./target/databases/issue4743-hash-bulk";
  private static final int    TOTAL   = Integer.parseInt(System.getProperty("issue4743.total", "200000"));
  private static final int    BATCH   = Integer.parseInt(System.getProperty("issue4743.batch", "10000"));
  private static final long   SEED    = 4743L;

  @Test
  void uniqueHashIndexOnUuidBulkLoad() {
    final String path = DB_PATH + "-load";
    FileUtils.deleteRecursively(new File(path));
    try (final DatabaseFactory factory = new DatabaseFactory(path)) {
      final Database database = factory.create();
      try {
        database.command("sql", "CREATE VERTEX TYPE Address");
        database.command("sql", "CREATE PROPERTY Address.uid STRING");
        database.command("sql", "CREATE PROPERTY Address.name STRING");
        database.command("sql", "CREATE INDEX ON Address (uid) UNIQUE_HASH");

        final Random random = new Random(SEED);
        for (int i = 0; i < TOTAL; i += BATCH) {
          final int from = i;
          final int to = Math.min(i + BATCH, TOTAL);
          database.transaction(() -> {
            for (int k = from; k < to; k++) {
              final MutableVertex v = database.newVertex("Address");
              v.set("uid", uuidAt(random));
              v.set("name", "name-" + k);
              v.save();
            }
          });
        }

        assertThat(database.countType("Address", true)).isEqualTo(TOTAL);
        assertThat(structuralProblems(database, "Address[uid]")).isEmpty();

        // every generated key must be retrievable: replay the same random sequence
        final Random verify = new Random(SEED);
        database.transaction(() -> {
          final var index = database.getSchema().getIndexByName("Address[uid]");
          for (int k = 0; k < TOTAL; k++) {
            final String key = uuidAt(verify);
            if (k % 97 == 0)
              assertThat(index.get(new Object[] { key }).hasNext())
                  .withFailMessage("key " + key + " (#" + k + ") not found").isTrue();
          }
        });
      } finally {
        if (database.isOpen())
          database.drop();
      }
    } finally {
      FileUtils.deleteRecursively(new File(path));
    }
  }

  /**
   * The reporter's SQL path: a type with subtypes, a UNIQUE_HASH index on the lookup property and a batch of
   * UPSERT statements, run twice so the second pass exercises remove + put on the hash index.
   */
  @Test
  void upsertOnTypeWithSubTypes() {
    final String path = DB_PATH + "-upsert";
    final int total = Integer.parseInt(System.getProperty("issue4743.upsert.total", "20000"));
    FileUtils.deleteRecursively(new File(path));
    try (final DatabaseFactory factory = new DatabaseFactory(path)) {
      final Database database = factory.create();
      try {
        database.command("sql", "CREATE VERTEX TYPE Address");
        database.command("sql", "CREATE PROPERTY Address.uid STRING");
        database.command("sql", "CREATE PROPERTY Address.name STRING");
        database.command("sql", "CREATE INDEX ON Address (uid) UNIQUE_HASH");
        database.command("sql", "CREATE VERTEX TYPE PostalAddress EXTENDS Address");
        database.command("sql", "CREATE VERTEX TYPE EmailAddress EXTENDS Address");

        for (int pass = 0; pass < 2; pass++) {
          final String prefix = pass == 0 ? "n" : "m";
          for (int i = 0; i < total; i += 1_000) {
            final int from = i;
            final int to = Math.min(i + 1_000, total);
            database.transaction(() -> {
              for (int k = from; k < to; k++)
                database.command("sql", "UPDATE Address CONTENT {\"uid\":\"uid-" + k + "\",\"name\":\"" + prefix + k
                    + "\"} UPSERT WHERE uid = 'uid-" + k + "'");
            });
          }
          assertThat(database.countType("Address", true)).isEqualTo(total);
          assertThat(structuralProblems(database, "Address[uid]")).isEmpty();
        }
      } finally {
        if (database.isOpen())
          database.drop();
      }
    } finally {
      FileUtils.deleteRecursively(new File(path));
    }
  }

  private static String uuidAt(final Random random) {
    return new UUID(random.nextLong(), random.nextLong()).toString();
  }

  private List<String> structuralProblems(final Database database, final String indexName) {
    final List<String> problems = new ArrayList<>();
    database.transaction(() -> {
      for (final IndexInternal sub : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getSubIndexes())
        if (sub instanceof HashIndex hashIndex)
          for (final String problem : hashIndex.bucket.checkStructuralIntegrity())
            problems.add(hashIndex.getName() + ": " + problem);
    });
    return problems;
  }
}
