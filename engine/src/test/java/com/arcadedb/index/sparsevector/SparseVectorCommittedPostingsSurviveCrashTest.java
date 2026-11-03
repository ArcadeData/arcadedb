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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.index.CrashingChildJvm;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Committed sparse-vector writes must survive a process kill. The engine replays a transaction's postings into an
 * in-memory memtable and seals it into a page-backed segment only past ~1M postings or at a clean close, so a kill
 * below that threshold has nothing on disk for the postings: the records come back through WAL recovery, their
 * postings must come back too.
 */
class SparseVectorCommittedPostingsSurviveCrashTest {
  private static final int    DIMENSIONS = 1_000;
  private static final int    RECORDS    = 300;
  private static final String TYPE_NAME  = "CrashSparseDoc";
  private static final String IDX_NAME   = TYPE_NAME + "[tokens,weights]";

  @TempDir
  Path tempDirectory;

  @Test
  void committedPostingsSurviveAProcessKill() throws Exception {
    final Path databasePath = tempDirectory.resolve("sparse-crash");

    final CrashingChildJvm.Result prepared = CrashingChildJvm.run(getClass(), 60, "prepare", databasePath.toString());
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    final CrashingChildJvm.Result crashed = CrashingChildJvm.run(getClass(), 60, "crash", databasePath.toString());
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CrashingChildJvm.CRASH_EXIT_CODE);

    assertEveryRecordIsIndexed(databasePath);
  }

  /**
   * Control: the same writes followed by a clean close, whose close-time flush seals the memtable. Proves the
   * assertions are not vacuous.
   */
  @Test
  void committedPostingsSurviveACleanClose() throws Exception {
    final Path databasePath = tempDirectory.resolve("sparse-clean");

    final CrashingChildJvm.Result prepared = CrashingChildJvm.run(getClass(), 60, "prepare", databasePath.toString());
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    final CrashingChildJvm.Result closed = CrashingChildJvm.run(getClass(), 60, "clean", databasePath.toString());
    assertThat(closed.exitCode()).as(closed.output()).isZero();

    assertEveryRecordIsIndexed(databasePath);
  }

  private static void assertEveryRecordIsIndexed(final Path databasePath) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      // the records are WAL-backed: they must all be there
      assertThat(database.countType(TYPE_NAME, false)).as("records after WAL recovery").isEqualTo(RECORDS);

      // one posting per record on its own dimension plus one on the shared last dimension
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
      final long postings = index.countEntries();

      final AtomicInteger foundBySearch = new AtomicInteger();
      final Iterator<?> it = database.iterateType(TYPE_NAME, false);
      while (it.hasNext()) {
        final Document doc = ((Identifiable) it.next()).asDocument();
        final int dim = doc.getInteger("id");
        try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))", IDX_NAME,
            new int[] { dim }, new float[] { 1.0f }, 3)) {
          while (rs.hasNext()) {
            final RID rid = rs.next().getProperty("@rid");
            if (doc.getIdentity().equals(rid)) {
              foundBySearch.incrementAndGet();
              break;
            }
          }
        }
      }

      assertThat(foundBySearch.get()).as("records a sparse search on their own dimension finds (postings=%d of %d)", postings,
          2L * RECORDS).isEqualTo(RECORDS);
      assertThat(postings).as("postings after the reopen").isEqualTo(2L * RECORDS);
    }
  }

  public static void main(final String[] args) {
    if (args.length != 2)
      throw new IllegalArgumentException("Expected mode and database path");

    if ("prepare".equals(args[0])) {
      try (final DatabaseFactory factory = new DatabaseFactory(args[1]); final Database database = factory.create()) {
        database.transaction(() -> {
          final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
          type.createProperty("id", Type.INTEGER);
          type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
          type.createProperty("weights", Type.ARRAY_OF_FLOATS);
          database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
              .withSparseVectorType()
              .withDimensions(DIMENSIONS)
              .create();
        });
      }
      return;
    }

    if ("crash".equals(args[0]) || "clean".equals(args[0])) {
      final DatabaseFactory factory = new DatabaseFactory(args[1]);
      final Database database = factory.open();
      // three committed transactions, well below the memtable flush threshold
      for (int batch = 0; batch < 3; batch++) {
        final int from = batch * (RECORDS / 3);
        database.transaction(() -> {
          for (int i = from; i < from + RECORDS / 3; i++)
            database.newDocument(TYPE_NAME)
                .set("id", i)
                .set("tokens", new int[] { i, DIMENSIONS - 1 })
                .set("weights", new float[] { 1.0f, 0.5f })
                .save();
        });
      }
      if ("crash".equals(args[0]))
        Runtime.getRuntime().halt(CrashingChildJvm.CRASH_EXIT_CODE);
      database.close();
      factory.close();
      return;
    }

    throw new IllegalArgumentException("Unknown mode " + args[0]);
  }
}
