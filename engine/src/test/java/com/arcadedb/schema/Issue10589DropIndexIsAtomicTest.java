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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Issue #10589: a refused {@code DROP INDEX} left the index half dropped. Dropping a {@link TypeIndex} walks its bucket sub-indexes and
 * deletes one file per sub-index; nothing asked first whether those files could be deleted (issue #10421 added that probe to
 * {@code DROP TYPE}, {@code ALTER TYPE ... BUCKET} and {@code ... SUPERTYPE} only), so the first file that refused aborted the statement
 * with every sub-index before it already dropped, unregistered and saved. The wrapper stayed registered over a SUBSET of the buckets,
 * and every record of a bucket whose sub-index was gone answered 0 rows through the index, across a restart. When the refusing file
 * was the last one the wrapper was left empty, which wedged every later DDL.
 * <p>
 * The failure is injected as {@link Issue10421SchemaStatementsAreAtomicTest} does: the file of a sub-index is moved aside and a
 * non-empty directory takes its path, which {@code Files.delete} refuses. Every statement that reaches {@code dropIndexInternal} is
 * covered, and the ones that drop several indexes are checked with the refusing file on each of them in turn, so the index dropped
 * BEFORE the refusal is the one that proves nothing was changed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10589DropIndexIsAtomicTest extends TestHelper {

  private static final int BUCKETS = 4;
  private static final int RECORDS = 12;

  /** A directory with something in it where a file of the schema is: it cannot be deleted. */
  private record Fault(Path file, Path aside) {
    void remove() throws IOException {
      Files.deleteIfExists(file.resolve("blocker"));
      Files.deleteIfExists(file);
      Files.move(aside, file);
    }
  }

  private Fault injectFault(final IndexInternal subIndex) throws IOException {
    assumeFalse(System.getProperty("os.name", "").toLowerCase().contains("win"), "an open file cannot be moved on Windows");
    final Path file = Path.of(((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getFilePath());
    final Path aside = file.resolveSibling(file.getFileName() + ".aside");
    Files.move(file, aside);
    Files.createDirectory(file);
    Files.writeString(file.resolve("blocker"), "x");
    return new Fault(file, aside);
  }

  @Test
  void aDropIndexThatCannotDeleteAMiddleFileChangesNothing() throws IOException {
    createIndexedType();
    refusedDropLeavesTheIndexWhole("DROP INDEX `T[p]`", 1);
  }

  @Test
  void aDropIndexThatCannotDeleteTheLastFileChangesNothing() throws IOException {
    createIndexedType();
    refusedDropLeavesTheIndexWhole("DROP INDEX `T[p]`", BUCKETS - 1);
  }

  @Test
  void aDropIndexThatCannotDeleteTheFirstFileChangesNothing() throws IOException {
    createIndexedType();
    refusedDropLeavesTheIndexWhole("DROP INDEX `T[p]`", 0);
  }

  @Test
  void aDropOfTheWrapperFromTheJavaApiChangesNothingEither() throws IOException {
    createIndexedType();
    final Fault fault = injectFault(subIndexOf("T[p]", 2));
    try {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("T[p]");
      assertThatThrownBy(index::drop).hasStackTraceContaining("Nothing was changed");

      assertIndexWhole("T[p]");
    } finally {
      fault.remove();
    }
    reopenDatabase();
    assertIndexWhole("T[p]");
  }

  @Test
  void aDropPropertyForceThatCannotDeleteAFileKeepsThePropertyAndItsIndex() throws IOException {
    createIndexedType();
    refusedDropLeavesTheIndexWhole("DROP PROPERTY T.p FORCE", 1);
    assertThat(database.getSchema().getType("T").existsProperty("p")).isTrue();
  }

  @Test
  void aRebuildIndexThatCannotDeleteAFileKeepsTheIndexWhole() throws IOException {
    createIndexedType();
    refusedDropLeavesTheIndexWhole("REBUILD INDEX `T[p]`", 1);
  }

  @Test
  void aTruncateTypeThatCannotDeleteAFileKeepsTheIndexAndTheRecords() throws IOException {
    createIndexedType();
    refusedDropLeavesTheIndexWhole("TRUNCATE TYPE T", 1);
    assertThat(database.countType("T", true)).isEqualTo(RECORDS);
  }

  @Test
  void aTruncateTypeOverSeveralIndexesDropsNoneWhenTheFirstDroppedWouldNotBeTheRefusedOne() throws IOException {
    for (final String refusing : new String[] { "p", "q" }) {
      createTwoIndexedType();
      final Fault fault = injectFault(subIndexOf("T[" + refusing + "]", 1));
      try {
        assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE T")).hasStackTraceContaining("Nothing was changed");

        assertIndexWhole("T[p]");
        assertIndexWhole("T[q]");
        assertThat(database.countType("T", true)).isEqualTo(RECORDS);
      } finally {
        fault.remove();
      }
      reopenDatabase();
      assertIndexWhole("T[p]");
      assertIndexWhole("T[q]");
      database.command("sql", "DROP TYPE T");
    }
  }

  @Test
  void aDropPropertyForceOverSeveralIndexesDropsNoneWhenOneRefuses() throws IOException {
    for (final String refusing : new String[] { "T[p]", "T[p,q]" }) {
      database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY T.p STRING");
      database.command("sql", "CREATE PROPERTY T.q STRING");
      database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON T (p, q) NOTUNIQUE");
      insertRecords();

      final Fault fault = injectFault(subIndexOf(refusing, 1));
      try {
        assertThatThrownBy(() -> database.command("sql", "DROP PROPERTY T.p FORCE")).hasStackTraceContaining("Nothing was changed");

        assertIndexWhole("T[p]");
        assertIndexWhole("T[p,q]");
        assertThat(database.getSchema().getType("T").existsProperty("p")).isTrue();
      } finally {
        fault.remove();
      }
      reopenDatabase();
      assertIndexWhole("T[p]");
      assertIndexWhole("T[p,q]");
      database.command("sql", "DROP TYPE T");
    }
  }

  @Test
  void anEdgeTypeThatCannotDeleteTheFileOfItsUniqueIndexKeepsTheConstraint() throws IOException {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E UNIQUE");
    final TypeIndex unique = (TypeIndex) database.getSchema().getIndexByName(LocalEdgeType.uniqueIndexName("E"));
    final int subIndexes = unique.getIndexesOnBuckets().length;
    final Fault fault = injectFault(unique.getIndexesOnBuckets()[subIndexes - 1]);
    try {
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE E WITH unique = false")).hasStackTraceContaining("Nothing was changed");

      assertThat(((TypeIndex) database.getSchema().getIndexByName(LocalEdgeType.uniqueIndexName("E"))).getIndexesOnBuckets()).hasSize(
          subIndexes);
      assertThat(((LocalEdgeType) database.getSchema().getType("E")).isUnique()).isTrue();
    } finally {
      fault.remove();
    }
  }

  // ---- helpers

  private void refusedDropLeavesTheIndexWhole(final String statement, final int refusingSubIndex) throws IOException {
    final Fault fault = injectFault(subIndexOf("T[p]", refusingSubIndex));
    try {
      assertThatThrownBy(() -> database.command("sql", statement)).hasStackTraceContaining("Nothing was changed");

      assertIndexWhole("T[p]");
      // a later DDL of any kind is not wedged by a wrapper without sub-indexes
      database.command("sql", "CREATE DOCUMENT TYPE Z");
      database.command("sql", "DROP TYPE Z");
    } finally {
      fault.remove();
    }

    reopenDatabase();

    assertIndexWhole("T[p]");

    // and the statement completes once nothing refuses it
    if (statement.startsWith("DROP INDEX")) {
      database.command("sql", statement);
      assertThat(database.getSchema().existsIndex("T[p]")).isFalse();
      assertThat(database.countType("T", true)).isEqualTo(RECORDS);
    }
  }

  /** The index has a sub-index on every bucket of the type, and every record is found through it. */
  private void assertIndexWhole(final String indexName) {
    assertThat(database.getSchema().existsIndex(indexName)).isTrue();
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    assertThat(index.getIndexesOnBuckets()).hasSize(BUCKETS);
    assertThat(index.countEntries()).isEqualTo(RECORDS);
    for (final IndexInternal subIndex : index.getIndexesOnBuckets())
      assertThat(database.getSchema().existsIndex(subIndex.getName())).isTrue();

    // a lookup through the index finds every record, including the ones of the buckets a half dropped index would have lost
    final boolean onQ = indexName.equals("T[q]");
    for (int i = 0; i < RECORDS; i++)
      assertThat(database.query("sql", "SELECT FROM T WHERE " + (onQ ? "q" : "p") + " = ?", (onQ ? "w" : "v") + i).stream().count()).as(
          "record %d through %s", i, indexName).isEqualTo(1L);
  }

  private IndexInternal subIndexOf(final String indexName, final int position) {
    return ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets()[position];
  }

  private void createIndexedType() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS " + BUCKETS);
    database.command("sql", "CREATE PROPERTY T.p STRING");
    database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE");
    insertRecords();
  }

  private void createTwoIndexedType() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS " + BUCKETS);
    database.command("sql", "CREATE PROPERTY T.p STRING");
    database.command("sql", "CREATE PROPERTY T.q STRING");
    database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON T (q) NOTUNIQUE");
    insertRecords();
  }

  private void insertRecords() {
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("T").set("p", "v" + i).set("q", "w" + i).save();
    });
  }
}
