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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * DROP TYPE takes the paired external-property buckets of the type with its primary ones: the files, every record of an EXTERNAL value,
 * and the schema entry. They are no bucket of the type (they belong to a primary bucket), so a loop over the type's buckets leaves them
 * behind, and a type created later under the same name adopts them with the records of the type that was dropped.
 */
class DropTypeExternalBucketsTest extends TestHelper {

  private void createDoc(final String name, final String parent) {
    database.command("sql", "CREATE DOCUMENT TYPE " + name + (parent != null ? " EXTENDS " + parent : "") + " BUCKETS 2").close();
    if (parent == null) {
      database.command("sql", "CREATE PROPERTY " + name + ".body STRING").close();
      database.command("sql", "ALTER PROPERTY " + name + ".body EXTERNAL true").close();
    }
  }

  private void insert(final String type, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument(type).set("id", i).set("body", "payload-" + type + "-" + i + "-" + "x".repeat(2000)).save();
    });
  }

  private List<String> externalBucketNames() {
    final List<String> names = new ArrayList<>();
    for (final Bucket bucket : database.getSchema().getBuckets())
      if (bucket.getName().endsWith("_ext"))
        names.add(bucket.getName());
    names.sort(null);
    return names;
  }

  private List<String> externalBucketFiles() {
    final List<String> names = new ArrayList<>();
    final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.contains("_ext"));
    if (files != null)
      for (final File file : files)
        names.add(file.getName());
    return names;
  }

  @Test
  void dropTypeDropsItsOwnExternalBuckets() {
    createDoc("Doc", null);
    insert("Doc", 10);
    assertThat(externalBucketNames()).containsExactly("Doc_0_ext", "Doc_1_ext");
    assertThat(externalBucketFiles()).hasSize(2);

    database.command("sql", "DROP TYPE Doc").close();

    assertThat(externalBucketNames()).isEmpty();
    assertThat(externalBucketFiles()).isEmpty();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM schema:buckets")) {
      assertThat(rs.hasNext()).isFalse();
    }

    reopenDatabase();
    assertThat(externalBucketNames()).isEmpty();
    assertThat(externalBucketFiles()).isEmpty();
  }

  @Test
  void dropTypeOfASubTypeThatInheritsTheExternalPropertyDropsItsOwnExternalBucketsOnly() {
    createDoc("Parent", null);
    createDoc("Child", "Parent");
    insert("Parent", 5);
    insert("Child", 5);
    assertThat(externalBucketNames()).containsExactly("Child_0_ext", "Child_1_ext", "Parent_0_ext", "Parent_1_ext");

    database.command("sql", "DROP TYPE Child").close();

    assertThat(externalBucketNames()).containsExactly("Parent_0_ext", "Parent_1_ext");
    // the parent keeps writing and reading its EXTERNAL values, across a reopen too
    insert("Parent", 5);
    reopenDatabase();
    assertThat(externalBucketNames()).containsExactly("Parent_0_ext", "Parent_1_ext");
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM Parent WHERE body.length() > 2000")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(10L);
    }
  }

  @Test
  void aTypeCreatedUnderTheNameOfADroppedOneStartsWithEmptyExternalBuckets() {
    createDoc("Doc", null);
    insert("Doc", 10);
    database.command("sql", "DROP TYPE Doc").close();

    createDoc("Doc", null);
    insert("Doc", 3);
    assertThat(database.countBucket("Doc_0_ext") + database.countBucket("Doc_1_ext")).as("records of the new type only").isEqualTo(3L);

    reopenDatabase();
    assertThat(database.countBucket("Doc_0_ext") + database.countBucket("Doc_1_ext")).isEqualTo(3L);
  }

  @Test
  void dropTypeOfAVertexTypeDropsItsExternalBucketsAndItsEdgeBuckets() {
    database.command("sql", "CREATE VERTEX TYPE V BUCKETS 2").close();
    database.command("sql", "CREATE PROPERTY V.body STRING").close();
    database.command("sql", "ALTER PROPERTY V.body EXTERNAL true").close();
    database.command("sql", "CREATE EDGE TYPE E").close();
    database.transaction(() -> {
      final MutableVertex previous = database.newVertex("V").set("body", "x".repeat(2000)).save();
      for (int i = 0; i < 6; i++) {
        final MutableVertex vertex = database.newVertex("V").set("body", "payload-" + i + "-" + "x".repeat(2000)).save();
        previous.newEdge("E", vertex);
      }
    });
    assertThat(externalBucketNames()).containsExactly("V_0_ext", "V_1_ext");

    database.command("sql", "DROP TYPE V UNSAFE").close();
    // the edges of the type go too: a dangling one is what CHECK DATABASE reports at the end of the test
    database.command("sql", "DROP TYPE E UNSAFE").close();

    assertThat(externalBucketNames()).isEmpty();
    assertThat(externalBucketFiles()).isEmpty();
    assertThat(database.getSchema().getBuckets()).extracting(Bucket::getName).noneMatch(name -> name.startsWith("V_"));

    reopenDatabase();
    assertThat(externalBucketNames()).isEmpty();
    assertThat(externalBucketFiles()).isEmpty();
  }

  /**
   * A bucket detached from its type stays in the schema with the records of its EXTERNAL values, which point into its pair: the pair
   * lives as long as the primary bucket does, so the drop of the type takes the pairs of the buckets the type OWNS, not the pair of a
   * bucket it let go earlier.
   */
  @Test
  void aBucketDetachedBeforeTheDropKeepsItsExternalBucketAndItsValues() {
    createDoc("Doc", null);
    insert("Doc", 10);
    database.command("sql", "ALTER TYPE Doc BUCKET -Doc_1").close();
    final long detachedRecords = database.countBucket("Doc_1");
    assertThat(detachedRecords).isGreaterThan(0L);
    assertThat(externalBucketNames()).containsExactly("Doc_0_ext", "Doc_1_ext");

    database.command("sql", "DROP TYPE Doc").close();

    assertThat(database.getSchema().existsBucket("Doc_0")).isFalse();
    assertThat(database.getSchema().existsBucket("Doc_1")).isTrue();
    assertThat(externalBucketNames()).containsExactly("Doc_1_ext");
    assertThat(externalBucketFiles()).hasSize(1);

    reopenDatabase();
    assertThat(externalBucketNames()).containsExactly("Doc_1_ext");

    // the bucket is attached to a type that declares the property: the values of its records are still there
    database.command("sql", "CREATE DOCUMENT TYPE Other BUCKETS 1").close();
    database.command("sql", "CREATE PROPERTY Other.body STRING").close();
    database.command("sql", "ALTER PROPERTY Other.body EXTERNAL true").close();
    database.command("sql", "ALTER TYPE Other BUCKET +Doc_1").close();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM Other WHERE body.length() > 2000")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(detachedRecords);
    }
  }

  /** The probe of the files a drop is going to delete (#10421) names the paired buckets: one that cannot be deleted refuses the drop. */
  @Test
  void aDropTypeThatCannotDeleteAnExternalBucketFileIsRefusedBeforeItChangesAnything() throws IOException {
    assumeFalse(System.getProperty("os.name", "").toLowerCase().contains("win"), "an open file cannot be moved on Windows");
    createDoc("Doc", null);
    insert("Doc", 10);

    // a directory with something in it where the file of the pair is: it cannot be deleted
    final Path file = Path.of(((DatabaseInternal) database).getFileManager()
        .getFile(database.getSchema().getBucketByName("Doc_1_ext").getFileId()).getFilePath());
    final Path aside = file.resolveSibling(file.getFileName() + ".aside");
    Files.move(file, aside);
    Files.createDirectory(file);
    Files.writeString(file.resolve("blocker"), "x");
    try {
      assertThatThrownBy(() -> database.command("sql", "DROP TYPE Doc")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Doc_1_ext").hasMessageContaining("Nothing was changed");

      assertThat(database.getSchema().existsType("Doc")).isTrue();
      assertThat(database.getSchema().getType("Doc").getBuckets(false)).hasSize(2);
      assertThat(externalBucketNames()).containsExactly("Doc_0_ext", "Doc_1_ext");
    } finally {
      Files.deleteIfExists(file.resolve("blocker"));
      Files.deleteIfExists(file);
      Files.move(aside, file);
    }

    // and the statement completes once nothing refuses it
    database.command("sql", "DROP TYPE Doc").close();
    assertThat(externalBucketNames()).isEmpty();
    assertThat(externalBucketFiles()).isEmpty();
  }
}
