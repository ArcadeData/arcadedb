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
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
}
