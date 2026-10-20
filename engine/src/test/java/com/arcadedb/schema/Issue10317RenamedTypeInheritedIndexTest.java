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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10317 (case B): an index a type inherits has one sub-index per bucket of the type, and that sub-index names the type
 * (and so does its file). {@code ALTER TYPE ... NAME} renamed only the indexes the type declares itself, so renaming a sub type
 * left the sub-index of an inherited index naming the old type, and a later {@code DROP TYPE} of the super type could not drop
 * it ("Type with name 'T10' was not found"). The opposite direction was wrong too: renaming a super type re-labelled the
 * sub-indexes that sit on the buckets of its sub types.
 * <p>
 * The invariant pinned here: a sub-index names the type that owns its bucket, and its file is named after that type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10317RenamedTypeInheritedIndexTest extends TestHelper {

  @Test
  void renamingASubTypeCarriesTheSubIndexOfAnInheritedIndex() {
    createSuperAndSubType();
    insert("T10", "x");
    insert("T5", "y");

    database.getSchema().getType("T10").rename("T11");

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    assertThat(count("T5", "x")).isEqualTo(1L);
    assertThat(count("T5", "y")).isEqualTo(1L);

    reopenDatabase();

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    assertThat(count("T5", "x")).isEqualTo(1L);
    insert("T11", "z");
    assertThat(count("T5", "z")).isEqualTo(1L);
    assertThat(count("T11", "z")).isEqualTo(1L);
  }

  @Test
  void renamingASuperTypeLeavesTheSubIndexesOfItsSubTypesAlone() {
    createSuperAndSubType();
    insert("T10", "x");
    insert("T5", "y");

    database.getSchema().getType("T5").rename("T6");

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    assertThat(count("T6", "x")).isEqualTo(1L);
    assertThat(count("T6", "y")).isEqualTo(1L);

    reopenDatabase();

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    assertThat(count("T6", "x")).isEqualTo(1L);
    assertThat(count("T6", "y")).isEqualTo(1L);
  }

  @Test
  void dropTypeOfTheSuperTypeAfterRenamingASubTypeBehavesAsWithoutTheRename() {
    createSuperAndSubType();
    insert("T10", "x");
    database.getSchema().getType("T10").rename("T11");

    database.command("sql", "DROP TYPE T5");

    assertThat(database.getSchema().existsType("T5")).isFalse();
    assertThat(database.getSchema().getType("T11").getSuperTypes()).isEmpty();
    assertThat(database.getSchema().getType("T11").getAllIndexes(true)).isEmpty();
    assertThat(database.countType("T11", true)).isEqualTo(1L);

    reopenDatabase();

    assertThat(database.getSchema().existsType("T5")).isFalse();
    assertThat(database.getSchema().getType("T11").getSuperTypes()).isEmpty();
    assertThat(database.countType("T11", true)).isEqualTo(1L);
  }

  @Test
  void renamingATypeThatDeclaresAnIndexAndInheritsAnotherCarriesBoth() {
    database.command("sql", "CREATE DOCUMENT TYPE T1");
    database.command("sql", "CREATE PROPERTY T1.p STRING");
    database.command("sql", "CREATE INDEX ON T1 (p) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE T2 EXTENDS T1");
    database.command("sql", "CREATE PROPERTY T2.q STRING");
    database.command("sql", "CREATE INDEX ON T2 (q) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE T3 EXTENDS T2");
    database.transaction(() -> {
      database.newDocument("T2").set("p", "p2").set("q", "q2").save();
      database.newDocument("T3").set("p", "p3").set("q", "q3").save();
    });

    database.getSchema().getType("T2").rename("T2b");

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    assertThat(database.query("sql", "SELECT FROM T1 WHERE p = 'p3'").stream().count()).isEqualTo(1L);
    assertThat(database.query("sql", "SELECT FROM T2b WHERE q = 'q3'").stream().count()).isEqualTo(1L);

    reopenDatabase();

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    assertThat(database.query("sql", "SELECT FROM T1 WHERE p = 'p2'").stream().count()).isEqualTo(1L);
    assertThat(database.query("sql", "SELECT FROM T2b WHERE q = 'q2'").stream().count()).isEqualTo(1L);
  }

  /**
   * The inherited sub-index is renamed with the type, so a failure on it has to put the type back like a failure on an index the
   * type declares: the failure is injected by parking a directory on the path its file is about to move to.
   */
  @Test
  void aFailedRenameOfTheInheritedSubIndexPutsTheTypeBack() {
    createSuperAndSubType();
    insert("T10", "x");

    final IndexInternal inherited = subIndexOn("T10");
    final File databaseDirectory = new File(database.getDatabasePath());
    final File blockedFile = fileStartingWith(databaseDirectory, inherited.getName() + ".");
    final File parked = new File(databaseDirectory, blockedFile.getName().replace("T10_0_", "T11_0_"));
    assertThat(parked.mkdir()).as("fault injection: park a directory on the target path").isTrue();

    try {
      assertThatThrownBy(() -> database.getSchema().getType("T10").rename("T11")).isInstanceOf(SchemaException.class);

      assertThat(database.getSchema().existsType("T10")).isTrue();
      assertThat(database.getSchema().existsType("T11")).isFalse();
      assertEverySubIndexNamesTheTypeOwningItsBucket();
    } finally {
      parked.delete();
    }

    assertThat(count("T5", "x")).isEqualTo(1L);

    reopenDatabase();

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    assertThat(count("T5", "x")).isEqualTo(1L);
  }

  /**
   * A database renamed by a version before #10317 holds the sub-index of an inherited index under the OLD type name, in its
   * metadata and in its file name. Nothing has to repair it by hand (issue #10394): the schema load binds every sub-index to the
   * type that owns its bucket (#10093, #10412) and names the type in its metadata, so opening the database with this version is
   * the repair, and the DROP TYPE of the super type that used to fail finds the sub-index. The file keeps the older prefix, which
   * nothing reads: a later rename leaves a file that does not carry the type's name where it is.
   */
  @Test
  void aSchemaRenamedByAnOlderVersionIsRepairedByTheLoad() {
    createSuperAndSubType();
    insert("T10", "x");
    database.getSchema().getType("T10").rename("T11");

    // What the older version left behind: the sub-index naming the type as it was before the rename
    subIndexOn("T11").updateTypeName("T10");
    database.getSchema().getEmbedded().saveConfiguration();
    assertThat(subIndexOn("T11").getTypeName()).isEqualTo("T10");
    // updateTypeName moves the file with the name: the stale layout is on disk too, not only in the metadata
    assertThat(componentNameOf(subIndexOn("T11"))).startsWith("T10_");

    reopenDatabase();

    assertThat(subIndexOn("T11").getTypeName()).isEqualTo("T11");
    assertThat(componentNameOf(subIndexOn("T11"))).as("the file keeps the older prefix, which nothing reads").startsWith("T10_");
    assertThat(count("T5", "x")).isEqualTo(1L);

    database.command("sql", "DROP TYPE T5");

    assertThat(database.getSchema().existsType("T5")).isFalse();
    assertThat(database.getSchema().getType("T11").getAllIndexes(true)).isEmpty();
    assertThat(database.countType("T11", true)).isEqualTo(1L);

    reopenDatabase();

    assertThat(database.getSchema().existsType("T5")).isFalse();
    assertThat(database.countType("T11", true)).isEqualTo(1L);
  }

  private void createSuperAndSubType() {
    database.command("sql", "CREATE DOCUMENT TYPE T5");
    database.command("sql", "CREATE PROPERTY T5.p STRING");
    database.command("sql", "CREATE INDEX ON T5 (p) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE T10 EXTENDS T5");
  }

  private void insert(final String typeName, final String value) {
    database.transaction(() -> database.newDocument(typeName).set("p", value).save());
  }

  private long count(final String typeName, final String value) {
    return database.query("sql", "SELECT FROM " + typeName + " WHERE p = ?", value).stream().count();
  }

  private IndexInternal subIndexOn(final String typeName) {
    final int bucketId = database.getSchema().getType(typeName).getBuckets(false).getFirst().getFileId();
    for (final TypeIndex index : database.getSchema().getType("T5").getAllIndexes(false))
      for (final IndexInternal subIndex : index.getIndexesOnBuckets())
        if (subIndex.getAssociatedBucketId() == bucketId)
          return subIndex;
    throw new AssertionError("no sub-index on a bucket of " + typeName);
  }

  /** A sub-index names the type that owns its bucket, and its file is named after that type. */
  private void assertEverySubIndexNamesTheTypeOwningItsBucket() {
    for (final DocumentType type : database.getSchema().getTypes())
      for (final TypeIndex index : type.getAllIndexes(false))
        for (final IndexInternal subIndex : index.getIndexesOnBuckets()) {
          final String owner = database.getSchema().getTypeByBucketId(subIndex.getAssociatedBucketId()).getName();
          assertThat(subIndex.getTypeName()).as("type named by the sub-index " + subIndex.getName()).isEqualTo(owner);
          assertThat(((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getComponentName())
              .as("file of the sub-index " + subIndex.getName()).startsWith(owner + "_");
        }
  }

  private String componentNameOf(final IndexInternal subIndex) {
    return ((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getComponentName();
  }

  private static File fileStartingWith(final File directory, final String prefix) {
    final File[] files = directory.listFiles((dir, name) -> name.startsWith(prefix));
    assertThat(files).as("a file starting with " + prefix).isNotNull().hasSize(1);
    return files[0];
  }
}
