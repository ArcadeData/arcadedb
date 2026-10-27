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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10751: a type rename gave every {@link TypeIndex} of the type its new name and left {@code LocalSchema.indexMap} keyed by
 * the old one. The index answered to a name nothing resolved: {@code DROP INDEX} and {@code REBUILD INDEX} with the name the catalog
 * printed failed, the old name dropped the index of a type that no longer existed, a new type under the freed name could not index
 * the same property, a {@code DROP TYPE} of the renamed type left the wrapper behind and broke {@code schema:indexes}, and a manual
 * index name was replaced by the derived one and lost for good at the next reopen.
 * <p>
 * The guarantee the census pins: after a rename, every index the schema lists is reached through the name it answers to, and no
 * name it answered to before resolves any longer. The census walks the index kinds from the schema's own factory, so a kind added
 * there is covered without touching this test.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10751TypeRenameRekeysIndexMapTest extends TestHelper {

  @Test
  void aDefaultNamedIndexAnswersToItsNewNameOnly() {
    database.command("sql", "CREATE DOCUMENT TYPE Foo");
    database.command("sql", "CREATE PROPERTY Foo.a STRING");
    database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");
    database.transaction(() -> database.newDocument("Foo").set("a", "x").save());
    final Index index = database.getSchema().getIndexByName("Foo[a]");

    database.command("sql", "ALTER TYPE Foo NAME Bar");

    assertThat(index.getName()).isEqualTo("Bar[a]");
    assertThat(database.getSchema().existsIndex("Bar[a]")).isTrue();
    assertThat(database.getSchema().getIndexByName("Bar[a]")).isSameAs(index);
    assertThat(database.getSchema().existsIndex("Foo[a]")).isFalse();
    assertEveryIndexResolvesToItself();

    // the old name is not a way to reach the index of a type that no longer exists under it
    assertThatThrownBy(() -> database.command("sql", "DROP INDEX `Foo[a]`")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Index not found");
    assertThat(database.getSchema().existsIndex("Bar[a]")).isTrue();

    database.command("sql", "REBUILD INDEX `Bar[a]`");
    assertThat(database.query("sql", "SELECT FROM Bar WHERE a = 'x'").stream().count()).isEqualTo(1L);

    database.command("sql", "DROP INDEX `Bar[a]`");
    assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
    assertThat(database.getSchema().getType("Bar").getAllIndexes(true)).isEmpty();
  }

  @Test
  void theFreedNameCanBeIndexedAgainAndTheRenamedTypeDropsCleanly() {
    database.command("sql", "CREATE DOCUMENT TYPE Foo");
    database.command("sql", "CREATE PROPERTY Foo.a STRING");
    database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");
    database.command("sql", "ALTER TYPE Foo NAME Bar");

    database.command("sql", "CREATE DOCUMENT TYPE Foo");
    database.command("sql", "CREATE PROPERTY Foo.a STRING");
    database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");
    assertThat(database.getSchema().getIndexByName("Foo[a]").getTypeName()).isEqualTo("Foo");
    assertThat(database.getSchema().getIndexByName("Bar[a]").getTypeName()).isEqualTo("Bar");
    assertEveryIndexResolvesToItself();

    database.command("sql", "DROP TYPE Bar");

    assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
    // the catalog lists the type index and its bucket sub-index, both of the new Foo, and answers at all
    assertThat(indexNamesOfTheCatalog()).contains("Foo[a]").allMatch(n -> n.startsWith("Foo"));
    assertEveryIndexResolvesToItself();

    reopenDatabase();
    assertThat(indexNamesOfTheCatalog()).contains("Foo[a]").allMatch(n -> n.startsWith("Foo"));
  }

  @Test
  void aManualIndexNameSurvivesTheRenameAndAReopen() {
    database.command("sql", "CREATE DOCUMENT TYPE Foo");
    database.command("sql", "CREATE PROPERTY Foo.a STRING");
    database.command("sql", "CREATE INDEX MyIdx ON Foo (a) UNIQUE");
    database.transaction(() -> database.newDocument("Foo").set("a", "x").save());

    database.command("sql", "ALTER TYPE Foo NAME Bar");

    assertThat(database.getSchema().existsIndex("MyIdx")).isTrue();
    assertThat(database.getSchema().getIndexByName("MyIdx").getName()).isEqualTo("MyIdx");
    assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
    assertEveryIndexResolvesToItself();

    reopenDatabase();

    assertThat(database.getSchema().existsIndex("MyIdx")).isTrue();
    assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
    assertThat(database.getSchema().getIndexByName("MyIdx").getTypeName()).isEqualTo("Bar");
    assertThat(database.query("sql", "SELECT FROM Bar WHERE a = 'x'").stream().count()).isEqualTo(1L);
    assertEveryIndexResolvesToItself();

    // and a second rename keeps it too
    database.command("sql", "ALTER TYPE Bar NAME Baz");
    assertThat(database.getSchema().getIndexByName("MyIdx").getTypeName()).isEqualTo("Baz");
    reopenDatabase();
    assertThat(database.getSchema().existsIndex("MyIdx")).isTrue();
    assertThat(database.getSchema().existsIndex("Baz[a]")).isFalse();
  }

  @Test
  void aRenameBackRestoresTheOriginalNames() {
    database.command("sql", "CREATE DOCUMENT TYPE Foo");
    database.command("sql", "CREATE PROPERTY Foo.a STRING");
    database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");

    database.command("sql", "ALTER TYPE Foo NAME Bar");
    database.command("sql", "ALTER TYPE Bar NAME Foo");

    assertThat(database.getSchema().existsIndex("Foo[a]")).isTrue();
    assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
    assertEveryIndexResolvesToItself();
  }

  /** A rename refused after the type index took its new name puts the map back under the old one (the rollback of the rename). */
  @Test
  void aRefusedRenameLeavesTheIndexUnderItsOldName() throws IOException {
    database.command("sql", "CREATE DOCUMENT TYPE Foo BUCKETS 2");
    database.command("sql", "CREATE PROPERTY Foo.a STRING");
    database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Foo[a]");
    // the file of the LAST sub-index cannot take its new name: the type index and the first sub-index are renamed before the refusal
    final IndexInternal last = index.getIndexesOnBuckets()[1];
    final Path file = Path.of(((DatabaseInternal) database).getFileManager().getFile(last.getFileId()).getFilePath());
    final Path blocked = file.resolveSibling(file.getFileName().toString().replace("Foo_", "Bar_"));
    Files.createDirectory(blocked);
    try {
      assertThatThrownBy(() -> database.getSchema().getType("Foo").rename("Bar")).isInstanceOf(SchemaException.class);

      assertThat(database.getSchema().existsType("Foo")).isTrue();
      assertThat(index.getName()).isEqualTo("Foo[a]");
      assertThat(database.getSchema().getIndexByName("Foo[a]")).isSameAs(index);
      assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
      assertEveryIndexResolvesToItself();
    } finally {
      Files.deleteIfExists(blocked);
    }

    reopenDatabase();
    assertThat(database.getSchema().existsIndex("Foo[a]")).isTrue();
    assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
    assertEveryIndexResolvesToItself();
  }

  /**
   * Every index kind the schema can build, each on a type of its own, the types renamed: every index answers to a name that reaches
   * it, and none of the names the type indexes answered to before the rename does. Then again after a reopen.
   */
  @Test
  void censusOfTheRegisteredIndexKinds() {
    final Set<String> kinds = ((LocalSchema) database.getSchema().getEmbedded()).indexFactory.getRegisteredTypes();
    assertThat(kinds).as("the census reads the kinds from the factory").isNotEmpty();

    final List<String> oldNames = new ArrayList<>();
    for (final String kind : kinds) {
      final String typeName = "T_" + kind;
      createIndexOfKind(typeName, Schema.INDEX_TYPE.valueOf(kind));
      for (final TypeIndex index : database.getSchema().getType(typeName).getAllIndexes(false))
        oldNames.add(index.getName());
    }
    assertThat(oldNames).as("one type index per registered kind").hasSize(kinds.size());

    for (final String kind : kinds)
      database.command("sql", "ALTER TYPE `T_" + kind + "` NAME `R_" + kind + "`");

    for (final String kind : kinds) {
      final List<TypeIndex> indexes = new ArrayList<>(database.getSchema().getType("R_" + kind).getAllIndexes(false));
      assertThat(indexes).as("kind %s keeps its index", kind).hasSize(1);
      assertThat(indexes.getFirst().getName()).as("kind %s", kind).startsWith("R_" + kind + "[");
    }
    for (final String oldName : oldNames)
      assertThat(database.getSchema().existsIndex(oldName)).as("the retired name %s", oldName).isFalse();
    assertEveryIndexResolvesToItself();

    reopenDatabase();

    for (final String oldName : oldNames)
      assertThat(database.getSchema().existsIndex(oldName)).as("the retired name %s after a reopen", oldName).isFalse();
    for (final String kind : kinds)
      assertThat(database.getSchema().getType("R_" + kind).getAllIndexes(false)).as("kind %s after a reopen", kind).hasSize(1);
    assertEveryIndexResolvesToItself();
  }

  // ---- helpers

  private void createIndexOfKind(final String typeName, final Schema.INDEX_TYPE kind) {
    final String t = "`" + typeName + "`";
    database.command("sql", "CREATE DOCUMENT TYPE " + t);
    switch (kind) {
    case LSM_TREE -> {
      database.command("sql", "CREATE PROPERTY " + t + ".s STRING");
      database.command("sql", "CREATE INDEX ON " + t + " (s) UNIQUE");
    }
    case HASH -> {
      database.command("sql", "CREATE PROPERTY " + t + ".s STRING");
      database.command("sql", "CREATE INDEX ON " + t + " (s) UNIQUE_HASH");
    }
    case FULL_TEXT -> {
      database.command("sql", "CREATE PROPERTY " + t + ".s STRING");
      database.command("sql", "CREATE INDEX ON " + t + " (s) FULL_TEXT");
    }
    case GEOSPATIAL -> {
      database.command("sql", "CREATE PROPERTY " + t + ".s STRING");
      database.command("sql", "CREATE INDEX ON " + t + " (s) GEOSPATIAL");
    }
    case LSM_VECTOR -> {
      database.command("sql", "CREATE PROPERTY " + t + ".v ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + t + " (v) LSM_VECTOR METADATA { dimensions: 4, similarity: 'COSINE' }");
    }
    case LSM_SPARSE_VECTOR -> {
      database.command("sql", "CREATE PROPERTY " + t + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + t + ".weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + t + " (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
    }
    default -> throw new IllegalStateException("index kind " + kind + " registered in the factory and not covered by this census");
    }
  }

  /** Every index the schema lists - type indexes and their bucket sub-indexes - is the one its own name resolves to. */
  private void assertEveryIndexResolvesToItself() {
    final Map<IndexInternal, Boolean> seen = new IdentityHashMap<>();
    for (final Index index : database.getSchema().getIndexes()) {
      assertThat(database.getSchema().existsIndex(index.getName())).as("index %s", index.getName()).isTrue();
      assertThat(database.getSchema().getIndexByName(index.getName())).as("index %s", index.getName()).isSameAs(index);
      seen.put((IndexInternal) index, true);
    }
    for (final DocumentType type : database.getSchema().getTypes())
      for (final TypeIndex index : type.getAllIndexes(false)) {
        assertThat(seen).as("type index %s of %s is listed", index.getName(), type.getName()).containsKey(index);
        for (final IndexInternal subIndex : index.getIndexesOnBuckets())
          assertThat(seen).as("sub-index %s is listed", subIndex.getName()).containsKey(subIndex);
      }
  }

  private List<String> indexNamesOfTheCatalog() {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        names.add(row.getProperty("name"));
      }
    }
    return names;
  }
}
