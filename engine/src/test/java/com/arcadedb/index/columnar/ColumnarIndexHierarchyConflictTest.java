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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Final review C-I2: the dictionaries of a bucket projection are keyed by property, so two COLUMNAR columns on the same base property in
 * one bucket would share one dictionary slot and count wrong. A property therefore takes one COLUMNAR index per type hierarchy: a second
 * one is refused wherever it would reach a bucket the first projects (supertypes, subtypes, a diamond), BY ITEM or not, and so is an
 * {@code ALTER TYPE ... SUPERTYPE} that would bring two together.
 */
class ColumnarIndexHierarchyConflictTest extends TestHelper {
  private static final String PHOTO_INDEX = "Photo[brand]_columnar";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset");
    database.command("sql", "CREATE DOCUMENT TYPE Photo EXTENDS Asset");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        database.newDocument("Asset").set("brand", "a" + i % 3).save();
        database.newDocument("Photo").set("brand", "p" + i % 4).save();
      }
    });
  }

  @Test
  void aSupertypeIndexOverASubtypeIndexIsRefused() {
    database.command("sql", "CREATE INDEX ON Photo (brand) COLUMNAR");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR")).isInstanceOf(SchemaException.class)
        .hasMessageContaining(PHOTO_INDEX).hasMessageContaining("brand");
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON Asset (brand) COLUMNAR"))
        .isInstanceOf(SchemaException.class).hasMessageContaining(PHOTO_INDEX);

    assertThat(database.getSchema().existsIndex("Asset[brand]_columnar")).isFalse();
    assertOneColumnPerBucket("Photo");
    FacetOracle.assertEquivalent(database, "Photo", "brand", null);
  }

  @Test
  void theBaseBuilderRouteIsRefusedToo() {
    // batch 3 r1 I1: withType(COLUMNAR) records the type on the base builder, whose create() must run the COLUMNAR checks as well
    database.command("sql", "CREATE INDEX ON Photo (brand) COLUMNAR");

    final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Asset", new String[] { "brand" });
    builder.withType(Schema.INDEX_TYPE.COLUMNAR);
    assertThatThrownBy(builder::create).isInstanceOf(SchemaException.class).hasMessageContaining(PHOTO_INDEX);

    assertThat(database.getSchema().existsIndex("Asset[brand]_columnar")).isFalse();
    assertOneColumnPerBucket("Photo");
  }

  @Test
  void aNewTypeExtendingTwoTypesWithAColumnarIndexOnOnePropertyIsRefusedAndTakenBack() {
    // batch 3 r1 M4: the second link refuses, after the type was registered; the take-back leaves nothing behind
    database.command("sql", "CREATE DOCUMENT TYPE Tagged");
    database.command("sql", "CREATE PROPERTY Tagged.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Tagged (brand) COLUMNAR");
    final Path columnarRoot = Path.of(database.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR);
    final long directoriesBefore = countEntries(columnarRoot);
    final int assetSubIndexes = ((TypeIndex) database.getSchema().getIndexByName("Asset[brand]_columnar")).getSubIndexes().size();

    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE Both EXTENDS Asset, Tagged")).isInstanceOf(
        SchemaException.class).hasMessageContaining("brand");

    assertThat(database.getSchema().existsType("Both")).isFalse();
    assertThat(countEntries(columnarRoot)).as("no projection directory left").isEqualTo(directoriesBefore);
    assertThat(((TypeIndex) database.getSchema().getIndexByName("Asset[brand]_columnar")).getSubIndexes()).hasSize(assetSubIndexes);
    assertThat(((TypeIndex) database.getSchema().getIndexByName("Tagged[brand]_columnar")).getSubIndexes()).hasSize(1);
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);
  }

  private static long countEntries(final Path dir) {
    try (final Stream<Path> entries = Files.list(dir)) {
      return entries.count();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  void aSubtypeIndexUnderASupertypeIndexIsAnsweredByTheSupertypeOne() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");

    // unchanged since before C-I2: the inherited index is the one on these properties
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Photo (brand) COLUMNAR")).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("Asset[brand]_columnar");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON Photo (brand) COLUMNAR");

    assertThat(database.getSchema().existsIndex(PHOTO_INDEX)).isFalse();
    assertOneColumnPerBucket("Photo");
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);
    FacetOracle.assertEquivalent(database, "Photo", "brand", null);
  }

  @Test
  void aScalarAndAByItemIndexOnOnePropertyAreRefused() {
    database.transaction(() -> database.newDocument("Asset").set("free", List.of("x", "y")).save());
    database.getSchema().buildTypeIndex("Asset", new String[] { "free" }).withDefaultKeyTypesForUndeclaredProperties(
        new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.COLUMNAR).create();

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Asset", new String[] { "free by item" })
        .withDefaultKeyTypesForUndeclaredProperties(new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.COLUMNAR).create())
        .isInstanceOf(SchemaException.class).hasMessageContaining("Asset[free]_columnar");
    assertThat(database.getSchema().existsIndex("Asset[freebyitem]_columnar")).isFalse();
  }

  @Test
  void aDiamondReachingTheSameBucketIsRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE Tagged");
    database.command("sql", "CREATE PROPERTY Tagged.brand STRING");
    database.command("sql", "CREATE DOCUMENT TYPE TaggedPhoto EXTENDS Photo, Tagged");
    database.command("sql", "CREATE INDEX ON Tagged (brand) COLUMNAR");

    // Asset's index would reach TaggedPhoto's buckets, which Tagged's index projects already
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Tagged[brand]_columnar");
    assertOneColumnPerBucket("TaggedPhoto");
  }

  @Test
  void anotherPropertyOfTheHierarchyIsAccepted() {
    database.command("sql", "CREATE PROPERTY Asset.author STRING");
    database.command("sql", "CREATE INDEX ON Photo (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (author) COLUMNAR");
    assertThat(database.getSchema().existsIndex("Asset[author]_columnar")).isTrue();
  }

  @Test
  void alterTypeSupertypeBringingTwoTogetherIsRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE Picture");
    database.command("sql", "CREATE PROPERTY Picture.brand STRING");
    database.transaction(() -> database.newDocument("Picture").set("brand", "q").save());
    database.command("sql", "CREATE INDEX ON Picture (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Picture SUPERTYPE +Asset")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Picture[brand]_columnar").hasMessageContaining("Asset[brand]_columnar");
    final DocumentType picture = database.getSchema().getType("Picture");
    assertThat(picture.getSuperTypes()).isEmpty();
    assertOneColumnPerBucket("Picture");
    FacetOracle.assertEquivalent(database, "Picture", "brand", null);

    // a supertype with no COLUMNAR index on the property links as before
    database.command("sql", "CREATE DOCUMENT TYPE Plain");
    database.command("sql", "ALTER TYPE Picture SUPERTYPE +Plain");
    assertThat(picture.getSuperTypes()).hasSize(1);
  }

  /** Every bucket of the type has exactly one COLUMNAR column per property: no two columns share a dictionary slot. */
  private void assertOneColumnPerBucket(final String typeName) {
    final ColumnarIndexManager manager = ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
    for (final var bucket : database.getSchema().getType(typeName).getBuckets(false)) {
      final ColumnarProjection projection = manager.projection(bucket.getFileId());
      assertThat(projection).isNotNull();
      assertThat(projection.columns()).extracting(ColumnDefinition::property).doesNotHaveDuplicates();
    }
    for (final TypeIndex index : database.getSchema().getType(typeName).getAllIndexes(true))
      assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
  }
}
