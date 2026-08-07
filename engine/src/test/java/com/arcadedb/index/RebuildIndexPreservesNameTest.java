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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.parser.RebuildIndexStatement;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5791: {@code REBUILD INDEX <name>} dropped and recreated the underlying {@link TypeIndex}
 * without carrying over its explicitly-given name, so a manually-named index silently reverted to the auto-derived
 * {@code typeName[properties]} form. {@link RebuildIndexStatement#buildIndex} rebuilt the
 * index through {@code TypeIndexBuilder} without calling {@code withIndexName(...)}, so
 * {@code TypeIndexBuilder.create()} never populated {@code metadata.typeIndexName} and
 * {@code LocalDocumentType#addIndexInternal} fell back to the default name. Every name-based lookup against the
 * original name - including {@code SEARCH_INDEX}, which has no other way to reference an index - broke silently.
 * <p>
 * A plain {@code LSM_TREE} index rebuild happened to carry the name through anyway, because its metadata object is
 * reused as-is; {@code FULL_TEXT} and {@code GEOSPATIAL} do not, because {@link RebuildIndexStatement#buildIndex}
 * deliberately reconstructs their metadata from the index's persisted JSON to recover type-specific settings
 * (analyzers, GeoHash precision) that the generic {@code IndexMetadata} does not carry - and {@code typeIndexName} is
 * not part of that JSON, so the reconstruction silently drops it. All three are covered here so the fix is not
 * allowed to regress to "only helps FULL_TEXT".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildIndexPreservesNameTest extends TestHelper {

  @Test
  void rebuildKeepsExplicitNameOnPlainIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "INSERT INTO Doc SET name = 'alpha'");
      database.command("sql", "CREATE INDEX myNamedIdx ON Doc (name) NOTUNIQUE");
    });

    database.transaction(() -> database.command("sql", "REBUILD INDEX `myNamedIdx`"));

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("myNamedIdx")).isTrue();
      assertThat(database.getSchema().existsIndex("Doc[name]")).isFalse();
      assertThat(database.getSchema().getIndexByName("myNamedIdx").getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    });
  }

  @Test
  void rebuildKeepsExplicitNameOnFullTextIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "INSERT INTO Doc SET title = 'java database tutorial'");
      database.command("sql", "CREATE INDEX ftDocTitle ON Doc (title) FULL_TEXT");
    });

    database.transaction(() -> database.command("sql", "REBUILD INDEX `ftDocTitle`"));

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("ftDocTitle")).isTrue();
      assertThat(database.getSchema().existsIndex("Doc[title]")).isFalse();
      assertThat(database.getSchema().getIndexByName("ftDocTitle").getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);

      // The index must remain usable through the name SEARCH_INDEX needs.
      final var result = database.query("sql", "SELECT title FROM Doc WHERE SEARCH_INDEX('ftDocTitle', 'java') = true");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("title")).isEqualTo("java database tutorial");
    });
  }

  @Test
  void rebuildKeepsExplicitNameOnGeospatialIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Place");
      database.command("sql", "CREATE PROPERTY Place.location STRING");
      database.command("sql", "INSERT INTO Place SET location = 'POINT (9.19 45.46)'");
      database.command("sql", "CREATE INDEX geoPlaceLocation ON Place (location) GEOSPATIAL");
    });

    database.transaction(() -> database.command("sql", "REBUILD INDEX `geoPlaceLocation`"));

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("geoPlaceLocation")).isTrue();
      assertThat(database.getSchema().existsIndex("Place[location]")).isFalse();
      assertThat(database.getSchema().getIndexByName("geoPlaceLocation").getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
    });
  }

  /**
   * {@code REBUILD INDEX *} targets bucket sub-indexes, not the {@link TypeIndex} wrapper directly
   * ({@code idx.isAutomatic() && !(idx instanceof TypeIndex)}) - but for a single-bucket type (the common default,
   * used here) that sub-index is also the LAST one under its {@code TypeIndex}, and dropping the last sub-index drops
   * the wrapper too ({@code LocalSchema.dropIndex}). The rebuilt replacement then finds no existing {@code TypeIndex}
   * to reattach to and mints a new one, hitting the exact same auto-derived-name fallback as the direct
   * {@code typeIndexRebuild} path - a second occurrence of #5791 this test caught after a code reviewer's (and this
   * fix's first draft's) assumption that the sweep was unaffected turned out to be wrong.
   */
  @Test
  void rebuildAllDoesNotRenameNamedFullTextOrGeospatialIndexes() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "INSERT INTO Doc SET title = 'java database tutorial'");
      database.command("sql", "CREATE INDEX ftDocTitle ON Doc (title) FULL_TEXT");

      database.command("sql", "CREATE DOCUMENT TYPE Place");
      database.command("sql", "CREATE PROPERTY Place.location STRING");
      database.command("sql", "INSERT INTO Place SET location = 'POINT (9.19 45.46)'");
      database.command("sql", "CREATE INDEX geoPlaceLocation ON Place (location) GEOSPATIAL");
    });

    database.transaction(() -> database.command("sql", "REBUILD INDEX *"));

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("ftDocTitle")).isTrue();
      assertThat(database.getSchema().existsIndex("Doc[title]")).isFalse();

      assertThat(database.getSchema().existsIndex("geoPlaceLocation")).isTrue();
      assertThat(database.getSchema().existsIndex("Place[location]")).isFalse();
    });
  }

  /**
   * Round out the coverage on the other side of the single-bucket trap: on a multi-bucket type the {@link TypeIndex}
   * wrapper always has a surviving sub-index while any one of the others is being rebuilt, so it is never dropped and
   * {@code addIndexInternal} reattaches to it directly - the {@code withIndexName(...)} carried through by this fix
   * is unused on this path. Asserted explicitly so a future change cannot silently break the "wrapper survives, name
   * argument unused" branch without a test noticing.
   */
  @Test
  void rebuildAllKeepsNameOnMultiBucketFullTextIndex() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(3).create();
      type.createProperty("title", String.class);
      database.command("sql", "INSERT INTO Doc SET title = 'java database tutorial'");
      database.command("sql", "INSERT INTO Doc SET title = 'python guide'");
      database.command("sql", "INSERT INTO Doc SET title = 'rust systems programming'");
      database.command("sql", "CREATE INDEX ftDocTitle ON Doc (title) FULL_TEXT");
    });

    database.transaction(() -> database.command("sql", "REBUILD INDEX *"));

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("ftDocTitle")).isTrue();
      assertThat(database.getSchema().existsIndex("Doc[title]")).isFalse();
      assertThat(database.getSchema().getIndexByName("ftDocTitle").getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);

      final var result = database.query("sql", "SELECT title FROM Doc WHERE SEARCH_INDEX('ftDocTitle', 'java') = true");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("title")).isEqualTo("java database tutorial");
    });
  }
}
