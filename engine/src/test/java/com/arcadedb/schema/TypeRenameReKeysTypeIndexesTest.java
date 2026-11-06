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
import com.arcadedb.index.Index;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A type rename re-keys the type's indexes in the schema registry at once: the new names resolve and can be dropped without a
 * reload, the old names are gone, and a COLUMNAR index keeps its suffix: the names the next schema load gives them.
 */
class TypeRenameReKeysTypeIndexesTest extends TestHelper {

  @Test
  void renamedTypeIndexesResolveAndDropByTheirNewNames() {
    createIndexedAsset();

    database.command("sql", "ALTER TYPE Asset NAME Item");

    final Schema schema = database.getSchema();
    assertThat(schema.existsIndex("Asset[brand]")).isFalse();
    assertThat(schema.existsIndex("Asset[brand]_columnar")).isFalse();
    final Index key = schema.getIndexByName("Item[brand]");
    assertThat(key.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    assertThat(key.getName()).isEqualTo("Item[brand]");
    final Index columnar = schema.getIndexByName("Item[brand]_columnar");
    assertThat(columnar.getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
    assertThat(columnar.getName()).isEqualTo("Item[brand]_columnar");

    database.command("sql", "DROP INDEX `Item[brand]`");
    database.command("sql", "DROP INDEX `Item[brand]_columnar`");
    assertThat(schema.existsIndex("Item[brand]")).isFalse();
    assertThat(schema.existsIndex("Item[brand]_columnar")).isFalse();
    assertThat(schema.getType("Item").getAllIndexes(false)).isEmpty();
  }

  @Test
  void namesAfterARenameAreTheNamesAfterAReopen() {
    createIndexedAsset();
    database.command("sql", "ALTER TYPE Asset NAME Item");
    final String before = indexNames();

    reopenDatabase();

    assertThat(indexNames()).isEqualTo(before).isEqualTo("Item[brand], Item[brand]_columnar");
  }

  private void createIndexedAsset() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Asset").set("brand", "b" + (i % 5)).save();
    });
  }

  /** The type-level index names the registry resolves for the type, sorted. */
  private String indexNames() {
    return database.getSchema().getType("Item").getAllIndexes(false).stream().map(Index::getName)
        .filter(name -> database.getSchema().existsIndex(name)).sorted().reduce((a, b) -> a + ", " + b).orElse("");
  }
}
