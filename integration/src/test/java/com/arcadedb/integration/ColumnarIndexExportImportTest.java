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
package com.arcadedb.integration;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnarIndex;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.schema.ColumnarIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A JSONL export and import keeps a COLUMNAR index with its settings (hierarchy, maxCardinality), next to the key index on the same
 * property: the importer used to rebuild every index without metadata.
 */
class ColumnarIndexExportImportTest {
  private static final String SOURCE_PATH  = "target/databases/columnar-export-source";
  private static final String RESTORE_PATH = "target/databases/columnar-export-restore";
  private static final String FILE         = "target/columnar-export.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(RESTORE_PATH));
    FileUtils.deleteRecursively(new File(FILE));
  }

  @Test
  void exportAndImportKeepsTheColumnarIndexAndItsSettings() throws Exception {
    try (final Database db = new DatabaseFactory(SOURCE_PATH).create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Asset");
      db.command("sql", "CREATE PROPERTY Asset.brand STRING");
      db.command("sql", "CREATE INDEX ON Asset (brand) NOTUNIQUE");
      db.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA "
          + "{\"hierarchy\":{\"vertexType\":\"Tag\",\"key\":\"path\",\"parentEdge\":\"ParentOf\"}, \"maxCardinality\": 42}");
      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("Asset").set("brand", "b" + (i % 3)).save();
      });
    }

    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    try (final Database db = new DatabaseFactory(RESTORE_PATH).create()) {
      db.command("sql", "IMPORT DATABASE file://" + new File(FILE).getAbsolutePath());
    }

    try (final Database db = new DatabaseFactory(RESTORE_PATH).open()) {
      assertThat(db.countType("Asset", true)).isEqualTo(10);
      assertThat(db.getSchema().getIndexByName("Asset[brand]").getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);

      final TypeIndex columnar = (TypeIndex) db.getSchema().getIndexByName("Asset[brand]_columnar");
      assertThat(columnar.getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
      final ColumnarIndexMetadata metadata = ((ColumnarIndex) columnar.getIndexesOnBuckets()[0]).getColumnarMetadata();
      assertThat(metadata.getMaxCardinality()).isEqualTo(42);
      assertThat(metadata.getHierarchy())
          .isEqualTo(new JSONObject().put("vertexType", "Tag").put("key", "path").put("parentEdge", "ParentOf"));
    }
  }
}
