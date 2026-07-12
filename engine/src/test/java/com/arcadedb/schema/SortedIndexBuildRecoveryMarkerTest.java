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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SortedIndexBuildRecoveryMarkerTest extends TestHelper {
  @Test
  void preservesPublishedIndexWhenManifestClearWasInterrupted() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("PublishedBuild");
    type.createProperty("lookupKey", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        database.newDocument("PublishedBuild").set("lookupKey", i).save();
    });

    SortedIndexBuildRecoveryMarker.create((DatabaseInternal) database, "PublishedBuild", List.of("lookupKey"), null);
    assertThat(recoveryMarkers()).hasSize(1);

    TypeIndex index = database.getSchema().buildTypeIndex("PublishedBuild", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(4L << 20)
        .withUnique(false)
        .create();
    assertThat(count(index.iterator(true))).isEqualTo(500);

    reopenDatabase();
    index = database.getSchema().getType("PublishedBuild").getIndexByProperties("lookupKey");
    assertThat(index).isNotNull();
    assertThat(count(index.iterator(false))).isEqualTo(500);
    assertThat(recoveryMarkers()).isEmpty();
  }

  @Test
  void usesPreviousSchemaToPreservePublishedIndexWhenPrimarySchemaIsCorrupt() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("PreviousSchemaBuild");
    type.createProperty("lookupKey", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        database.newDocument("PreviousSchemaBuild").set("lookupKey", i).save();
    });

    SortedIndexBuildRecoveryMarker.create((DatabaseInternal) database, "PreviousSchemaBuild", List.of("lookupKey"), null);
    TypeIndex index = database.getSchema().buildTypeIndex("PreviousSchemaBuild", new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(4L << 20)
        .withUnique(false)
        .create();
    assertThat(count(index.iterator(true))).isEqualTo(500);

    database.getSchema().getEmbedded().saveConfiguration();
    final Path databasePath = Path.of(database.getDatabasePath());
    assertThat(databasePath.resolve(LocalSchema.SCHEMA_PREV_FILE_NAME)).isRegularFile();

    database.close();
    database = null;
    Files.writeString(databasePath.resolve(LocalSchema.SCHEMA_FILE_NAME), "{broken",
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
    database = factory.open();

    index = database.getSchema().getType("PreviousSchemaBuild").getIndexByProperties("lookupKey");
    assertThat(index).isNotNull();
    assertThat(count(index.iterator(false))).isEqualTo(500);
    assertThat(recoveryMarkers()).isEmpty();
  }

  private List<Path> recoveryMarkers() throws Exception {
    try (Stream<Path> files = Files.list(Path.of(database.getDatabasePath()))) {
      return files.filter(path -> path.getFileName().toString().startsWith(SortedIndexBuildRecoveryMarker.FILE_PREFIX))
          .toList();
    }
  }

  private static int count(final IndexCursor cursor) {
    int count = 0;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      return count;
    } finally {
      cursor.close();
    }
  }
}
