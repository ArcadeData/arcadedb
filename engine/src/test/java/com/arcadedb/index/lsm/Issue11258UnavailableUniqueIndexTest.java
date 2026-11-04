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
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11258: a unique index whose file has no header page is left out of the schema at load, so the
 * constraint is not enforced and a node cannot tell that from a half-arrived compaction. The schema must report such an
 * index, so an HA node can re-install the database from its cluster; a non-unique one is only a slower scan.
 */
class Issue11258UnavailableUniqueIndexTest extends TestHelper {
  private static final String TYPE = "Issue11258";

  @Test
  void aUniqueIndexBoundToAHeaderlessFileIsReported() throws Exception {
    createTypeWithIndex(true);
    final String compactedName = breakIndexFile();
    reopenDatabase();

    assertThat(schema().getUnavailableUniqueIndexNames()).containsExactly(compactedName);

    // CREATE INDEX on the same properties serves the constraint again
    database.getSchema().getType(TYPE).createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    assertThat(schema().getUnavailableUniqueIndexNames()).isEmpty();
  }

  @Test
  void aNonUniqueIndexBoundToAHeaderlessFileIsNotReported() throws Exception {
    createTypeWithIndex(false);
    breakIndexFile();
    reopenDatabase();

    assertThat(schema().getUnavailableUniqueIndexNames()).isEmpty();
  }

  @Test
  void aHealthyUniqueIndexIsNotReported() {
    createTypeWithIndex(true);

    assertThat(schema().getUnavailableUniqueIndexNames()).isEmpty();
  }

  private LocalSchema schema() {
    return ((DatabaseInternal) database).getSchema().getEmbedded();
  }

  private void createTypeWithIndex(final boolean unique) {
    database.transaction(() -> {
      database.getSchema().createVertexType(TYPE).createProperty("id", Type.INTEGER)
          .createIndex(Schema.INDEX_TYPE.LSM_TREE, unique);
      for (int i = 0; i < 5; i++)
        database.newVertex(TYPE).set("id", i).save();
    });
  }

  /** Closes the database and swaps the index's mutable file for an empty one, as a power loss leaves it. */
  private String breakIndexFile() throws Exception {
    final IndexInternal bucketIndex = database.getSchema().getType(TYPE).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
    final LSMTreeIndexMutable mutable = ((LSMTreeIndex) bucketIndex).getMutableIndex();
    final String mutableName = mutable.getName();
    final File mutableFile = new File(((DatabaseInternal) database).getFileManager().getFile(mutable.getFileId()).getFilePath());
    final String compactedName = LSMTreeIndexAbstract.compactionBaseName(mutableName) + "_" + System.nanoTime();

    database.close();

    final File schemaFile = new File(database.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
    final String schemaJson = Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8);
    Files.writeString(schemaFile.toPath(), schemaJson.replace(mutableName, compactedName), StandardCharsets.UTF_8);
    final File prevSchemaFile = new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME);
    if (prevSchemaFile.exists())
      Files.delete(prevSchemaFile.toPath());

    final File headerless = new File(mutableFile.getParentFile(), mutableFile.getName().replace(mutableName, compactedName));
    Files.delete(mutableFile.toPath());
    assertThat(headerless.createNewFile()).isTrue();
    return compactedName;
  }
}
