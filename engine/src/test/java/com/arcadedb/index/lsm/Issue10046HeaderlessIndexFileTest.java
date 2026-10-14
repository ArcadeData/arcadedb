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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.FileManager;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #10046: after an HA leader change, every Cypher lookup and range scan on one index of the new
 * leader failed with {@code ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0} in
 * {@code LSMTreeIndexAbstract.convertKeysToDeclaredTypes}. The index was served by a mutable component loaded from a file
 * with no header page - a compaction's new mutable whose pages had not arrived, which an HA follower holds between the
 * entry that creates a compaction's files and the entry that writes them. Reading the missing page 0 invented a zeroed
 * one, so the component came back with no key types and no sub-index, and nothing complained until a query used it.
 * Such a file is now left out of the schema: the index it stood for is reported missing and the query is answered by a
 * scan, correctly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10046HeaderlessIndexFileTest extends TestHelper {
  private static final String TYPE = "Issue10046";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(TYPE).createProperty("id", Type.INTEGER)
          .createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
      for (int i = 0; i < 20; i++)
        database.newVertex(TYPE).set("id", i).save();
    });
  }

  /** A header-less index file the file manager holds is not turned into a component by a schema load. */
  @Test
  void aHeaderlessIndexFileIsNotLoaded() throws Exception {
    final LocalSchema schema = ((DatabaseInternal) database).getSchema().getEmbedded();
    final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
    final int fileId = fileManager.newFileId();
    fileManager.getOrCreateFile(fileId, database.getDatabasePath() + File.separator + "Issue10046_0_1_" + System.nanoTime() + "."
        + fileId + ".262144.v" + LSMTreeIndexMutable.CURRENT_VERSION + "." + LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT);

    assertThat(schema.loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isTrue();

    assertThat(schema.getFileByIdIfExists(fileId)).as("a file with no header page must not be served as an index").isNull();
    assertThat(fileManager.existsFile(fileId)).as("the load must not delete what it cannot read (#4743)").isTrue();

    assertLookupsAnswer();
  }

  /**
   * The shape of the report: the schema binds the index to a compaction's new mutable whose header never arrived. The
   * lookups the reporter's Cypher query made failed with the {@code ArrayIndexOutOfBoundsException}; they are now
   * answered without the index.
   */
  @Test
  void anIndexBoundToAHeaderlessFileAnswersWithoutIt() throws Exception {
    final IndexInternal bucketIndex = database.getSchema().getType(TYPE).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
    final LSMTreeIndexMutable mutable = ((LSMTreeIndex) bucketIndex).getMutableIndex();
    final String mutableName = mutable.getName();
    final File mutableFile = new File(((DatabaseInternal) database).getFileManager().getFile(mutable.getFileId()).getFilePath());
    final String compactedName = LSMTreeIndexAbstract.compactionBaseName(mutableName) + "_" + System.nanoTime();

    database.close();

    // What a follower holds after the entry that creates the compaction's files, once a later schema names the new
    // mutable: the file exists, it has no page, and the schema binds the index to it
    final File schemaFile = new File(database.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
    final String schemaJson = Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8);
    assertThat(schemaJson).contains(mutableName);
    Files.writeString(schemaFile.toPath(), schemaJson.replace(mutableName, compactedName), StandardCharsets.UTF_8);
    final File prevSchemaFile = new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME);
    if (prevSchemaFile.exists())
      Files.delete(prevSchemaFile.toPath());

    final File headerless = new File(mutableFile.getParentFile(), mutableFile.getName().replace(mutableName, compactedName));
    Files.delete(mutableFile.toPath());
    assertThat(headerless.createNewFile()).isTrue();

    reopenDatabase();

    assertLookupsAnswer();
    assertThat(headerless.exists()).as("the load must not delete what it cannot read (#4743)").isTrue();

    // The definition survives a save and a reopen: dropping it would lose the user's index for good
    database.getSchema().createDocumentType("Issue10046Unrelated");
    reopenDatabase();
    assertThat(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8))
        .as("the definition of an index waiting for its header must stay in the schema file").contains(compactedName);
    assertLookupsAnswer();

    // Creating the index again replaces it, and only then does the old definition leave the schema file
    database.getSchema().getType(TYPE).createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");
    assertThat(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8)).doesNotContain(compactedName);
    reopenDatabase();
    assertThat(database.getSchema().getType(TYPE).getAllIndexes(false)).hasSize(1);
    assertLookupsAnswer();
  }

  @Test
  void aNegativeCompactionStampIsStrippedLikeAPositiveOne() {
    assertThat(LSMTreeIndexAbstract.compactionBaseName("Type_0_-1234")).isEqualTo("Type_0");
    assertThat(LSMTreeIndexAbstract.compactionBaseName("Type_0_1234")).isEqualTo("Type_0");
    assertThat(LSMTreeIndexAbstract.compactionBaseName("Manual_-")).isEqualTo("Manual_-");
    assertThat(LSMTreeIndexAbstract.compactionBaseName("Manual_Idx")).isEqualTo("Manual_Idx");
  }

  /**
   * The mutable that would claim a compacted file is the one waiting for its header, so the load's orphan sweep must not
   * delete that compacted file: once the header arrives, the index would come back without its compacted entries.
   */
  @Test
  void theCompactedFileOfAHeaderlessMutableIsNotSweptAway() throws Exception {
    final String typeName = "Issue10046Compacted";
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName(typeName).withTotalBuckets(1).create().createProperty("id", Type.INTEGER);
      database.getSchema().buildTypeIndex(typeName, new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
          .withPageSize(2048).create();
    });
    for (int batch = 0; batch < 3; batch++) {
      final int base = batch * 1_000;
      database.transaction(() -> {
        for (int i = base; i < base + 1_000; i++)
          database.newVertex(typeName).set("id", i).save();
      });
    }

    final LSMTreeIndex index = (LSMTreeIndex) database.getSchema().getType(typeName).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
    database.async().waitCompletion();
    for (int attempt = 0; attempt < 200 && !index.scheduleCompaction(); ++attempt)
      Thread.sleep(25); // a background compaction owns the status; let it finish
    index.compact();
    final LSMTreeIndexCompacted compacted = index.getMutableIndex().getSubIndex();
    assertThat(compacted).as("the fixture needs a compacted component").isNotNull();

    final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
    final File compactedFile = new File(fileManager.getFile(compacted.getFileId()).getFilePath());
    final String mutableName = index.getMutableIndex().getName();
    final File mutableFile = new File(fileManager.getFile(index.getMutableIndex().getFileId()).getFilePath());
    final String headerlessName = LSMTreeIndexAbstract.compactionBaseName(mutableName) + "_" + System.nanoTime();

    database.close();

    final File schemaFile = new File(database.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
    Files.writeString(schemaFile.toPath(),
        Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8).replace(mutableName, headerlessName), StandardCharsets.UTF_8);
    final File prevSchemaFile = new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME);
    if (prevSchemaFile.exists())
      Files.delete(prevSchemaFile.toPath());
    Files.delete(mutableFile.toPath());
    assertThat(new File(mutableFile.getParentFile(), mutableFile.getName().replace(mutableName, headerlessName)).createNewFile())
        .isTrue();

    reopenDatabase();

    assertThat(compactedFile.exists()).as("the compacted file of a mutable waiting for its header must survive the load")
        .isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName + " WHERE id = 2500")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }

  private void assertLookupsAnswer() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + TYPE + " {id: 7}) RETURN n.id AS id")) {
      final List<Result> rows = rs.stream().toList();
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).<Integer>getProperty("id")).isEqualTo(7);
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + TYPE + ") WHERE n.id >= 15 RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(5L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE id = 3")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }
}
