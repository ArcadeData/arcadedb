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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * A REBUILD INDEX of an earlier version stopped half way left mutable bucket index files on disk that no schema entry references
 *. Every open re-created them as type-less indexes, and the ones whose compacted file was already gone
 * logged "Invalid sub-index for index 'T_4_423537393901917' ... Run 'REBUILD INDEX `T_4_423537393901917`'", a name no statement
 * can reach. The open now drops them, and leaves alone what a schema entry or a user owns.
 * <p>
 * The leftover is made the way it arises: a copy of the mutable file of a bucket index, under a bucket index name and a free file
 * id, taken after a first rebuild gave the index a compacted file and put in place once a second one retired it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class UnreferencedBucketIndexFilesTest extends TestHelper {
  private static final String TYPE    = "Form";
  private static final String INDEX   = TYPE + "[uid]";
  private static final int    RECORDS = 2_000;

  @Test
  void anOpenDropsTheBucketIndexFilesNoSchemaEntryReferences() throws IOException {
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(4).create();
    type.createProperty("uid", Type.STRING);
    database.getSchema().buildTypeIndex(TYPE, new String[] { "uid" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
        .create();
    database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.LSM_TREE).create();
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("uid", "key-" + i).save();
    });
    // the bucket indexes get a compacted file
    database.command("sql", "REBUILD INDEX `" + INDEX + "`");

    final IndexInternal bucketIndex = ((TypeIndex) database.getSchema().getIndexByName(INDEX)).getIndexesOnBuckets()[1];
    final String bucketName = database.getSchema().getBucketById(bucketIndex.getAssociatedBucketId()).getName();
    final ComponentFile mutableFile = ((DatabaseInternal) database).getFileManager().getFile(bucketIndex.getFileId());
    final Path source = Path.of(mutableFile.getFilePath());
    int freeFileId = 0;
    for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles())
      if (file != null)
        freeFileId = Math.max(freeFileId, file.getFileId());
    freeFileId += 50;

    final String orphanName = bucketName + "_423537393901917";
    // <component>.<fileId>.<pageSize>.v<version>.<extension>
    final String sourceName = source.getFileName().toString();
    final String afterId = sourceName.substring(sourceName.indexOf('.', mutableFile.getComponentName().length() + 1));
    final Path orphan = source.resolveSibling(orphanName + "." + freeFileId + afterId);

    database.close();
    final byte[] leftover = Files.readAllBytes(source);
    reopenDatabase();

    // the second rebuild retires the compacted file the copy points to, and the copy appears afterwards: the state the support
    // case reopened in, a leftover whose compacted file is gone
    database.command("sql", "REBUILD INDEX `" + INDEX + "`");
    database.close();
    Files.write(orphan, leftover);

    // the open that drops the leftover does not first report it as an index to rebuild under a name no statement can reach
    final CapturingHandler handler = new CapturingHandler();
    final Logger mutableLogger = Logger.getLogger(LSMTreeIndexMutable.class.getName());
    final Logger schemaLogger = Logger.getLogger(LocalSchema.class.getName());
    final Level mutableLevel = mutableLogger.getLevel();
    final Level schemaLevel = schemaLogger.getLevel();
    mutableLogger.addHandler(handler);
    schemaLogger.addHandler(handler);
    // the test logging configuration can filter WARNING out before any handler sees it
    mutableLogger.setLevel(Level.ALL);
    schemaLogger.setLevel(Level.ALL);
    try {
      reopenDatabase();
    } finally {
      mutableLogger.removeHandler(handler);
      schemaLogger.removeHandler(handler);
      mutableLogger.setLevel(mutableLevel);
      schemaLogger.setLevel(schemaLevel);
    }
    assertThat(handler.records).noneMatch(m -> m.contains("Invalid sub-index"));
    assertThat(handler.records).anyMatch(m -> m.contains("Dropping index file") && m.contains(orphanName));

    assertThat(Files.exists(orphan)).as("the unreferenced bucket index file is dropped").isFalse();
    assertThat(database.getSchema().existsIndex(orphanName)).isFalse();
    for (final Index index : database.getSchema().getIndexes())
      assertThat(index.getTypeName() != null || index.getName().equals("manualIdx")).as("index %s", index.getName()).isTrue();

    // what the schema and the user own is untouched
    assertThat(database.getSchema().existsIndex("manualIdx")).isTrue();
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(index.isValid()).isTrue();
    assertThat(index.countEntries()).isEqualTo(RECORDS);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-7").save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  /**
   * A schema.json that cannot be read makes the open fall back to schema.prev.json, which can predate the last index created: its
   * bucket index files are not in that schema, and they are not leftovers either. The open that fell back drops nothing.
   */
  @Test
  void anOpenThatFallsBackToThePreviousSchemaDropsNothing() throws IOException {
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(2).create();
    type.createProperty("uid", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument(TYPE).set("uid", "key-" + i).save();
    });
    database.getSchema().buildTypeIndex(TYPE, new String[] { "uid" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
    final IndexInternal bucketIndex = ((TypeIndex) database.getSchema().getIndexByName(INDEX)).getIndexesOnBuckets()[0];
    final Path indexFile = Path.of(((DatabaseInternal) database).getFileManager().getFile(bucketIndex.getFileId()).getFilePath());
    final Path databasePath = Path.of(database.getDatabasePath());
    database.close();

    final Path previous = databasePath.resolve(LocalSchema.SCHEMA_PREV_FILE_NAME);
    assertThat(Files.exists(previous)).isTrue();
    // the previous copy predates the index: the state a fallback can find
    assumeFalse(Files.readString(previous).contains(bucketIndex.getName()), "schema.prev.json already holds the index");
    Files.writeString(databasePath.resolve(LocalSchema.SCHEMA_FILE_NAME), "{ corrupt");

    reopenDatabase();
    assertThat(Files.exists(indexFile)).as("the bucket index file a newer schema referenced is kept").isTrue();
  }

  /**
   * A schema.json that parses at its root but fails inside the index definitions leaves the load with only the entries read before
   * the failure: no published generation exists yet on a first open, so the load goes on, and the sweep must not judge files
   * against that partial list.
   */
  @Test
  void anOpenWhoseSchemaFailsInsideTheIndexDefinitionsDropsNothing() throws IOException {
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(2).create();
    type.createProperty("uid", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument(TYPE).set("uid", "key-" + i).save();
    });
    database.getSchema().buildTypeIndex(TYPE, new String[] { "uid" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
    final List<Path> indexFiles = new ArrayList<>();
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(INDEX)).getIndexesOnBuckets())
      indexFiles.add(Path.of(((DatabaseInternal) database).getFileManager().getFile(bucketIndex.getFileId()).getFilePath()));
    final Path databasePath = Path.of(database.getDatabasePath());
    database.close();

    // no previous copy to fall back to, and every bucket index definition of the type unreadable
    Files.deleteIfExists(databasePath.resolve(LocalSchema.SCHEMA_PREV_FILE_NAME));
    final Path schemaFile = databasePath.resolve(LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject root = new JSONObject(Files.readString(schemaFile));
    final JSONObject indexes = root.getJSONObject("types").getJSONObject(TYPE).getJSONObject("indexes");
    for (final String indexName : indexes.keySet())
      indexes.getJSONObject(indexName).put("properties", 42);
    Files.writeString(schemaFile, root.toString());

    reopenDatabase();
    for (final Path indexFile : indexFiles)
      assertThat(Files.exists(indexFile)).as("bucket index file %s is kept", indexFile.getFileName()).isTrue();
  }

  private static final class CapturingHandler extends Handler {
    private final List<String> records = new CopyOnWriteArrayList<>();

    @Override
    public void publish(final LogRecord record) {
      if (record == null || record.getLevel().intValue() < Level.WARNING.intValue())
        return;
      String message = record.getMessage();
      if (message != null && record.getParameters() != null && record.getParameters().length > 0)
        try {
          message = message.formatted(record.getParameters());
        } catch (final Exception ignored) {
          // keep the pattern
        }
      if (message != null)
        records.add(message);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  }
}
