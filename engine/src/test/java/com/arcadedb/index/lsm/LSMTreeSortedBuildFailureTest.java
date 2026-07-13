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
import com.arcadedb.index.IndexException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexBuildMode;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LSMTreeSortedBuildFailureTest extends TestHelper {
  @Test
  void removesPartialRunAndSchemaEntriesAfterSpillWriteFailure() throws Exception {
    final Path spillParent = Path.of(getDatabasePath()).resolve("spill-failure");
    createRecords("SpillFailure", 1, 1_000);

    final AtomicInteger written = new AtomicInteger();
    LSMTreeIndexExternalSorter.setSpillWriteTestHook(() -> {
      if (written.incrementAndGet() == 50)
        throw new IOException("injected spill exhaustion");
    });
    try {
      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("SpillFailure", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget(1L << 20)
          .withBuildSpillDirectory(spillParent)
          .withBuildMergeFanIn(2)
          .withUnique(false)
          .create())
          .isInstanceOf(IndexException.class)
          .hasStackTraceContaining("injected spill exhaustion");
    } finally {
      LSMTreeIndexExternalSorter.setSpillWriteTestHook(null);
    }

    assertThat(database.getSchema().getType("SpillFailure").getIndexByProperties("lookupKey")).isNull();
    assertNoSortedBuildArtifacts(spillParent);
    reopenDatabase();
    assertThat(database.getSchema().getType("SpillFailure").getIndexByProperties("lookupKey")).isNull();
  }

  @Test
  void removesGenerationOutputsAfterMaterializedMergeFailure() throws Exception {
    final Path spillParent = Path.of(getDatabasePath()).resolve("merge-failure");
    createRecords("MergeFailure", 1, 1_000);

    final AtomicInteger written = new AtomicInteger();
    LSMTreeIndexExternalSorter.setSpillWriteTestHook(() -> {
      if (written.incrementAndGet() == 1_050)
        throw new IOException("injected materialized merge failure");
    });
    try {
      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("MergeFailure", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget(1L << 20)
          .withBuildSpillDirectory(spillParent)
          .withBuildMergeFanIn(2)
          .withUnique(false)
          .create())
          .isInstanceOf(IndexException.class)
          .hasStackTraceContaining("injected materialized merge failure");
    } finally {
      LSMTreeIndexExternalSorter.setSpillWriteTestHook(null);
    }

    assertThat(written).hasValueGreaterThan(1_000);
    assertThat(database.getSchema().getType("MergeFailure").getIndexByProperties("lookupKey")).isNull();
    assertNoSortedBuildArtifacts(spillParent);
    reopenDatabase();
    assertThat(database.getSchema().getType("MergeFailure").getIndexByProperties("lookupKey")).isNull();
  }

  @Test
  void defersSchemaSaveAndCleansFilesAfterFirstBucketAttachment() throws Exception {
    createRecords("AttachmentFailure", 2, 2_000);
    final AtomicBoolean schemaStillUnpublished = new AtomicBoolean();

    LSMTreeIndexBulkLoader.setBuildTestHook((phase, index, completedBuckets) -> {
      if (phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_ATTACHMENT && completedBuckets == 1) {
        try {
          final JSONObject schema = new JSONObject(
              Files.readString(Path.of(database.getDatabasePath()).resolve("schema.json")));
          final JSONObject type = schema.getJSONObject("types").getJSONObject("AttachmentFailure");
          schemaStillUnpublished.set(!type.has("indexes") || type.getJSONObject("indexes").keySet().isEmpty());
        } catch (final IOException error) {
          throw new IllegalStateException("Cannot inspect schema publication boundary", error);
        }
        throw new IllegalStateException("injected failure after first attachment");
      }
    });
    try {
      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("AttachmentFailure", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget(16L << 20)
          .withUnique(false)
          .create())
          .isInstanceOf(IndexException.class)
          .hasStackTraceContaining("injected failure after first attachment");
    } finally {
      LSMTreeIndexBulkLoader.setBuildTestHook(null);
    }

    assertThat(schemaStillUnpublished).isTrue();
    assertThat(database.getSchema().getType("AttachmentFailure").getIndexByProperties("lookupKey")).isNull();
    assertNoIndexFiles();
    reopenDatabase();
    assertThat(database.getSchema().getType("AttachmentFailure").getIndexByProperties("lookupKey")).isNull();
  }

  @Test
  void removesUnattachedCompactedFileAfterEntryWriteFailure() throws Exception {
    createRecords("EntryWriteFailure", 1, 2_000);
    LSMTreeIndexBulkLoader.setBuildTestHook((phase, index, completedBuckets) -> {
      if (phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_ENTRY_WRITES)
        throw new IllegalStateException("injected failure after entry writes");
    });
    try {
      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("EntryWriteFailure", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget(16L << 20)
          .withUnique(false)
          .create())
          .isInstanceOf(IndexException.class)
          .hasStackTraceContaining("injected failure after entry writes");
    } finally {
      LSMTreeIndexBulkLoader.setBuildTestHook(null);
    }

    assertThat(database.getSchema().getType("EntryWriteFailure").getIndexByProperties("lookupKey")).isNull();
    assertNoIndexFiles();
  }

  private void createRecords(final String typeName, final int buckets, final int records) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
    type.createProperty("lookupKey", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        database.newDocument(typeName).set("lookupKey", i).save();
    });
  }

  private static void assertNoSortedBuildArtifacts(final Path parent) throws IOException {
    if (!Files.exists(parent))
      return;
    try (Stream<Path> paths = Files.list(parent)) {
      assertThat(paths.filter(path -> path.getFileName().toString()
          .startsWith(LSMTreeIndexExternalSorter.DIRECTORY_PREFIX)).toList()).isEmpty();
    }
  }

  private void assertNoIndexFiles() throws IOException {
    try (Stream<Path> files = Files.list(Path.of(database.getDatabasePath()))) {
      assertThat(files.filter(Files::isRegularFile).map(path -> path.getFileName().toString())
          .filter(name -> name.endsWith(".numtidx") || name.endsWith(".nuctidx")
              || name.endsWith(".temp_numtidx") || name.endsWith(".temp_nuctidx"))
          .toList()).isEmpty();
    }
  }
}
