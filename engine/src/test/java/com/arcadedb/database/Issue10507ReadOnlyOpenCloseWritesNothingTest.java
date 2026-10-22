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
package com.arcadedb.database;

import com.arcadedb.engine.ComponentFile;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10507, the general form of the rule: a database opened {@code READ_ONLY} does not own its directory (a read-write process may
 * have it open, and the engine lets it), so neither the open nor the close writes, rewrites or deletes any file in it.
 * <p>
 * {@link Issue10507ReadOnlyCloseTest} pins the files the report named. This one takes the whole directory: it is listed before and
 * after the read-only handle's life, with the modification time of every file moved to the past first, so a rewrite of the same
 * bytes is seen too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10507ReadOnlyOpenCloseWritesNothingTest {
  private static final String DB_PATH    = "target/test-databases/Issue10507ReadOnlyOpenCloseWritesNothingTest";
  private static final int    DIMENSIONS = 8;
  private static final int    VECTORS    = 50;
  /** The size from which a vector index defers its graph build on close (LSMVectorIndex.ASYNC_REBUILD_MIN_GRAPH_SIZE). */
  private static final int    DEFERRED_GRAPH_VECTORS = 1000;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aPlainDatabaseIsLeftAsItWas() throws IOException {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          db.getSchema().createDocumentType("Doc").createProperty("id", Type.INTEGER);
          db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "id");
          for (int i = 0; i < 100; i++)
            db.newDocument("Doc").set("id", i).save();
        });
      } finally {
        db.close();
      }
    }

    assertThatReadOnlyLifeWritesNothing("Doc", 100);
  }

  @Test
  void aVectorIndexWhoseGraphTheWritersCloseDeferredIsLeftAsItWas() throws IOException {
    // AT OR ABOVE THIS SIZE THE CLOSE OF A WRITER DOES NOT BUILD THE GRAPH: IT RECORDS THE DEFERRAL IN THE MANIFEST AND LEAVES THE
    // GRAPH FILE EMPTY. A READ-ONLY CLOSE FINDS THE SAME STATE, AND MUST NOT RECORD ANYTHING
    createVectorDatabase(DEFERRED_GRAPH_VECTORS);

    assertThatReadOnlyLifeWritesNothing("Doc", DEFERRED_GRAPH_VECTORS);
  }

  @Test
  void aVectorIndexBelowTheDeferralSizeWhoseGraphIsEmptyIsLeftAsItWas() throws IOException {
    // BELOW THAT SIZE THE CLOSE OF A WRITER BUILDS AND PERSISTS THE GRAPH, WHICH A READ-ONLY CLOSE MUST NOT DO, NOT EVEN FOR A COPY
    // OF THE DIRECTORY MADE BEFORE THE WRITER CLOSED (THE GRAPH FILE IS THERE, WITHOUT A GRAPH IN IT)
    createVectorDatabase(VECTORS);
    final File[] graphFiles = new File(DB_PATH).listFiles((dir, name) -> name.endsWith(".vecgraph"));
    assertThat(graphFiles).as("the writer's close persisted the graph, so the fixture can empty it").isNotEmpty();
    for (final File graphFile : graphFiles)
      try (final FileChannel channel = FileChannel.open(graphFile.toPath(), StandardOpenOption.WRITE)) {
        channel.truncate(0);
      }
    final File[] manifests = new File(DB_PATH).listFiles((dir, name) -> name.endsWith(".vecgraphfp"));
    for (final File manifest : manifests)
      assertThat(manifest.delete()).isTrue();

    assertThatReadOnlyLifeWritesNothing("Doc", VECTORS);
  }

  private static void createVectorDatabase(final int vectors) {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          db.getSchema().createDocumentType("Doc").createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });
        db.transaction(() -> {
          final Random random = new Random(10507);
          for (int i = 0; i < vectors; i++) {
            final float[] vector = new float[DIMENSIONS];
            for (int d = 0; d < DIMENSIONS; d++)
              vector[d] = random.nextFloat();
            db.newDocument("Doc").set("vector", vector).save();
          }
        });
      } finally {
        db.close();
      }
    }
  }

  private static void assertThatReadOnlyLifeWritesNothing(final String type, final long expectedCount) throws IOException {
    final long oldTimestamp = System.currentTimeMillis() - 3_600_000L;
    for (final File file : new File(DB_PATH).listFiles())
      if (file.isFile())
        assertThat(file.setLastModified(oldTimestamp)).isTrue();

    final Map<String, String> before = snapshot();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open(ComponentFile.MODE.READ_ONLY);
      try {
        assertThat(db.countType(type, true)).isEqualTo(expectedCount);
      } finally {
        db.close();
      }
    }

    assertThat(snapshot()).as("the files of the directory after a READ_ONLY open and close").isEqualTo(before);
  }

  /** File name to size, modification time and hash of the content, for every file of the directory. */
  private static Map<String, String> snapshot() throws IOException {
    final Map<String, String> files = new TreeMap<>();
    for (final File file : new File(DB_PATH).listFiles())
      if (file.isFile())
        files.put(file.getName(), file.length() + "/" + file.lastModified() + "/" + Arrays.hashCode(Files.readAllBytes(file.toPath())));
    return files;
  }
}
