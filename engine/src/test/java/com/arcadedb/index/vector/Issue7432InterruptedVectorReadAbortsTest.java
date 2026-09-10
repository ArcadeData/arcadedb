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
package com.arcadedb.index.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageManager;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7432, third defect: a vector read on an interrupted thread was logged and swallowed
 * like a bad vector.
 * <p>
 * A graph build is cancelled by interrupting the workers of its pool, and {@code PageManager} refuses I/O on an
 * interrupted thread and leaves the flag set. Every read in {@link ArcadePageVectorValues#getVector(int)} caught
 * its failure, logged it at WARNING, and fell through to the deleted sentinel - so after the interrupt each
 * worker went on reading, failing and logging, one line per node, until the executor's cancellation surfaced. A
 * 4.2M-vector build cancelled by a database close logged 42,700 such lines in five seconds and kept its pool alive
 * past the close's 5s grace. An interrupted read is a cancellation and must end the build at the first one.
 * <p>
 * Pinned at the read, where it is deterministic, rather than by racing a close against a build: the read only
 * reaches the interrupt check when the page is not cached, so each test reopens the database (which drops its
 * pages from the cache) and evicts again after the location index has been materialised.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7432InterruptedVectorReadAbortsTest {
  private static final String DB_ROOT    = "target/test-databases/Issue7432InterruptedVectorReadAbortsTest";
  private static final int    DIMENSIONS = 16;
  private static final int    RECORDS    = 20;

  private String dbPath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
    FileUtils.deleteRecursively(new File(dbPath));
    // The flag must not leak between tests whatever an assertion below does.
    Thread.interrupted();
  }

  @AfterEach
  void tearDown() {
    Thread.interrupted();
    FileUtils.deleteRecursively(new File(dbPath));
  }

  /**
   * The document path: an index without quantization reads the vector back from the record.
   */
  @Test
  void anInterruptedDocumentReadIsACancellationNotASentinel() {
    assertInterruptedReadAborts("{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\" }");
  }

  /**
   * The page path: a quantized index reads the vector back from its own pages
   * ({@code LSMVectorIndex.readVectorFromOffset}), which has a swallowing catch of its own.
   */
  @Test
  void anInterruptedQuantizedPageReadIsACancellationNotASentinel() {
    assertInterruptedReadAborts("{ \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"INT8\" }");
  }

  private void assertInterruptedReadAborts(final String indexMetadata) {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database created = factory.create();
      try {
        created.transaction(() -> {
          final var type = created.getSchema().createVertexType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          created.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA " + indexMetadata);
        });
        created.begin();
        for (int i = 0; i < RECORDS; i++)
          created.newVertex("Doc").set("id", i).set("vector", embedding(i)).save();
        created.commit();
      } finally {
        // Closing drops this database's pages from the cache; the reads below start cold.
        created.close();
      }

      final Database db = factory.open();
      try {
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc")
            .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];

        // Materialising the location index scans the index pages, so evict again after it.
        final VectorLocationIndex locations = index.getVectorIndex();
        final int vectorId = locations.getActiveVectorIds().findFirst().orElseThrow();
        PageManager.INSTANCE.removeAllReadPagesOfDatabase(db);

        final ArcadePageVectorValues values = ArcadePageVectorValues.forSearch((DatabaseInternal) db, DIMENSIONS,
            "vector", locations, new int[] { vectorId }, index);

        Thread.currentThread().interrupt();
        try {
          assertThatThrownBy(() -> values.getVector(0))
              .as("a read refused because the thread was interrupted is a cancellation, not a vector that "
                  + "could not be read: swallowing it fed the graph builder a sentinel per node and logged a "
                  + "line per node for the rest of the build (issue #7432)")
              .isInstanceOf(CancellationException.class);
        } finally {
          assertThat(Thread.interrupted()).as("the read leaves the interrupt flag for the caller to observe").isTrue();
        }

        // The control: the same read with the flag clear returns the vector, so the throw above was the
        // interruption and not a fixture that cannot read vectors at all.
        final VectorFloat<?> vector = values.getVector(0);
        assertThat(values.isDeletedSentinel(vector)).as("a real vector, not the sentinel").isFalse();
        assertThat(vector.length()).isEqualTo(DIMENSIONS);
      } finally {
        db.drop();
      }
    }
  }

  /** Deterministic per-id embedding, so a fixture is reproducible run to run. */
  private static float[] embedding(final int id) {
    final Random random = new Random(0x7432L * 17 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }
}
