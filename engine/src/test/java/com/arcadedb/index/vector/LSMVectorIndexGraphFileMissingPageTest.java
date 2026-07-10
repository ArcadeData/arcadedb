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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for GitHub issue #4582:
 * {@code LSMVectorIndexGraphFile.computeTotalGraphBytes} NPEs on missing/evicted page.
 * <p>
 * When the component's page count is ahead of what is actually on disk (truncated/evicted graph
 * file), {@link com.arcadedb.engine.PageManager#getImmutablePage} returns {@code null} for the last
 * page. The old code dereferenced that null and aborted {@code loadGraph()} with a
 * {@link NullPointerException} instead of degrading to the "rebuild graph from scratch" recovery
 * path. After the fix {@code loadGraph()} must return {@code null} so callers rebuild.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexGraphFileMissingPageTest {
  private static final String DB_PATH = "target/test-databases/LSMVectorIndexGraphFileMissingPageTest";

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void cleanup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void loadGraphReturnsNullWhenLastPageMissing() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database database = factory.create()) {
        final DatabaseInternal dbInternal = (DatabaseInternal) database;
        final int pageSize = 65536;

        database.begin();
        final LSMVectorIndexGraphFile graphFile = new LSMVectorIndexGraphFile(
            dbInternal, "missing-page-test", dbInternal.getDatabasePath(), ComponentFile.MODE.READ_WRITE, pageSize);
        database.commit();

        // Simulate a truncated/evicted graph file: the component believes it has one page, but the
        // underlying file is still empty so the last page cannot be loaded from disk.
        graphFile.updatePageCount(1);
        assertThat(graphFile.getTotalPages()).isEqualTo(1);
        assertThat(graphFile.hasPersistedGraph()).isTrue();

        // Before the fix this threw NullPointerException; now it must degrade gracefully to null
        // so callers fall through to the rebuild-from-scratch recovery path.
        assertThatCode(() -> {
          final var graph = graphFile.loadGraph();
          assertThat(graph).as("missing last page must be treated as no persisted graph").isNull();
        }).doesNotThrowAnyException();
      }
    }
  }
}
