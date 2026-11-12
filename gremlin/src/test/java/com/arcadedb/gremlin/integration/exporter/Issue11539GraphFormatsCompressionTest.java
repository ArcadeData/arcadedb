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
package com.arcadedb.gremlin.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11539: the GraphML and GraphSON exports compressed through the same single-threaded {@code GZIPOutputStream} at the
 * default level as the JSONL one. They honour {@code compressionLevel} and {@code compressionThreads} too, and every
 * combination imports back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11539GraphFormatsCompressionTest {
  private static final String DATABASE_PATH = "target/databases/issue11539-graph-formats";
  private static final String IMPORT_PATH   = "target/databases/issue11539-graph-formats-import";
  private static final int    VERTICES      = 2_000;

  @BeforeEach
  @AfterEach
  void clean() {
    TestServerHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(IMPORT_PATH));
    for (final String format : new String[] { "graphson", "graphml" })
      for (final String level : new String[] { "0", "9", "default" })
        new File("exports/issue11539-" + level + "." + format + ".tgz").delete();
  }

  @ParameterizedTest
  @ValueSource(strings = { "graphson", "graphml" })
  void theGraphFormatsHonourTheCompressionSettings(final String format) {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("Knows");
      database.transaction(() -> {
        MutableVertex previous = null;
        for (int i = 0; i < VERTICES; i++) {
          final MutableVertex vertex = database.newVertex("Person").set("id", i).set("name", "Person " + i).save();
          if (previous != null)
            previous.newEdge("Knows", vertex);
          previous = vertex;
        }
      });

      final File stored = export(database, format, "0", " WITH compressionLevel = 0, compressionThreads = 2");
      final File smallest = export(database, format, "9", " WITH compressionLevel = 9, compressionThreads = 0");
      final File byDefault = export(database, format, "default", "");

      assertThat(stored.length()).isGreaterThan(byDefault.length());
      assertThat(byDefault.length()).isGreaterThanOrEqualTo(smallest.length());

      for (final File file : new File[] { stored, smallest, byDefault }) {
        FileUtils.deleteRecursively(new File(IMPORT_PATH));
        try (final Database target = new DatabaseFactory(IMPORT_PATH).create()) {
          target.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath());
          assertThat(target.countType("Person", false)).as(file.getName()).isEqualTo(VERTICES);
          assertThat(target.countType("Knows", false)).as(file.getName()).isEqualTo(VERTICES - 1);
        }
      }
    }
  }

  private static File export(final Database database, final String format, final String name, final String with) {
    final String file = "issue11539-" + name + "." + format + ".tgz";
    final String settings = with.isEmpty() ? " WITH format = '" + format + "'" : with + ", format = '" + format + "'";
    database.command("sql", "EXPORT DATABASE file://" + file + settings);
    return new File("exports", file);
  }
}
