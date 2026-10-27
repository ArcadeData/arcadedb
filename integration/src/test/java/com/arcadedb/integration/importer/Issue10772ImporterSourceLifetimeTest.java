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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10772: an import held its source in one field that each of its four phases (url, documents, vertices, edges)
 * overwrote, and only the last value was closed, so a {@code -vertices X -edges Y} import leaked the vertices source. The
 * analysis of a source in {@code SourceDiscovery.getSchema} closed it as a plain statement after the analysis, so every refusal
 * raised by the analysis leaked it too. Each acquisition is now a try-with-resources scope.
 * <p>
 * The assertion is "every source the import opened is closed", over all of them, rather than a count of the phases: a phase added
 * later is covered without touching this test.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10772ImporterSourceLifetimeTest {
  private static final String DB_PATH   = "target/databases/issue-10772-source-lifetime";
  private static final String DATA_PATH = "target/test-data/issue-10772-source-lifetime";

  private final List<Source> opened = new ArrayList<>();
  private       File         dataDir;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
    dataDir = new File(DATA_PATH);
    dataDir.mkdirs();
  }

  @AfterEach
  void cleanup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
  }

  @Test
  void aVerticesAndEdgesImportClosesEverySourceItOpened() throws Exception {
    final Importer importer = recordingImporter("-vertices", write("vertices.csv", "Id,Name", "0,Jay", "1,Kim", "2,Lee"),
        "-edges", write("edges.csv", "From,To", "0,1", "1,2"),
        "-database", DB_PATH,
        "-typeIdProperty", "Id",
        "-typeIdType", "Long",
        "-typeIdUnique", "true",
        "-forceDatabaseCreate", "true",
        "-edgeFromField", "From",
        "-edgeToField", "To");
    importer.load();

    try (final Database db = new DatabaseFactory(DB_PATH).open()) {
      assertThat(db.countType("Node", true)).isEqualTo(3);
      assertThat(db.countType("Relationship", true)).isEqualTo(2);
    }

    // AN ANALYSIS AND A LOAD FOR EACH OF THE TWO FILES
    assertThat(opened).hasSize(4);
    assertEveryOpenedSourceIsClosed();
  }

  @Test
  void aSourceWhoseAnalysisIsRefusedIsClosed() throws Exception {
    // THIS MODULE'S CLASSPATH HAS NO arcadedb-gremlin, SO A GRAPHML SOURCE IS REFUSED FROM INSIDE THE ANALYSIS (#7781)
    final Importer importer = recordingImporter("-url", write("graph.graphml", "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
            "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"><graph id=\"G\"><node id=\"n0\"/></graph></graphml>"),
        "-database", DB_PATH,
        "-forceDatabaseCreate", "true");

    assertThatThrownBy(importer::load).isInstanceOf(ImportException.class).hasMessageContaining("arcadedb-gremlin");

    assertThat(opened).isNotEmpty();
    assertEveryOpenedSourceIsClosed();
  }

  @Test
  void aSourceWhoseAnalysisIsRefusedIsClosedWhenTheDiscoveryIsUsedAlone() throws Exception {
    final String url = write("unknown.bin", "\u0001\u0002\u0003\u0004");
    final SourceDiscovery discovery = recordingDiscovery(url);
    final ImporterSettings settings = new ImporterSettings();
    settings.parseParameter("url", url);

    assertThatThrownBy(() -> discovery.getSchema(settings, AnalyzedEntity.EntityType.DATABASE, new AnalyzedSchema(100),
        new ConsoleLogger(0))).isInstanceOf(ImportException.class).hasMessageContaining("Cannot determine the file type");

    assertThat(opened).hasSize(1);
    assertEveryOpenedSourceIsClosed();
  }

  @Test
  void aProbeOnlyImportClosesTheSourceItProbed() throws Exception {
    final Importer importer = recordingImporter("-url", write("probe.csv", "Id,Name", "0,Jay"),
        "-database", DB_PATH,
        "-forceDatabaseCreate", "true",
        "-probeOnly", "true");
    importer.load();

    assertThat(opened).hasSize(1);
    assertEveryOpenedSourceIsClosed();
  }

  @Test
  void closingASourceTwiceIsHarmless() throws Exception {
    final Source source = new SourceDiscovery(write("twice.csv", "Id", "0")).getSource();
    assertThat(source.isClosed()).isFalse();
    source.close();
    source.close();
    assertThat(source.isClosed()).isTrue();
  }

  private void assertEveryOpenedSourceIsClosed() {
    for (final Source source : opened)
      assertThat(source.isClosed()).as("source %s is closed", source).isTrue();
  }

  private Importer recordingImporter(final String... args) {
    return new Importer(args) {
      @Override
      protected SourceDiscovery newSourceDiscovery(final String url) {
        return recordingDiscovery(url);
      }
    };
  }

  private SourceDiscovery recordingDiscovery(final String url) {
    return new SourceDiscovery(url) {
      @Override
      public Source getSource() throws IOException {
        final Source source = super.getSource();
        opened.add(source);
        return source;
      }
    };
  }

  private String write(final String fileName, final String... lines) throws IOException {
    final File f = new File(dataDir, fileName);
    Files.write(f.toPath(), String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
    return f.getAbsolutePath();
  }
}
