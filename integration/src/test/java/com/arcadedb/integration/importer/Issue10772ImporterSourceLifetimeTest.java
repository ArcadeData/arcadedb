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
import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
  void anOrientDBImportClosesTheStreamOfEachOfItsPasses() throws Exception {
    final URL export = Issue10772ImporterSourceLifetimeTest.class.getClassLoader().getResource("orientdb-export-small.gz");
    final List<boolean[]> passes = new ArrayList<>();

    final OrientDBImporter importer = new OrientDBImporter(("-i " + export.getFile() + " -d " + DB_PATH + " -o").split(" ")) {
      @Override
      public GZIPInputStream openInputStream() throws IOException {
        final boolean[] closed = new boolean[1];
        passes.add(closed);
        return new GZIPInputStream(new FileInputStream(export.getFile())) {
          @Override
          public void close() throws IOException {
            closed[0] = true;
            super.close();
          }
        };
      }
    };
    importer.run().close();
    assertThat(importer.isError()).isFalse();

    // THE FILE IS READ ONCE TO COUNT, ONCE FOR THE SCHEMA AND ONCE FOR THE EDGES
    assertThat(passes).hasSize(3);
    for (final boolean[] closed : passes)
      assertThat(closed[0]).isTrue();
  }

  @Test
  void closingASourceTwiceIsHarmless() throws Exception {
    final Source source = new SourceDiscovery(write("twice.csv", "Id", "0")).getSource();
    assertThat(source.isClosed()).isFalse();
    source.close();
    source.close();
    assertThat(source.isClosed()).isTrue();
  }

  @Test
  void theReleaseCallbackRunsWhenTheStreamFailsToClose() {
    final boolean[] released = new boolean[1];
    final Source source = new Source("test", new ByteArrayInputStream(new byte[0]) {
      @Override
      public void close() throws IOException {
        throw new IOException("cannot close");
      }
    }, 0, false, null, () -> {
      released[0] = true;
      return null;
    });

    source.close();

    assertThat(source.isClosed()).isTrue();
    assertThat(released[0]).isTrue();
  }

  @Test
  void aZipWithoutTheRequestedEntryLeavesNoFileOpen() throws Exception {
    assumeTrue(ManagementFactory.getOperatingSystemMXBean() instanceof UnixOperatingSystemMXBean,
        "counts the open file descriptors of the process");
    final UnixOperatingSystemMXBean os = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    final File zipFile = new File(dataDir, "archive.zip");
    try (final ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(zipFile))) {
      zip.putNextEntry(new ZipEntry("present.csv"));
      zip.write("Id\n0\n".getBytes(StandardCharsets.UTF_8));
      zip.closeEntry();
    }
    final String url = zipFile.getAbsolutePath() + ":::absent.csv";

    // NO Source REACHES THE CALLER, SO THE STREAM OPENED FOR IT HAS TO BE CLOSED WHERE IT WAS OPENED. EACH LEAKED ATTEMPT HOLDS
    // ONE DESCRIPTOR UNTIL A GC RUNS ITS CLEANER, SO A LEAK SHOWS AS A GROWTH CLOSE TO THE NUMBER OF ATTEMPTS
    final int attempts = 200;
    final long before = os.getOpenFileDescriptorCount();
    for (int i = 0; i < attempts; i++)
      assertThatThrownBy(() -> new SourceDiscovery(url).getSource()).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("absent.csv");
    assertThat(os.getOpenFileDescriptorCount() - before).isLessThan(attempts / 2);
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
