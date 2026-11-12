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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11539: gzip was about 90% of the time of an {@code EXPORT DATABASE}, at the default level on the thread that
 * formats the records, and neither could be set. {@code compressionLevel} (0-9, default 6, the level every export was
 * written at) and {@code compressionThreads} (-1 automatic, the default; 0 the single-threaded stream; N a pool) now set
 * them, named like the {@code BACKUP DATABASE} settings, from SQL, the {@link Exporter} API and the command line. Every
 * combination writes one standard gzip member that the importer reads back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11539ExportCompressionSettingsTest {
  private static final String DATABASE_PATH = "target/databases/Issue11539ExportCompressionSettingsTest";
  private static final String IMPORT_PATH   = "target/databases/Issue11539ExportCompressionSettingsTest-import";
  private static final int    VERTICES      = 20_000;

  private final List<File> exportFiles = new ArrayList<>();

  @BeforeEach
  void setUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(IMPORT_PATH));
  }

  @AfterEach
  void tearDown() {
    for (final File file : exportFiles)
      file.delete();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(IMPORT_PATH));
    TestHelper.checkActiveDatabases();
  }

  @Test
  void everyLevelAndThreadCountExportsAFileTheImporterReadsBack() throws IOException {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createGraph(database);

      final String[][] combinations = { { "0", "0" }, { "0", "4" }, { "1", "0" }, { "1", "4" }, { "6", "-1" }, { "9", "2" } };
      for (final String[] combination : combinations) {
        final File exportFile = newExportFile();
        database.command("sql", "EXPORT DATABASE file://" + exportFile.getName() + " WITH compressionLevel = " + combination[0]
            + ", compressionThreads = " + combination[1]);

        final String as = "compressionLevel = %s, compressionThreads = %s".formatted(combination[0], combination[1]);
        assertThat(exportFile).as(as).exists();
        assertImportsBack(exportFile, as);
      }
    }
  }

  /**
   * Level 0 stores, so the file is the plain text and then some; the parallel stream at the default level writes the size
   * the single-threaded one does (the chunks start with an empty dictionary, a fraction of a percent); level 1 sits between.
   */
  @Test
  void theLevelSetsTheSizeAndTheThreadsDoNot() throws IOException {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createGraph(database);

      final long stored = export(database, " WITH compressionLevel = 0").length();
      final long fastest = export(database, " WITH compressionLevel = 1, compressionThreads = 4").length();
      final File singleThreadedFile = export(database, " WITH compressionThreads = 0");
      final File parallelFile = export(database, " WITH compressionThreads = 4");
      final File byDefaultFile = export(database, "");
      final long singleThreaded = singleThreadedFile.length();
      final long parallel = parallelFile.length();
      final long byDefault = byDefaultFile.length();

      // THE CHUNKED STREAM IS NOT BYTE FOR BYTE THE SINGLE-THREADED ONE: THIS IS WHAT SAYS THE THREADS REACHED IT
      assertThat(Files.mismatch(parallelFile.toPath(), singleThreadedFile.toPath())).isNotEqualTo(-1L);
      assertThat(Files.mismatch(byDefaultFile.toPath(), singleThreadedFile.toPath())).isNotEqualTo(-1L);
      // AND IT CARRIES THE SAME TEXT: ONLY THE TIME OF THE EXPORT DIFFERS
      assertThat(withoutExportTime(gunzip(parallelFile))).isEqualTo(withoutExportTime(gunzip(singleThreadedFile)));

      assertThat(stored).isGreaterThan(fastest);
      assertThat(fastest).isGreaterThan(singleThreaded);
      assertThat((double) parallel).isLessThan(singleThreaded * 1.01);
      assertThat((double) byDefault).isLessThan(singleThreaded * 1.01);
    }
  }

  /**
   * The default export (no setting) is the parallel stream at level 6: the same bytes, inflated, as the plain text the
   * single-threaded stream compresses, and the same header {@code GZIPOutputStream} writes.
   */
  @Test
  void theDefaultExportIsTheSameTextAsTheSingleThreadedOne() throws IOException {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createGraph(database);

      final ExporterSettings defaults = new ExporterSettings();
      assertThat(defaults.compressionLevel).isEqualTo(6);
      assertThat(defaults.compressionThreads).isEqualTo(-1);

      final File byDefault = export(database, "");
      final String text = gunzip(byDefault);
      assertThat(text).contains("\"t\":\"schema\"").contains("Person");

      final ByteArrayOutputStream header = new ByteArrayOutputStream();
      try (final OutputStream out = new GZIPOutputStream(header)) {
        out.write('x');
      }
      final byte[] expected = new byte[10];
      final byte[] actual = new byte[10];
      System.arraycopy(header.toByteArray(), 0, expected, 0, 10);
      try (final InputStream in = new FileInputStream(byDefault)) {
        assertThat(in.readNBytes(actual, 0, 10)).isEqualTo(10);
      }
      assertThat(actual).isEqualTo(expected);
    }
  }

  @Test
  void anInvalidValueIsRefusedBeforeAnythingIsWritten() {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createGraph(database);

      final File exportFile = newExportFile();
      for (final String with : new String[] { "compressionLevel = 10", "compressionLevel = -1", "compressionLevel = 'fast'",
          "compressionLevel = ''", "compressionThreads = -2", "compressionThreads = 257", "compressionThreads = 'many'" })
        assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://" + exportFile.getName() + " WITH " + with))//
            .as(with)//
            .hasStackTraceContaining("Invalid value")//
            .hasStackTraceContaining("Supported values are");

      assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://" + exportFile.getName() + " WITH compresionLevel = 1"))//
          .hasStackTraceContaining("Did you mean 'compressionLevel'?");

      assertThat(exportFile).doesNotExist();
    }
  }

  @Test
  void theApiAndTheCommandLineSetThemToo() throws IOException {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      createGraph(database);

      final File exportFile = newExportFile();
      final Exporter exporter = new Exporter(database, exportFile.getPath()).setFormat("jsonl").setCompressionLevel(1)
          .setCompressionThreads(3);
      assertThat(exporter.settings.compressionLevel).isEqualTo(1);
      assertThat(exporter.settings.compressionThreads).isEqualTo(3);
      exporter.exportDatabase();
      assertImportsBack(exportFile, "API");

      // THE PYTHON BINDINGS AND THE HTTP PATH PASS THEM AS A SETTINGS MAP
      final Exporter mapped = new Exporter(database, newExportFile().getPath());
      mapped.setSettings(Map.of("compressionLevel", "9", "compressionThreads", "0"));
      assertThat(mapped.settings.compressionLevel).isEqualTo(9);
      assertThat(mapped.settings.compressionThreads).isZero();

      assertThatThrownBy(() -> new Exporter(database, "x").setCompressionLevel(10)).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("-compressionLevel");
      assertThatThrownBy(() -> new Exporter(database, "x").setCompressionThreads(-2)).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("-compressionThreads");
    }

    final ExporterSettings settings = new ExporterSettings();
    settings.parseParameters(new String[] { "-format", "jsonl", "-compressionLevel", "1", "-compressionThreads", "0", "-o" });
    assertThat(settings.compressionLevel).isEqualTo(1);
    assertThat(settings.compressionThreads).isZero();
    assertThat(settings.overwriteFile).isTrue();

    // A SETTING NAME IS AN OPTION, NOT A VALUE: '-compressionLevel -o' IS A MISSING VALUE
    assertThatThrownBy(() -> new ExporterSettings().parseParameters(new String[] { "-format", "jsonl", "-compressionLevel", "-o" }))//
        .hasMessageContaining("Missing value for -compressionLevel");
  }

  private File export(final Database database, final String with) {
    final File exportFile = newExportFile();
    database.command("sql", "EXPORT DATABASE file://" + exportFile.getName() + with);
    return exportFile;
  }

  private void assertImportsBack(final File exportFile, final String as) {
    FileUtils.deleteRecursively(new File(IMPORT_PATH));
    try (final Database target = new DatabaseFactory(IMPORT_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + exportFile.getAbsolutePath());
      assertThat(target.countType("Person", false)).as(as).isEqualTo(VERTICES);
      assertThat(target.countType("Knows", false)).as(as).isEqualTo(VERTICES - 1);
      assertThat(target.query("sql", "SELECT sum(age) AS total FROM Person").next().<Number>getProperty("total").longValue())//
          .as(as).isEqualTo(expectedAgeSum());
    } finally {
      FileUtils.deleteRecursively(new File(IMPORT_PATH));
    }
  }

  private File newExportFile() {
    final File file = new File("exports", "issue11539-" + UUID.randomUUID() + ".jsonl.tgz");
    exportFiles.add(file);
    return file;
  }

  private static long expectedAgeSum() {
    long sum = 0;
    for (int i = 0; i < VERTICES; i++)
      sum += i % 97;
    return sum;
  }

  /** A few MB of text: several 1 MB chunks, so the parallel stream really runs in parallel. */
  private static void createGraph(final Database database) {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("Knows");
    database.transaction(() -> {
      MutableVertex previous = null;
      for (int i = 0; i < VERTICES; i++) {
        final MutableVertex vertex = database.newVertex("Person").set("id", i).set("age", i % 97)
            .set("name", "Person number " + i + " of the export compression test").save();
        if (previous != null)
          previous.newEdge("Knows", vertex);
        previous = vertex;
      }
    });
  }

  private static String withoutExportTime(final String text) {
    return text.replaceAll("\"executedOn(Timestamp)?\":(\"[^\"]*\"|\\d+)", "");
  }

  private static String gunzip(final File file) throws IOException {
    try (final InputStream in = new GZIPInputStream(new FileInputStream(file))) {
      return new String(in.readAllBytes(), DatabaseFactory.getDefaultCharset());
    }
  }
}
