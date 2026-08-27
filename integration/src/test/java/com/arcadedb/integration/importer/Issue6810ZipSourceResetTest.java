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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6810: importing any ZIP source silently yielded 0 records.
 * <p>
 * Both reset callbacks in {@link SourceDiscovery} rebuilt the {@link java.util.zip.ZipInputStream} but then called
 * {@code getNextEntry()} on the *old* (already closed, on the local-file path) stream instead of the new one. The new
 * stream was therefore left with no current entry, and {@link java.util.zip.ZipInputStream#read()} returns {@code -1}
 * while no entry is current, so the import read an empty input. {@link Source#reset()} only logged the resulting
 * {@code IOException("Stream closed")}, which is what turned a hard failure into a "successful" import of 0 records.
 * <p>
 * The reset path is not optional: {@code Importer.loadFromSource()} always calls {@code parser.reset()}, and so do
 * {@code SourceDiscovery.getSchema()} and {@code CSVImporterFormat.analyze()}.
 */
class Issue6810ZipSourceResetTest {

  private static final String CSV_CONTENT   = """
      Id,First Name,Last Name
      0,Jay,Miner
      1,John,Red
      2,Steve,Jobs
      3,Isaac,Newton
      4,Nikola,Tesla
      5,Albert,Einstein
      """;
  private static final String OTHER_CONTENT = """
      Id,First Name,Last Name
      10,Grace,Hopper
      """;

  private File tempDir;

  @BeforeEach
  void setUp() throws IOException {
    tempDir = Files.createTempDirectory("issue6810").toFile();
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(tempDir);
  }

  @Test
  void localZipSourceResetsToTheStartOfTheEntry() throws IOException {
    final File zip = writeZip("importer-vertices.csv.zip", "importer-vertices.csv", CSV_CONTENT);

    final Source source = new SourceDiscovery("file://" + zip.getAbsolutePath()).getSource();
    try {
      assertThat(readAfterReset(source)).isEqualTo(CSV_CONTENT);
    } finally {
      source.close();
    }
  }

  @Test
  void localZipSourceResetsToTheRequestedEntryOfAMultiEntryArchive() throws IOException {
    final File zip = writeZip("multi.zip", "first.csv", OTHER_CONTENT, "importer-vertices.csv", CSV_CONTENT);

    // The second entry is the one requested: a reset that rebuilds the stream must seek to it again, not stop at the
    // first entry of the archive.
    final Source source = new SourceDiscovery(
        "file://" + zip.getAbsolutePath() + ":::importer-vertices.csv").getSource();
    try {
      assertThat(readAfterReset(source)).isEqualTo(CSV_CONTENT);
    } finally {
      source.close();
    }
  }

  @Test
  void remoteZipSourceResetsToTheStartOfTheEntry() throws IOException {
    final byte[] zipped = Files.readAllBytes(writeZip("remote.csv.zip", "remote.csv", CSV_CONTENT).toPath());

    final HttpServer server = startHttpServer(zipped);
    try {
      final String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/remote.csv.zip";
      // allowLocalUrls=true: the loopback address is otherwise refused by the SSRF guard (#6474).
      final Source source = new SourceDiscovery(url, true).getSource();
      try {
        assertThat(readAfterReset(source)).isEqualTo(CSV_CONTENT);
      } finally {
        source.close();
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void remoteZipResetFailsLoudlyWhenTheArchiveNoLongerHoldsTheEntry() throws IOException {
    final byte[] zipped = Files.readAllBytes(writeZip("remote.zip", "wanted.csv", CSV_CONTENT).toPath());
    // The re-fetch that the reset performs gets a body that is not a zip at all, the way a remote file replaced
    // between the two requests would behave. That must surface as an error, never as an empty import.
    final HttpServer server = startHttpServer(zipped, "not a zip any more".getBytes(StandardCharsets.UTF_8));
    try {
      final String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/remote.zip:::wanted.csv";
      final Source source = new SourceDiscovery(url, true).getSource();
      try {
        assertThatThrownBy(() -> readAfterReset(source)).isInstanceOf(ImportException.class)
            .hasMessageContaining("no entry found in the zip archive");
      } finally {
        source.close();
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void localGzipSourceStillResetsToTheStartOfTheStream() throws IOException {
    final File gz = new File(tempDir, "importer-vertices.csv.gz");
    try (final OutputStream out = new GZIPOutputStream(new FileOutputStream(gz))) {
      out.write(CSV_CONTENT.getBytes(StandardCharsets.UTF_8));
    }

    final Source source = new SourceDiscovery("file://" + gz.getAbsolutePath()).getSource();
    try {
      assertThat(readAfterReset(source)).isEqualTo(CSV_CONTENT);
    } finally {
      source.close();
    }
  }

  @Test
  void localPlainSourceStillResetsToTheStartOfTheStream() throws IOException {
    final File csv = new File(tempDir, "importer-vertices.csv");
    Files.writeString(csv.toPath(), CSV_CONTENT);

    final Source source = new SourceDiscovery("file://" + csv.getAbsolutePath()).getSource();
    try {
      assertThat(readAfterReset(source)).isEqualTo(CSV_CONTENT);
    } finally {
      source.close();
    }
  }

  @Test
  void importDatabaseFromZipLoadsEveryRow() throws IOException {
    final File zip = writeZip("importer-vertices.csv.zip", "importer-vertices.csv", CSV_CONTENT);
    final String databasePath = new File(tempDir, "db-zip").getAbsolutePath();

    try (final Database db = new DatabaseFactory(databasePath).create()) {
      db.command("sql", "IMPORT DATABASE file://" + zip.getAbsolutePath());
      assertThat(db.countType("Document", true)).isEqualTo(6);
    }
  }

  @Test
  void importDatabaseFromMultiEntryZipLoadsTheRequestedEntry() throws IOException {
    final File zip = writeZip("multi.zip", "first.csv", OTHER_CONTENT, "importer-vertices.csv", CSV_CONTENT);
    final String databasePath = new File(tempDir, "db-multi-zip").getAbsolutePath();

    try (final Database db = new DatabaseFactory(databasePath).create()) {
      db.command("sql", "IMPORT DATABASE file://" + zip.getAbsolutePath() + ":::importer-vertices.csv");
      assertThat(db.countType("Document", true)).isEqualTo(6);
    }
  }

  @Test
  void resetPropagatesACallbackFailureInsteadOfSwallowingIt() {
    // Before the fix a broken reset was logged at SEVERE and swallowed, so the caller went on reading a stream that
    // was left in an unusable state and reported an empty - but successful - import.
    final Source source = new Source("test", InputStream.nullInputStream(), 0, false, s -> {
      throw new ImportException("boom");
    }, () -> null);

    assertThatThrownBy(source::reset).isInstanceOf(ImportException.class).hasMessageContaining("boom");
  }

  /**
   * Reads the whole source the way {@code Importer.loadFromSource()} does: wrap it in a {@link Parser}, reset it, then
   * consume it.
   */
  private static String readAfterReset(final Source source) throws IOException {
    final Parser parser = new Parser(source, 0);
    parser.reset();
    return new String(parser.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
  }

  private File writeZip(final String zipName, final String... entryNameAndContent) throws IOException {
    final File zip = new File(tempDir, zipName);
    try (final ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zip))) {
      for (int i = 0; i < entryNameAndContent.length; i += 2) {
        out.putNextEntry(new ZipEntry(entryNameAndContent[i]));
        out.write(entryNameAndContent[i + 1].getBytes(StandardCharsets.UTF_8));
        out.closeEntry();
      }
    }
    return zip;
  }

  /**
   * Serves {@code payloads} one per request, repeating the last one once they run out. More than one payload lets a
   * test change what the reset's re-fetch gets back.
   */
  private static HttpServer startHttpServer(final byte[]... payloads) throws IOException {
    final HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    final AtomicInteger requests = new AtomicInteger();
    server.createContext("/", exchange -> {
      final byte[] payload = payloads[Math.min(requests.getAndIncrement(), payloads.length - 1)];
      exchange.sendResponseHeaders(200, payload.length);
      try (final OutputStream out = exchange.getResponseBody()) {
        out.write(payload);
      }
    });
    server.start();
    return server;
  }
}
