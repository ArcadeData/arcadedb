/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The follower extracts the snapshot archive of a database whose type, bucket or index names legally contain
 * {@code ..} inside a name (the engine only refuses a name that IS {@code .} or {@code ..}), and still refuses an entry
 * name that is not a plain file name.
 * <p>
 * The archive is written by the leader's own writers from a real database, so the entry names are the ones the engine
 * really gives to the component files; the entries that must be refused are hand-built.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class SnapshotExtractionEntryNamesTest {

  private static final String DATABASE_PATH = "target/databases/snapshot-extraction-entry-names";
  private static final String TYPE_NAME     = "a..b";

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void archiveOfDatabaseWithDotsInsideNamesIsExtracted(@TempDir final Path targetDir) throws Exception {
    final byte[] zip;
    final Map<String, byte[]> shipped;

    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      final VertexType type = database.getSchema().createVertexType(TYPE_NAME);
      type.createProperty("id", Integer.class).createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
      database.getSchema().createEdgeType("x..y");
      database.transaction(() -> {
        for (int i = 0; i < 200; i++)
          database.newVertex(TYPE_NAME).set("id", i).set("payload", "x".repeat(100)).save();
      });
      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);

      shipped = new LinkedHashMap<>();
      zip = buildLeaderArchive((DatabaseInternal) database, shipped);
    }

    assertThat(shipped.keySet()).as("the fixture: the leader ships bucket and index files named after the type")
        .anyMatch(name -> name.startsWith(TYPE_NAME + "_") && name.endsWith(".bucket"))
        .anyMatch(name -> name.startsWith(TYPE_NAME + "_") && name.endsWith(".umtidx"))
        .anyMatch(name -> name.startsWith("x..y_") && name.endsWith(".bucket"));

    extract(zip, targetDir);

    for (final Map.Entry<String, byte[]> entry : shipped.entrySet())
      assertThat(Files.readAllBytes(targetDir.resolve(entry.getKey()))).as("file '%s'", entry.getKey())
          .isEqualTo(entry.getValue());
  }

  @Test
  void entryNameWithDotsInsideIsExtracted(@TempDir final Path targetDir) throws Exception {
    final Map<String, byte[]> files = new LinkedHashMap<>();
    files.put("a..b_0.1.65536.v0.bucket", "bucket".getBytes(StandardCharsets.UTF_8));
    files.put("..hidden_0.2.65536.v0.bucket", "leading".getBytes(StandardCharsets.UTF_8));
    files.put("trailing..", "trailing".getBytes(StandardCharsets.UTF_8));
    files.put("a...b", "three".getBytes(StandardCharsets.UTF_8));

    extract(buildZip(files, true), targetDir);

    for (final Map.Entry<String, byte[]> entry : files.entrySet())
      assertThat(Files.readAllBytes(targetDir.resolve(entry.getKey()))).as("file '%s'", entry.getKey())
          .isEqualTo(entry.getValue());
  }

  @ParameterizedTest
  @ValueSource(strings = { "../outside.txt", "sub/../outside.txt", "sub/../../outside.txt", "..\\outside.txt", "sub\\..\\outside.txt",
      "sub/file.bin", "/absolute.bin", "sub/", "..", "." })
  void entryNameThatIsNotAPlainFileNameIsRefused(final String entryName, @TempDir final Path tempDir) throws Exception {
    final Path targetDir = Files.createDirectories(tempDir.resolve("staging"));
    final Map<String, byte[]> files = new LinkedHashMap<>();
    files.put(entryName, "content".getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> extract(buildZip(files, true), targetDir)).isInstanceOf(ReplicationException.class);

    assertThat(tempDir.resolve("outside.txt")).doesNotExist();
    assertThat(tempDir.resolve("absolute.bin")).doesNotExist();
    try (final Stream<Path> staged = Files.list(targetDir)) {
      assertThat(staged.toList()).as("nothing is left in the staging directory").isEmpty();
    }
  }

  @Test
  void refusedEntryDoesNotStopAtTheFirstValidOnes(@TempDir final Path tempDir) throws Exception {
    final Path targetDir = Files.createDirectories(tempDir.resolve("staging"));
    final Map<String, byte[]> files = new LinkedHashMap<>();
    files.put("a..b_0.1.65536.v0.bucket", "ok".getBytes(StandardCharsets.UTF_8));
    files.put("sub/../outside.txt", "no".getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> extract(buildZip(files, true), targetDir)).isInstanceOf(ReplicationException.class)
        .hasMessageContaining("sub/../outside.txt");

    assertThat(tempDir.resolve("outside.txt")).doesNotExist();
  }

  // ------------------------------------------------------------------------------------------------- HELPERS

  /** The archive the leader writes: configuration, page files, then the manifest as the last entry. */
  private static byte[] buildLeaderArchive(final DatabaseInternal db, final Map<String, byte[]> shipped) throws Exception {
    final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db); final ZipOutputStream zipOut = new ZipOutputStream(bytes)) {
      SnapshotHttpHandler.addConfigurationToZip(zipOut, db, snapshot, manifest);
      SnapshotHttpHandler.addPageFilesToZip(zipOut, db, snapshot, manifest);
      zipOut.putNextEntry(new ZipEntry(SnapshotManager.MANIFEST_ENTRY_NAME));
      zipOut.write(SnapshotManager.buildManifest(manifest).getBytes(StandardCharsets.UTF_8));
      zipOut.closeEntry();
      zipOut.finish();
    }
    final byte[] zip = bytes.toByteArray();
    try (final ZipInputStream zipIn = new ZipInputStream(new ByteArrayInputStream(zip))) {
      for (ZipEntry entry = zipIn.getNextEntry(); entry != null; entry = zipIn.getNextEntry()) {
        final byte[] content = zipIn.readAllBytes();
        if (!SnapshotManager.MANIFEST_ENTRY_NAME.equals(entry.getName()))
          shipped.put(entry.getName(), content);
      }
    }
    return zip;
  }

  /** A snapshot archive with one entry per file and, last, the manifest that names them. */
  private static byte[] buildZip(final Map<String, byte[]> files, final boolean includeManifest) throws Exception {
    final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final ZipOutputStream zipOut = new ZipOutputStream(bytes)) {
      for (final Map.Entry<String, byte[]> file : files.entrySet()) {
        zipOut.putNextEntry(new ZipEntry(file.getKey()));
        zipOut.write(file.getValue());
        zipOut.closeEntry();
        final CRC32 crc = new CRC32();
        crc.update(file.getValue());
        manifest.add(new SnapshotManager.ManifestEntry(file.getKey(), file.getValue().length, crc.getValue()));
      }
      if (includeManifest) {
        zipOut.putNextEntry(new ZipEntry(SnapshotManager.MANIFEST_ENTRY_NAME));
        zipOut.write(SnapshotManager.buildManifest(manifest).getBytes(StandardCharsets.UTF_8));
        zipOut.closeEntry();
      }
      zipOut.finish();
    }
    return bytes.toByteArray();
  }

  private static void extract(final byte[] zip, final Path targetDir) throws Exception {
    final SnapshotInstaller.CountingInputStream counter = new SnapshotInstaller.CountingInputStream(new ByteArrayInputStream(zip));
    SnapshotInstaller.extractAndVerifySnapshot(counter, counter, targetDir, true, null);
  }
}
