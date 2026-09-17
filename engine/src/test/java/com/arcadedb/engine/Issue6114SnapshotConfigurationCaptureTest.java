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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6114, second half: a {@link PageSnapshot} window carries the two configuration files as they stood at its
 * t0, so a consumer no longer has to hold the database read lock for its whole operation to keep them in step with
 * the page files it is serving.
 * <p>
 * Every assertion that the window did not move is paired with one that the LIVE files DID, so none of these can
 * pass by simply not changing anything.
 */
class Issue6114SnapshotConfigurationCaptureTest extends TestHelper {

  @Test
  void theWindowCarriesBothConfigurationFilesAsOfT0() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalSchema schema = (LocalSchema) database.getSchema();

    schema.createDocumentType("AtT0");

    final String liveSchemaAtT0 = Files.readString(schema.getConfigurationFile().toPath(), StandardCharsets.UTF_8);
    final String previousSchemaAtT0 = Files.readString(
        new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME).toPath(), StandardCharsets.UTF_8);

    try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
      final List<PageSnapshot.SnapshotConfigFile> captured = snapshot.getConfigurationFiles();

      assertThat(captured).extracting(PageSnapshot.SnapshotConfigFile::fileName)
          .as("configuration.json first, then schema.json, then schema.prev.json - the order a consumer archives "
              + "them in. The previous copy joined the window in #7637, so a restored database keeps the "
              + "corruption fallback its source had")
          .containsExactly(LocalDatabase.CONFIGURATION_FILE_NAME, LocalSchema.SCHEMA_FILE_NAME,
              LocalSchema.SCHEMA_PREV_FILE_NAME);

      assertThat(schemaOf(captured)).as("the window must serve the schema exactly as it was at t0")
          .isEqualTo(liveSchemaAtT0);

      // NOW MOVE THE LIVE SCHEMA ON, WHICH IS THE DDL THE READ LOCK USED TO EXCLUDE
      schema.createDocumentType("AfterT0");

      assertThat(Files.readString(schema.getConfigurationFile().toPath(), StandardCharsets.UTF_8))
          .as("the DDL must really have rewritten the live file").isNotEqualTo(liveSchemaAtT0);

      assertThat(schemaOf(snapshot.getConfigurationFiles()))
          .as("the window keeps serving t0 regardless of what the live schema does afterwards")
          .isEqualTo(liveSchemaAtT0);

      final JSONObject capturedTypes = new JSONObject(schemaOf(snapshot.getConfigurationFiles())).getJSONObject("types");
      assertThat(capturedTypes.keySet()).contains("AtT0");
      assertThat(capturedTypes.keySet())
          .as("a type created after t0 has no pages in this window, so it must not be in its schema either")
          .doesNotContain("AfterT0");

      // AND THE PREVIOUS COPY IS PINNED AT t0 TOO, not merely present in the captured set (issue #7637). This is
      // where that claim is decidable: the DDL above ran while this window was open, so the live schema.prev.json
      // now holds the generation the window is serving as its PRIMARY. A window that re-read the file instead of
      // capturing it would serve that newer copy - a fallback describing a page set the archive does not contain,
      // which is the whole reason the capture is inside the barrier rather than after it.
      final String capturedPrevious = new String(
          configurationFile(snapshot, LocalSchema.SCHEMA_PREV_FILE_NAME).content(), StandardCharsets.UTF_8);
      assertThat(capturedPrevious).isEqualTo(previousSchemaAtT0);
      assertThat(Files.readString(new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME).toPath(),
          StandardCharsets.UTF_8))
          .as("the DDL must really have moved the live previous copy on, or this proves nothing")
          .isNotEqualTo(capturedPrevious);
    }
  }

  /**
   * The bytes the consumer streams are the captured ones, not a re-read of the file: the stream is what the backup
   * and the HA snapshot ship actually write into their archive.
   */
  @Test
  void theCapturedStreamServesTheT0BytesAfterTheFileHasMovedOn() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalSchema schema = (LocalSchema) database.getSchema();

    try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
      final PageSnapshot.SnapshotConfigFile capturedSchema = configurationFile(snapshot, LocalSchema.SCHEMA_FILE_NAME);

      schema.createDocumentType("WrittenAfterTheWindowOpened");

      final String streamed;
      try (final InputStream in = capturedSchema.newInputStream()) {
        streamed = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }

      assertThat(streamed).isEqualTo(new String(capturedSchema.content(), StandardCharsets.UTF_8));
      assertThat(capturedSchema.size()).isEqualTo(capturedSchema.content().length);
      assertThat(new JSONObject(streamed).getJSONObject("types").keySet())
          .doesNotContain("WrittenAfterTheWindowOpened");
    }
  }

  /**
   * Overlapping windows each hold their own point in time for the configuration too, exactly as they do for the
   * pages (challenge C3 of #6075).
   */
  @Test
  void overlappingWindowsEachKeepTheirOwnConfiguration() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalSchema schema = (LocalSchema) database.getSchema();

    schema.createDocumentType("First");

    try (final PageSnapshot older = db.getPageManager().openSnapshot(db)) {
      schema.createDocumentType("Second");

      try (final PageSnapshot newer = db.getPageManager().openSnapshot(db)) {
        assertThat(new JSONObject(schemaOf(older.getConfigurationFiles())).getJSONObject("types").keySet())
            .contains("First").doesNotContain("Second");
        assertThat(new JSONObject(schemaOf(newer.getConfigurationFiles())).getJSONObject("types").keySet())
            .contains("First", "Second");
      }
    }
  }

  /**
   * A configuration file that does not exist at t0 is simply absent, never an empty entry a restore would extract.
   * Both optional files are removed here, not just {@code configuration.json}: {@code schema.prev.json} is legitimately
   * missing on a database whose schema has never been re-saved, and joining the captured set in #7637 must not have
   * turned that into a zero-length entry either.
   */
  @Test
  void aMissingConfigurationFileIsOmittedRatherThanCapturedEmpty() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalDatabase local = (LocalDatabase) db.getEmbedded();

    FileUtils.deleteFile(local.getConfigurationFile());
    assertThat(local.getConfigurationFile()).doesNotExist();

    final File previousSchema = new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME);
    FileUtils.deleteFile(previousSchema);
    assertThat(previousSchema).doesNotExist();

    try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
      assertThat(snapshot.getConfigurationFiles()).extracting(PageSnapshot.SnapshotConfigFile::fileName)
          .containsExactly(LocalSchema.SCHEMA_FILE_NAME);
    }
  }

  private static String schemaOf(final List<PageSnapshot.SnapshotConfigFile> files) {
    return new String(configurationFile(files, LocalSchema.SCHEMA_FILE_NAME).content(), StandardCharsets.UTF_8);
  }

  private static PageSnapshot.SnapshotConfigFile configurationFile(final PageSnapshot snapshot, final String name) {
    return configurationFile(snapshot.getConfigurationFiles(), name);
  }

  private static PageSnapshot.SnapshotConfigFile configurationFile(
      final List<PageSnapshot.SnapshotConfigFile> files, final String name) {
    return files.stream().filter(f -> f.fileName().equals(name)).findFirst()
        .orElseThrow(() -> new AssertionError("the window carries no '" + name + "'"));
  }
}
