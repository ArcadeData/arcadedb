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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

/**
 * Issue #7604: {@code LocalSchema.update()} publishes the in-memory schema version only after the bytes are on disk,
 * but its two internal callers - {@code saveConfiguration()} and {@code close()} - advanced {@code versionSerial}
 * BEFORE calling it. A failed write therefore left the in-memory version a generation ahead of every file.
 */
class Issue7604SchemaVersionPublishedAfterSaveTest extends TestHelper {

  @Test
  void failedSaveConfigurationDoesNotAdvanceTheVersion() throws Exception {
    final LocalSchema schema = database.getSchema().getEmbedded();
    final long version = schema.getVersion();
    assertThat(fileVersion(schemaPath())).isEqualTo(version);

    final AtomicInteger injected = new AtomicInteger();
    try (final MockedStatic<Files> ignored = failMoveTo(schemaPath(), injected)) {
      // saveConfiguration() logs the IOException and returns: the only observable outcome is the version
      schema.saveConfiguration();
    }
    assertThat(injected.get()).isEqualTo(1);

    assertThat(schema.getVersion()).isEqualTo(version);
    assertThat(fileVersion(schemaPath())).isEqualTo(version);

    // The next successful save publishes exactly the next generation, not one skipped by the failed attempt
    schema.saveConfiguration();
    assertThat(schema.getVersion()).isEqualTo(version + 1);
    assertThat(fileVersion(schemaPath())).isEqualTo(version + 1);
  }

  @Test
  void failedSaveOfAPendingChangeKeepsTheSchemaDirtyAndTheVersionOnDisk() throws Exception {
    final LocalSchema schema = database.getSchema().getEmbedded();
    final long version = schema.getVersion();

    database.begin();
    schema.createDocumentType("Issue7604Pending");
    assertThat(schema.isDirty()).isTrue();

    final AtomicInteger injected = new AtomicInteger();
    try (final MockedStatic<Files> ignored = failMoveTo(schemaPath(), injected)) {
      database.commit();
    }
    assertThat(injected.get()).isPositive();

    assertThat(schema.isDirty()).isTrue();
    assertThat(schema.getVersion()).isEqualTo(fileVersion(schemaPath())).isEqualTo(version);

    schema.saveConfiguration();
    assertThat(schema.isDirty()).isFalse();
    assertThat(schema.getVersion()).isEqualTo(fileVersion(schemaPath())).isEqualTo(version + 1);
  }

  @Test
  void failedSaveOnCloseDoesNotAdvanceTheVersion() throws Exception {
    final String path = getDatabasePath() + "_7604close";
    FileUtils.deleteRecursively(new File(path));
    try (final DatabaseFactory closeFactory = new DatabaseFactory(path)) {
      final Database db = closeFactory.create();
      final LocalSchema schema = db.getSchema().getEmbedded();
      final Path primary = Path.of(path, LocalSchema.SCHEMA_FILE_NAME).toAbsolutePath();
      final long version = schema.getVersion();

      // A DDL inside an open transaction postpones its save, so the schema is still dirty when close() runs
      db.begin();
      schema.createDocumentType("Issue7604OnClose");
      assertThat(schema.isDirty()).isTrue();

      final AtomicInteger injected = new AtomicInteger();
      try (final MockedStatic<Files> ignored = failMoveTo(primary, injected)) {
        db.close();
      }
      assertThat(injected.get()).isPositive();

      assertThat(schema.getVersion()).isEqualTo(version);
      assertThat(fileVersion(primary)).isEqualTo(version);
    } finally {
      FileUtils.deleteRecursively(new File(path));
    }
  }

  private Path schemaPath() {
    return Path.of(factory.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME).toAbsolutePath();
  }

  private static long fileVersion(final Path file) throws IOException {
    return new JSONObject(Files.readString(file)).getLong("schemaVersion");
  }

  /**
   * Fails every rename onto {@code target} on the calling thread, counting each one: the tests assert the counter so a
   * save moved off this thread, or a write that stops going through {@code Files.move}, cannot pass without the failure
   * having been injected.
   */
  private static MockedStatic<Files> failMoveTo(final Path target, final AtomicInteger injected) {
    return mockStatic(Files.class, invocation -> {
      if (invocation.getMethod().getName().equals("move") && target.equals(invocation.getArgument(1))) {
        injected.incrementAndGet();
        throw new IOException("simulated schema write failure");
      }
      return invocation.callRealMethod();
    });
  }
}
