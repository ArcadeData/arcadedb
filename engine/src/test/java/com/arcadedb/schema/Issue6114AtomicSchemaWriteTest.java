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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockStatic;

/** Tests the persisted schema at the replacement boundary, including an unclean child-JVM exit. */
@Timeout(180)
class Issue6114AtomicSchemaWriteTest extends TestHelper {
  @Test
  void keepsPreviousSchemaAndReopensTheCompleteUtf8Replacement() throws Exception {
    createIndexedRecord();
    final LocalSchema schema = database.getSchema().getEmbedded();
    final String before = Files.readString(schemaPath());
    final JSONObject replacement = replacement(schema, "café_日本");

    schema.update(replacement);

    assertThat(Files.readString(previousPath())).isEqualTo(before);
    assertThat(Files.readString(schemaPath())).isEqualTo(replacement.toString());
    assertThat(schema.getVersion()).isEqualTo(replacement.getLong("schemaVersion"));
    assertNoTemporaryFiles();
    reopenDatabase();
    assertIndexedRecord();
  }

  @Test
  void failedPrimaryReplacementPreservesTheCompleteSchemaAndVersion() throws Exception {
    createIndexedRecord();
    final LocalSchema schema = database.getSchema().getEmbedded();
    final String before = Files.readString(schemaPath());
    final long version = schema.getVersion();
    final Path primary = schemaPath();
    try (final MockedStatic<Files> ignored = failMoveTo(primary, new IOException("replacement failed"))) {
      assertThatThrownBy(() -> schema.update(replacement(schema, "unpublished")))
          .isInstanceOf(IOException.class).hasMessage("replacement failed");
    }

    assertThat(Files.readString(primary)).isEqualTo(before);
    assertThat(Files.readString(previousPath())).isEqualTo(before);
    assertThat(schema.getVersion()).isEqualTo(version);
    assertNoTemporaryFiles();
    reopenDatabase();
    assertIndexedRecord();
  }

  @Test
  void failedPreviousReplacementLeavesBothExistingFilesIntact() throws Exception {
    createIndexedRecord();
    final LocalSchema schema = database.getSchema().getEmbedded();
    final String before = Files.readString(schemaPath());
    final String previous = Files.readString(previousPath());
    final long version = schema.getVersion();
    try (final MockedStatic<Files> ignored = failMoveTo(previousPath(), new IOException("backup failed"))) {
      assertThatThrownBy(() -> schema.update(replacement(schema, "unpublished")))
          .isInstanceOf(IOException.class).hasMessage("backup failed");
    }

    assertThat(Files.readString(schemaPath())).isEqualTo(before);
    assertThat(Files.readString(previousPath())).isEqualTo(previous);
    assertThat(schema.getVersion()).isEqualTo(version);
    assertNoTemporaryFiles();
  }

  @Test
  void unsupportedAtomicMoveDoesNotFallBackToANonAtomicReplacement() throws Exception {
    createIndexedRecord();
    final LocalSchema schema = database.getSchema().getEmbedded();
    final Path primary = schemaPath();
    final String before = Files.readString(primary);
    final AtomicInteger attempts = new AtomicInteger();
    try (final MockedStatic<Files> ignored = mockStatic(Files.class, invocation -> {
      if (invocation.getMethod().getName().equals("move") && primary.equals(invocation.getArgument(1))) {
        attempts.incrementAndGet();
        throw new AtomicMoveNotSupportedException("temporary", primary.toString(), "test filesystem");
      }
      return invocation.callRealMethod();
    })) {
      assertThatThrownBy(() -> schema.update(replacement(schema, "unpublished")))
          .isInstanceOf(AtomicMoveNotSupportedException.class);
    }

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(Files.readString(primary)).isEqualTo(before);
    assertNoTemporaryFiles();
  }

  @Test
  void invalidVersionIsRejectedBeforeEitherFileIsReplaced() throws Exception {
    createIndexedRecord();
    final LocalSchema schema = database.getSchema().getEmbedded();
    final String before = Files.readString(schemaPath());
    final String previous = Files.readString(previousPath());
    final long version = schema.getVersion();
    final JSONObject invalid = replacement(schema, "invalid").put("schemaVersion", "not-a-number");
    assertThatThrownBy(() -> schema.update(invalid)).isInstanceOf(JSONException.class);
    assertThat(Files.readString(schemaPath())).isEqualTo(before);
    assertThat(Files.readString(previousPath())).isEqualTo(previous);
    assertThat(schema.getVersion()).isEqualTo(version);
    assertNoTemporaryFiles();
  }

  @Test
  void concurrentFileReadersOnlyObserveCompleteSchemas() throws Exception {
    final LocalSchema schema = database.getSchema().getEmbedded();
    final String first = replacement(schema, "a".repeat(64 * 1024)).toString();
    final String second = replacement(schema, "b".repeat(64 * 1024)).toString();
    schema.update(new JSONObject(first));
    final Path primary = schemaPath();
    final AtomicBoolean finished = new AtomicBoolean();
    final CountDownLatch reading = new CountDownLatch(1);
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var reader = executor.submit(() -> {
        int reads = 0;
        do {
          final String observed = Files.readString(primary);
          assertThat(observed.equals(first) || observed.equals(second))
              .as("reader must see a complete schema generation, observed %d characters", observed.length()).isTrue();
          ++reads;
          reading.countDown();
        } while (!finished.get());
        return reads;
      });
      try {
        assertThat(reading.await(30, TimeUnit.SECONDS)).isTrue();
        for (int i = 0; i < 32; ++i)
          schema.update(new JSONObject(i % 2 == 0 ? second : first));
      } finally {
        finished.set(true);
      }
      assertThat(reader.get(30, TimeUnit.SECONDS)).isGreaterThan(0);
    }
    assertNoTemporaryFiles();
  }

  @Test
  @Tag("slow")
  void abruptProcessExitBeforePublicationLeavesAReopenableSchema() throws Exception {
    createIndexedRecord();
    final Path primary = schemaPath();
    final Path log = primary.getParent().resolve("schema-write-child.log");
    database.close();
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    final Process process = new ProcessBuilder(java, "-XX:+EnableDynamicAgentLoading", "-cp", classpath,
        InterruptedWriter.class.getName(), primary.getParent().toString())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertThat(process.waitFor(120, TimeUnit.SECONDS)).as("disposable schema writer must terminate").isTrue();
      assertThat(process.exitValue()).as("child output: %s", Files.readString(log)).isEqualTo(73);
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(30, TimeUnit.SECONDS);
      }
    }

    // Opening the child can legitimately save a fresh schema generation before the attempted update.
    final String before = Files.readString(primary.resolveSibling("schema-before-interruption.json"));
    assertThat(Files.readString(primary)).isEqualTo(before);
    assertThat(Files.readString(previousPath())).isEqualTo(before);
    try (final var files = Files.list(primary.getParent())) {
      final var temporary = files.filter(path -> path.getFileName().toString().startsWith("schema.json.")
          && path.getFileName().toString().endsWith(".tmp")).toList();
      assertThat(temporary).hasSize(1);
      assertThat(new JSONObject(Files.readString(temporary.getFirst())).getString("testMarker"))
          .isEqualTo("not-published");
    }
    database = factory.open();
    assertIndexedRecord();
  }

  public static class InterruptedWriter {
    public static void main(final String[] args) throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(args[0]); final var database = factory.open()) {
        final LocalSchema schema = database.getSchema().getEmbedded();
        final Path primary = schema.getConfigurationFile().toPath().toAbsolutePath();
        Files.writeString(primary.resolveSibling("schema-before-interruption.json"), Files.readString(primary));
        try (final MockedStatic<Files> ignored = mockStatic(Files.class, invocation -> {
          if (invocation.getMethod().getName().equals("move") && primary.equals(invocation.getArgument(1)))
            // The new file has been written and synced, but publication and normal cleanup never run.
            Runtime.getRuntime().halt(73);
          return invocation.callRealMethod();
        })) {
          schema.update(replacement(schema, "not-published"));
        }
      }
    }
  }

  private void createIndexedRecord() {
    database.getSchema().createDocumentType("Evidence", 1).createProperty("name", Type.STRING)
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    database.transaction(() -> database.newDocument("Evidence").set("name", "café_日本").save());
  }

  private void assertIndexedRecord() {
    assertThat(database.getSchema().getType("Evidence").getIndexByProperties("name")).isNotNull();
    try (final var cursor = database.lookupByKey("Evidence", "name", "café_日本")) {
      assertThat(cursor.hasNext()).isTrue();
      assertThat(cursor.next().asDocument().getString("name")).isEqualTo("café_日本");
      assertThat(cursor.hasNext()).isFalse();
    }
  }

  private static JSONObject replacement(final LocalSchema schema, final String marker) {
    return schema.toJSON().put("schemaVersion", schema.getVersion() + 1).put("testMarker", marker);
  }

  private Path schemaPath() {
    return Path.of(factory.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME).toAbsolutePath();
  }

  private Path previousPath() {
    return schemaPath().resolveSibling(LocalSchema.SCHEMA_PREV_FILE_NAME);
  }

  private static MockedStatic<Files> failMoveTo(final Path target, final IOException failure) {
    return mockStatic(Files.class, invocation -> {
      if (invocation.getMethod().getName().equals("move") && target.equals(invocation.getArgument(1)))
        throw failure;
      return invocation.callRealMethod();
    });
  }

  private void assertNoTemporaryFiles() throws IOException {
    try (final var files = Files.list(schemaPath().getParent())) {
      assertThat(files.filter(path -> path.getFileName().toString().startsWith("schema.")
          && path.getFileName().toString().endsWith(".tmp"))).isEmpty();
    }
  }
}
