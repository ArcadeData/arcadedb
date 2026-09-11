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
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mockStatic;

/**
 * Tests the persisted schema at the replacement boundary, including an unclean child-JVM exit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Timeout(300) // Separate wall-clock watchdog for a hung test; completion budgets below discount JVM stalls.
class Issue6114AtomicSchemaWriteTest extends TestHelper {
  @Test
  void preservesUtf8PreviousSchemaDespiteANonUtf8EncodingSetting() throws Exception {
    assertPreviousSchemaBytesAndRecovery(StandardCharsets.UTF_8);
  }

  @Test
  void preservesNonUtf8PreviousSchemaAndRecoversNonAsciiPropertyNames() throws Exception {
    assertPreviousSchemaBytesAndRecovery(StandardCharsets.ISO_8859_1);
  }

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
  void unsupportedAtomicMoveStillPublishesTheSchemaThroughTheFallback() throws Exception {
    // A file store that cannot rename atomically must not be turned into a store where the schema stops
    // persisting: saveConfiguration()'s only error handling is a logged SEVERE, so a hard failure here would
    // lose every DDL silently. The replacement is merely non-atomic, which is still no worse than the
    // truncate-and-rewrite this change replaced.
    createIndexedRecord();
    final LocalSchema schema = database.getSchema().getEmbedded();
    final Path primary = schemaPath();
    final AtomicInteger attempts = new AtomicInteger();
    final JSONObject replacement = replacement(schema, "published-through-the-fallback");
    try (final MockedStatic<Files> ignored = mockStatic(Files.class, invocation -> {
      // Only the first move onto the primary is the ATOMIC_MOVE one; the retry must be allowed through.
      if (invocation.getMethod().getName().equals("move") && primary.equals(invocation.getArgument(1))
          && attempts.incrementAndGet() == 1)
        throw new AtomicMoveNotSupportedException("temporary", primary.toString(), "test filesystem");
      return invocation.callRealMethod();
    })) {
      schema.update(replacement);
    }

    assertThat(attempts.get()).as("the atomic move must be retried exactly once, without an atomic guarantee")
        .isEqualTo(2);
    assertThat(Files.readString(primary)).isEqualTo(replacement.toString());
    assertNoTemporaryFiles();
    reopenDatabase();
    assertIndexedRecord();
  }

  @Test
  void theSavedPreviousGenerationDoesNotRereadTheWholeSchema() throws Exception {
    // The previous generation is published as a hard link where the file store supports one, so a schema save
    // costs an inode operation instead of a full read + write + fsync of a file that can reach megabytes on a
    // large schema. Reading the primary back would silently reintroduce that cost on every DDL statement.
    createIndexedRecord();
    final LocalSchema schema = database.getSchema().getEmbedded();
    final Path primary = schemaPath();
    assumeTrue(supportsHardLinks(primary.getParent()), "the file store hosting the test databases has no hard links");
    final byte[] before = Files.readAllBytes(primary);
    final AtomicInteger reads = new AtomicInteger();
    final AtomicInteger links = new AtomicInteger();
    try (final MockedStatic<Files> ignored = mockStatic(Files.class, invocation -> {
      final String method = invocation.getMethod().getName();
      if ((method.equals("readAllBytes") || method.equals("readString") || method.equals("copy"))
          && primary.equals(invocation.getArgument(0)))
        reads.incrementAndGet();
      else if (method.equals("createLink"))
        links.incrementAndGet();
      return invocation.callRealMethod();
    })) {
      schema.update(replacement(schema, "linked"));
    }

    assertThat(links.get()).as("the previous generation must be published by link, not by copy").isEqualTo(1);
    assertThat(reads.get()).as("the primary schema must not be read back to save the previous generation").isZero();
    assertThat(Files.readAllBytes(previousPath())).isEqualTo(before);
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
  void explicitNullVersionIsRejectedBeforeEitherFileIsReplaced() throws Exception {
    createIndexedRecord();
    final LocalSchema schema = database.getSchema().getEmbedded();
    final byte[] before = Files.readAllBytes(schemaPath());
    final byte[] previous = Files.readAllBytes(previousPath());
    final long version = schema.getVersion();
    final JSONObject invalid = replacement(schema, "invalid").put("schemaVersion", JSONObject.NULL);
    assertThatThrownBy(() -> schema.update(invalid)).isInstanceOf(JSONException.class);
    assertThat(Files.readAllBytes(schemaPath())).isEqualTo(before);
    assertThat(Files.readAllBytes(previousPath())).isEqualTo(previous);
    assertThat(schema.getVersion()).isEqualTo(version);
    assertNoTemporaryFiles();
  }

  @Test
  void absentVersionKeepsTheCurrentVersion() throws Exception {
    final LocalSchema schema = database.getSchema().getEmbedded();
    final long version = schema.getVersion();
    final JSONObject replacement = replacement(schema, "no-version");
    replacement.remove("schemaVersion");
    schema.update(replacement);
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
        try {
          do {
            final String observed = Files.readString(primary);
            assertThat(observed.equals(first) || observed.equals(second))
                .as("reader must see a complete schema generation, observed %d characters", observed.length()).isTrue();
            ++reads;
            reading.countDown();
          } while (!finished.get());
        } finally {
          // Release the readiness wait even when the first read already failed, so the failure below is the
          // assertion the reader raised and not a 30-second "hung reader" that hides it.
          reading.countDown();
        }
        return reads;
      });
      try {
        awaitCompletion(() -> reading.getCount() == 0, 30_000, "a reader starting versus a hung reader");
        for (int i = 0; i < 32; ++i)
          schema.update(new JSONObject(i % 2 == 0 ? second : first));
      } finally {
        finished.set(true);
      }
      awaitCompletion(reader::isDone, 30_000, "a reader finishing versus a hung reader");
      assertThat(reader.get()).isGreaterThan(0);
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
    // Attach Byte Buddy up front the way surefire does for this build, instead of leaning on self-attach:
    // -XX:+EnableDynamicAgentLoading is a deprecated escape hatch that later JDKs refuse by default.
    final List<String> command = new ArrayList<>(List.of(java));
    final String agent = byteBuddyAgentJar(classpath);
    command.add(agent != null ? "-javaagent:" + agent : "-XX:+EnableDynamicAgentLoading");
    command.addAll(List.of("-cp", classpath, InterruptedWriter.class.getName(), primary.getParent().toString()));
    final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      awaitCompletion(() -> !process.isAlive(), 120_000, "a disposable writer terminating versus a hung writer");
      assertThat(process.exitValue()).as("child output: %s", Files.readString(log)).isEqualTo(73);
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        // Cleanup watchdog only, not an assertion about operation latency.
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

  private void assertPreviousSchemaBytesAndRecovery(final Charset persistedCharset) throws Exception {
    createIndexedRecord();
    final LocalSchema schema = database.getSchema().getEmbedded();
    schema.getType("Evidence").createProperty("café", Type.STRING);
    final String originalEncoding = schema.getEncoding();
    // Model both a UTF-8 primary with a changed reader setting and a legacy non-UTF-8 primary.
    Files.writeString(schemaPath(), schema.toJSON().toString(), persistedCharset);
    final byte[] before = Files.readAllBytes(schemaPath());
    schema.setEncoding(StandardCharsets.ISO_8859_1.name());
    try {
      schema.update(replacement(schema, "replacement"));
      assertThat(Files.readAllBytes(previousPath())).isEqualTo(before);
      assertNoTemporaryFiles();

      // Exercise the configured recovery reader before closing, which would rotate the backup again.
      Files.writeString(schemaPath(), "{\"schemaVersion\":");
      schema.setEncoding(persistedCharset.name());
      schema.readConfiguration();
      assertThat(schema.getType("Evidence").existsProperty("café")).isTrue();
      assertIndexedRecord();
    } finally {
      schema.setEncoding(originalEncoding);
    }
    // Recovery self-heals the primary; the normal UTF-8 reopen must retain the same names and index.
    reopenDatabase();
    assertThat(database.getSchema().getType("Evidence").existsProperty("café")).isTrue();
    assertIndexedRecord();
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

  /** Waits against an effective-time budget so JVM-wide stalls cannot exhaust a readiness/completion wait. */
  private static void awaitCompletion(final BooleanSupplier completed, final long boundMs, final String whatItSeparates)
      throws InterruptedException {
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    while (!completed.getAsBoolean()) {
      stopwatch.assertGaveUpWithin(boundMs, whatItSeparates);
      Thread.sleep(10);
    }
  }

  /** Probes the file store so the link assertion above is skipped, not failed, on FAT/exFAT and the like. */
  private static boolean supportsHardLinks(final Path dir) throws IOException {
    final Path source = Files.createTempFile(dir, "link-probe.", ".tmp");
    final Path link = dir.resolve("link-probe." + UUID.randomUUID() + ".tmp");
    try {
      Files.createLink(link, source);
      return true;
    } catch (final UnsupportedOperationException | FileSystemException e) {
      return false;
    } finally {
      Files.deleteIfExists(link);
      Files.deleteIfExists(source);
    }
  }

  /** Returns the byte-buddy-agent jar on {@code classpath}, or null when the build does not ship one. */
  private static String byteBuddyAgentJar(final String classpath) {
    for (final String entry : classpath.split(File.pathSeparator))
      if (entry.contains("byte-buddy-agent"))
        return entry;
    return null;
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
