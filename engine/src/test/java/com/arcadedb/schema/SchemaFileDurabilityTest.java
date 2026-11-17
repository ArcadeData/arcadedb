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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.NodeLocalFiles;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * Regression tests for the persistence of the schema files, one nested class per fixed issue.
 */
class SchemaFileDurabilityTest {
  /**
   * Regression for #6114: the persisted schema at the replacement boundary, including an unclean child-JVM exit.
   * <p>
   * Tests the persisted schema at the replacement boundary, including an unclean child-JVM exit.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Timeout(300) // Separate wall-clock watchdog for a hung test; completion budgets below discount JVM stalls.
  class AtomicSchemaWrite extends TestHelper {
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
    void thePreviousGenerationIsPublishedAsACopyNeverALink() throws Exception {
      // Issue #11097 reversed the earlier design (a hard link, to avoid re-reading the schema): a file system can accept
      // a link and still lose the data under the second name once the first is replaced, which left the recovery copy
      // unreadable. The previous generation is now always a byte copy.
      createIndexedRecord();
      final LocalSchema schema = database.getSchema().getEmbedded();
      final Path primary = schemaPath();
      final byte[] before = Files.readAllBytes(primary);
      final AtomicInteger links = new AtomicInteger();
      try (final MockedStatic<Files> ignored = mockStatic(Files.class, invocation -> {
        if (invocation.getMethod().getName().equals("createLink"))
          links.incrementAndGet();
        return invocation.callRealMethod();
      })) {
        schema.update(replacement(schema, "copied"));
      }

      assertThat(links.get()).as("the previous generation must never be published by link").isZero();
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

  /**
   * Regression for #11097: {@code schema.prev.json} was always published as a hard link to {@code schema.json}. A file system
   * that accepts the link but loses the data once the original name is replaced (SeaweedFS FUSE) left the fallback copy
   * unreadable. It is now always a byte copy.
   */
  @Nested
  class SchemaPrevWithoutHardLink extends TestHelper {

    @Test
    void schemaSaveNeverLinksThePreviousGeneration() throws Exception {
      assertThat(linkAttemptsDuringSchemaChange("NoLink")).isZero();

      final Path dir = Path.of(database.getDatabasePath());
      final Path schema = dir.resolve("schema.json");
      final Path prev = dir.resolve("schema.prev.json");
      assertThat(Files.isSameFile(schema, prev)).isFalse();

      // Second change: schema.prev.json must hold the complete previous generation, readable on its own.
      final String generationBefore = Files.readString(schema);
      assertThat(linkAttemptsDuringSchemaChange("NoLink2")).isZero();
      assertThat(Files.isSameFile(schema, prev)).isFalse();
      assertThat(Files.readString(prev)).isEqualTo(generationBefore);
    }

    private int linkAttemptsDuringSchemaChange(final String typeName) {
      final AtomicInteger attempts = new AtomicInteger();
      try (final MockedStatic<Files> ignored = mockStatic(Files.class, invocation -> {
        if (invocation.getMethod().getName().equals("createLink"))
          attempts.incrementAndGet();
        return invocation.callRealMethod();
      })) {
        database.getSchema().createDocumentType(typeName);
      }
      return attempts.get();
    }
  }

  /**
   * Regression for #11240: a schema save that fails leaves the schema dirty, so the next transaction end or the close writes it.
   * <p>
   * A schema save that fails leaves the schema dirty, so the next transaction end or the close writes it (issue #11240): a
   * compaction swap records its migrated file without marking the schema dirty and saves it at once, and when that one save
   * failed nothing ever wrote the swap, so a reopen read the old file as the index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FailedSchemaSaveStaysDirty extends TestHelper {
    @Test
    void aFailedSaveIsRetriedOnceTheFileCanBeWritten() throws Exception {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      final File directory = new File(database.getDatabasePath());

      assertThat(directory.setWritable(false)).isTrue();
      final boolean enforced = !Files.isWritable(directory.toPath());
      if (!enforced)
        directory.setWritable(true);
      assumeTrue(enforced, "directory permissions are not enforced for this user");

      try {
        // as the compaction swap does: recorded without marking the schema dirty, then saved at once
        schema.setMigratedFileId(1_000, 1_001, false);
        assertThatThrownBy(schema::saveConfigurationOrThrow).isInstanceOf(IOException.class);
        assertThat(schema.isDirty()).as("a failed save leaves the schema to be written again").isTrue();
      } finally {
        assertThat(directory.setWritable(true)).isTrue();
      }

      schema.saveConfigurationOrThrow();
      assertThat(schema.isDirty()).isFalse();

      reopenDatabase();
      assertThat(((LocalSchema) database.getSchema().getEmbedded()).getMigratedFileId(1_000)).isEqualTo(1_001);
    }
  }

  /**
   * Regression for #11514: every schema write fsync'd two files, the copy of the current generation published as
   * {@code schema.prev.json} and the new {@code schema.json}, so each DDL statement issued alone paid two file flushes plus
   * the directory flush. The recovery copy is only read back when {@code schema.json} is missing, empty or unreadable, and
   * {@code schema.json} is published by an fsync'd temporary file, an atomic rename and a directory fsync, so it is never
   * torn by a crash. The copy's flush is therefore deferred: once at close (a clean close leaves it durable for the next
   * open) and, after a crash, at recovery, where a copy whose bytes did not reach the disk is rebuilt from the primary.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DeferredRecoveryCopySync extends TestHelper {
    private final List<Path> synced = new CopyOnWriteArrayList<>();

    @Override
    protected void beginTest() {
      // The unit-test lanes turn fsync off (arcadedb.fileSync), and with it every flush this test counts
      GlobalConfiguration.FILE_SYNC.setValue(true);
      FileUtils.TEST_FILE_SYNC_HOOK = synced::add;
    }

    @Override
    protected void endTest() {
      FileUtils.TEST_FILE_SYNC_HOOK = null;
      GlobalConfiguration.FILE_SYNC.reset();
    }

    @Test
    void aDdlStatementFlushesOnlyThePrimarySchemaFile() throws Exception {
      // The first write of an open still flushes the copy: it has not yet seen the file store publish atomically
      database.getSchema().createDocumentType("Warm");

      synced.clear();
      database.getSchema().createDocumentType("Doc").createProperty("k", String.class);

      assertThat(syncsOf(LocalSchema.SCHEMA_FILE_NAME)).as("schema.json is still flushed on every write").isEqualTo(2);
      assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("the recovery copy is not flushed per write").isZero();
      // The copy is still published on every write, and holds the generation before the current one
      assertThat(prevFile()).exists();
      assertThat(versionOf(prevFile())).isEqualTo(versionOf(schemaFile()) - 1);
    }

    @Test
    void theFirstWriteOfAnOpenFlushesTheCopyUntilAnAtomicPublicationIsSeen() {
      database.getSchema().createDocumentType("Before");
      reopenDatabase();

      synced.clear();
      database.getSchema().createDocumentType("First");
      assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("nothing is known yet about this open's file store").isEqualTo(1);

      synced.clear();
      database.getSchema().createDocumentType("Second");
      assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).isZero();
    }

    @Test
    void aCleanCloseFlushesTheRecoveryCopyOnce() throws Exception {
      database.getSchema().createDocumentType("Warm");
      database.getSchema().createDocumentType("A");
      database.getSchema().createDocumentType("B");
      final byte[] prevBeforeClose = Files.readAllBytes(prevFile());

      synced.clear();
      reopenDatabase();
      assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("the close made the deferred copy durable").isEqualTo(1);
      assertThat(Files.readAllBytes(prevFile())).isEqualTo(prevBeforeClose);

      // Nothing written since the open: nothing owed at the next close
      synced.clear();
      reopenDatabase();
      assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).isZero();
    }

    @Test
    void aRecoveryCopyLostInACrashIsRebuiltFromThePrimaryAtRecovery() throws Exception {
      database.getSchema().createDocumentType("Warm");
      database.getSchema().createDocumentType("A");
      crashAndLoseTheRecoveryCopy(new byte[0]);

      synced.clear();
      database = factory.open();

      assertThat(database.getSchema().existsType("A")).isTrue();
      assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("rebuilt durably at recovery").isEqualTo(1);
      assertThat(Files.readString(prevFile())).isEqualTo(Files.readString(schemaFile()));
      assertThat(versionOf(prevFile())).isEqualTo(versionOf(schemaFile()));
    }

    @Test
    void aRecoveryCopyLeftWithZeroedBlocksIsRebuiltToo() throws Exception {
      database.getSchema().createDocumentType("Warm");
      database.getSchema().createDocumentType("A");
      // What a file system that allocated the blocks without writing them leaves: the right length, all zeroes
      crashAndLoseTheRecoveryCopy(new byte[(int) Files.size(prevFile())]);

      database = factory.open();

      assertThat(Files.readString(prevFile())).isEqualTo(Files.readString(schemaFile()));
    }

    @Test
    void aRecoveryCopyThatParsesButIsNoLoadableSchemaIsRebuiltToo() throws Exception {
      database.getSchema().createDocumentType("Warm");
      database.getSchema().createDocumentType("A");
      // Valid JSON with a version, but without the settings and types the load requires
      crashAndLoseTheRecoveryCopy("{\"schemaVersion\":7}".getBytes(StandardCharsets.UTF_8));

      database = factory.open();

      assertThat(Files.readString(prevFile())).isEqualTo(Files.readString(schemaFile()));
    }

    @Test
    void aReadableRecoveryCopyKeepsItsGenerationAndIsMadeDurableAtRecovery() throws Exception {
      database.getSchema().createDocumentType("Warm");
      database.getSchema().createDocumentType("A");
      final byte[] prevBeforeCrash = Files.readAllBytes(prevFile());
      crashAndLoseTheRecoveryCopy(null);

      synced.clear();
      database = factory.open();

      // A process crash leaves the copy in the page cache, where a later power failure can still drop it
      assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).isEqualTo(1);
      assertThat(Files.readAllBytes(prevFile())).as("the previous generation is kept, not replaced").isEqualTo(prevBeforeCrash);
      assertThat(versionOf(prevFile())).isEqualTo(versionOf(schemaFile()) - 1);
    }

    @Test
    void theRecoveryCopyStillServesAsTheFallbackForADamagedPrimary() throws Exception {
      database.getSchema().createDocumentType("Warm");
      database.getSchema().createDocumentType("A");
      database.getSchema().createDocumentType("B");
      database.close();

      // The primary is damaged after a clean close: the copy, flushed by that close, is what the next open reads
      Files.writeString(schemaFile(), "{ not json");
      database = factory.open();

      assertThat(database.getSchema().existsType("A")).isTrue();
      assertThat(((LocalSchema) database.getSchema().getEmbedded()).isLoadedFromPreviousSchema()).isTrue();
    }

    @Test
    void aRecoveryCopyWhoseNameWasLostInACrashIsRebuiltAtRecovery() throws Exception {
      database.getSchema().createDocumentType("Warm");
      database.getSchema().createDocumentType("A");
      crashAndLoseTheRecoveryCopy(null);
      Files.delete(prevFile());

      synced.clear();
      database = factory.open();

      assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).isEqualTo(1);
      assertThat(Files.readString(prevFile())).isEqualTo(Files.readString(schemaFile()));
    }

    @Test
    void aFailedFlushAtCloseKeepsTheLockFileSoTheNextOpenReSecuresTheCopy() throws Exception {
      database.getSchema().createDocumentType("Warm");
      database.getSchema().createDocumentType("A");
      final Path lockFile = Path.of(getDatabasePath()).toAbsolutePath().resolve(NodeLocalFiles.DATABASE_LOCK_FILE_NAME);

      // A copy the close cannot open for writing, whatever the privileges of the runner: a directory under its name
      final byte[] prevContent = Files.readAllBytes(prevFile());
      Files.delete(prevFile());
      Files.createDirectory(prevFile());
      try {
        database.close();
        assertThat(lockFile).as("kept as the marker that makes the next open re-secure the copy").exists();
      } finally {
        Files.delete(prevFile());
        Files.write(prevFile(), prevContent);
      }

      synced.clear();
      database = factory.open();

      assertThat(database.getSchema().existsType("A")).isTrue();
      assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("re-secured by the recovery").isEqualTo(1);
      assertThat(((LocalSchema) database.getSchema().getEmbedded()).isPreviousSchemaCopyUnsynced()).isFalse();

      // A clean close after that deletes the marker as usual
      database.close();
      assertThat(lockFile).doesNotExist();
      database = factory.open();
    }

    @Test
    void aCopyTheRecoveryCouldNotRebuildIsRebuiltByTheClose() throws Exception {
      // Missing by the close: there is nothing to flush, the close has to rebuild it
      closeAfterARecoveryThatCouldNotRebuildTheCopy(null);
    }

    @Test
    void aDamagedCopyTheRecoveryCouldNotRebuildIsRebuiltByTheCloseNotFlushed() throws Exception {
      // Still damaged by the close: flushing it would secure a copy that cannot be loaded, the close has to rebuild it
      closeAfterARecoveryThatCouldNotRebuildTheCopy(new byte[512]);
    }

    /**
     * The recovery's rebuild of a copy lost in the crash fails (a directory under its name is in the way), then the directory
     * gives way to {@code copyAtClose} ({@code null}: nothing) before the close, which must rebuild the copy from the primary.
     */
    private void closeAfterARecoveryThatCouldNotRebuildTheCopy(final byte[] copyAtClose) throws Exception {
      database.getSchema().createDocumentType("Warm");
      database.getSchema().createDocumentType("A");
      crashAndLoseTheRecoveryCopy(null);
      final Path lockFile = Path.of(getDatabasePath()).toAbsolutePath().resolve(NodeLocalFiles.DATABASE_LOCK_FILE_NAME);

      Files.delete(prevFile());
      Files.createDirectory(prevFile());
      database = factory.open();
      assertThat(((LocalSchema) database.getSchema().getEmbedded()).isPreviousSchemaCopyUnsynced()).isTrue();

      Files.delete(prevFile());
      if (copyAtClose != null)
        Files.write(prevFile(), copyAtClose);
      database.close();

      assertThat(Files.readString(prevFile())).isEqualTo(Files.readString(schemaFile()));
      assertThat(lockFile).doesNotExist();
      database = factory.open();
    }

    /**
     * Simulates a crash after which {@code schema.prev.json} holds {@code prevContent} ({@code null}: left as it was). The kill
     * runs the schema close too, so fsync is off meanwhile: the copy reaches the recovery with its flush still owed.
     */
    private void crashAndLoseTheRecoveryCopy(final byte[] prevContent) throws Exception {
      synced.clear();
      GlobalConfiguration.FILE_SYNC.setValue(false);
      try {
        ((DatabaseInternal) database).kill();
        database.close();
      } finally {
        GlobalConfiguration.FILE_SYNC.setValue(true);
      }
      assertThat(syncsOf(LocalSchema.SCHEMA_PREV_FILE_NAME)).as("the simulated crash flushed nothing").isZero();
      if (prevContent != null)
        Files.write(prevFile(), prevContent);
    }

    private long syncsOf(final String fileName) {
      return synced.stream().filter(p -> p.getFileName().toString().equals(fileName)).count();
    }

    private Path schemaFile() {
      return Path.of(getDatabasePath()).toAbsolutePath().resolve(LocalSchema.SCHEMA_FILE_NAME);
    }

    private Path prevFile() {
      return Path.of(getDatabasePath()).toAbsolutePath().resolve(LocalSchema.SCHEMA_PREV_FILE_NAME);
    }

    private static long versionOf(final Path file) throws Exception {
      return new JSONObject(Files.readString(file)).getLong("schemaVersion");
    }
  }

  /**
   * Regression for #7604: {@code LocalSchema.update()} publishes the in-memory schema version only after the bytes are on disk,
   * but its two internal callers - {@code saveConfiguration()} and {@code close()} - advanced {@code versionSerial}
   * BEFORE calling it. A failed write therefore left the in-memory version a generation ahead of every file.
   */
  @Nested
  class SchemaVersionPublishedAfterSave extends TestHelper {

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
}
