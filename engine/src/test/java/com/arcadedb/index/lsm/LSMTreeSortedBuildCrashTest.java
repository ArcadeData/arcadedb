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
package com.arcadedb.index.lsm;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexBuildMode;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LSMTreeSortedBuildCrashTest {
  private static final int CRASH_EXIT_CODE = 73;

  @TempDir
  Path tempDirectory;

  @Test
  void crashAfterAttachmentLeavesSchemaUnpublishedAndAllowsRetry() throws Exception {
    final Path databasePath = tempDirectory.resolve("crash-after-attachment");
    final ChildResult prepared = runChild("prepare", databasePath);
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    final ChildResult crashed = runChild("crash", databasePath);
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CRASH_EXIT_CODE);

    assertThatThrownBy(() -> {
      try (DatabaseFactory factory = new DatabaseFactory(databasePath.toString())) {
        factory.open(ComponentFile.MODE.READ_ONLY);
      }
    }).hasRootCauseMessage("Database contains an interrupted sorted index build; reopen it read-write to perform cleanup");

    try (DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); Database database = factory.open()) {
      assertThat(database.getSchema().getType("CrashBuild").getIndexByProperties("lookupKey")).isNull();
      assertThat(database.getSchema().getIndexes()).isEmpty();
      assertThat(database.countType("CrashBuild", false)).isEqualTo(2_000L);

      final TypeIndex index = database.getSchema().buildTypeIndex("CrashBuild", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget(16L << 20)
          .withUnique(false)
          .create();
      assertThat(count(index.iterator(true))).isEqualTo(2_000);
    }

    try (DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); Database database = factory.open()) {
      final TypeIndex index = database.getSchema().getType("CrashBuild").getIndexByProperties("lookupKey");
      assertThat(index).isNotNull();
      assertThat(count(index.iterator(false))).isEqualTo(2_000);
    }
  }

  @Test
  void crashAfterExternalSortRemovesSpillWorkspaceAndStagedFiles() throws Exception {
    final Path databasePath = tempDirectory.resolve("crash-after-sort");
    assertThat(runChild("prepare", databasePath).exitCode()).isZero();
    assertThat(runChild("crash-after-sort", databasePath).exitCode()).isEqualTo(CRASH_EXIT_CODE);
    assertThat(buildArtifacts(databasePath)).isNotEmpty();

    try (DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); Database database = factory.open()) {
      assertThat(database.getSchema().getType("CrashBuild").getIndexByProperties("lookupKey")).isNull();
      assertThat(database.getSchema().getIndexes()).isEmpty();
      assertThat(database.countType("CrashBuild", false)).isEqualTo(2_000L);
    }
    assertThat(buildArtifacts(databasePath)).isEmpty();
  }

  @Test
  void uniqueBuildCrashAfterAttachmentLeavesNothingPublishedAndAllowsRetry() throws Exception {
    final Path databasePath = tempDirectory.resolve("unique-crash-after-attachment");
    assertThat(runChild("prepare", databasePath).exitCode()).isZero();
    assertThat(runChild("crash-unique", databasePath).exitCode()).isEqualTo(CRASH_EXIT_CODE);

    try (DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); Database database = factory.open()) {
      assertThat(database.getSchema().getType("CrashBuild").getIndexByProperties("lookupKey")).isNull();
      assertThat(database.getSchema().getIndexes()).isEmpty();

      final TypeIndex index = database.getSchema().buildTypeIndex("CrashBuild", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget(16L << 20)
          .withUnique(true)
          .create();
      assertThat(index.isUnique()).isTrue();
      assertThat(count(index.iterator(true))).isEqualTo(2_000);
    }

    assertThat(buildArtifacts(databasePath)).isEmpty();
  }

  public static void main(final String[] args) {
    if (args.length != 2)
      throw new IllegalArgumentException("Expected mode and database path");

    if ("prepare".equals(args[0])) {
      prepare(args[1]);
      return;
    }
    if ("crash".equals(args[0]) || "crash-after-sort".equals(args[0]) || "crash-unique".equals(args[0])) {
      crash(args[0], args[1]);
      return;
    }
    throw new IllegalArgumentException("Unknown mode " + args[0]);
  }

  private static void prepare(final String databasePath) {
    try (DatabaseFactory factory = new DatabaseFactory(databasePath); Database database = factory.create()) {
      final DocumentType type = database.getSchema().buildDocumentType().withName("CrashBuild").withTotalBuckets(2).create();
      type.createProperty("lookupKey", Type.INTEGER);
      database.transaction(() -> {
        for (int i = 0; i < 2_000; i++)
          database.newDocument("CrashBuild").set("lookupKey", i).save();
      });
    }
  }

  private static void crash(final String mode, final String databasePath) {
    LSMTreeIndexBulkLoader.setBuildTestHook((phase, index, completedBuckets) -> {
      if ((("crash".equals(mode) || "crash-unique".equals(mode))
          && phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_ATTACHMENT && completedBuckets == 1)
          || ("crash-after-sort".equals(mode) && phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_SORT))
        Runtime.getRuntime().halt(CRASH_EXIT_CODE);
    });

    try (DatabaseFactory factory = new DatabaseFactory(databasePath); Database database = factory.open()) {
      database.getSchema().buildTypeIndex("CrashBuild", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget("crash-after-sort".equals(mode) ? 1L << 20 : 16L << 20)
          .withUnique("crash-unique".equals(mode))
          .create();
    }
    throw new AssertionError("Sorted build completed without triggering crash hook");
  }

  private ChildResult runChild(final String mode, final Path databasePath) throws IOException, InterruptedException {
    final Process process = startChild(mode, databasePath);
    final String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(process.waitFor(30, TimeUnit.SECONDS)).as(output).isTrue();
    return new ChildResult(process.exitValue(), output);
  }

  private Process startChild(final String mode, final Path databasePath) throws IOException {
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    final List<String> command = new ArrayList<>();
    command.add(java);
    command.add("-Xms128m");
    command.add("-Xmx512m");
    command.add("--add-exports");
    command.add("java.management/sun.management=ALL-UNNAMED");
    command.add("--add-opens");
    command.add("java.base/java.util.concurrent.atomic=ALL-UNNAMED");
    command.add("--add-opens");
    command.add("java.base/java.nio.channels.spi=ALL-UNNAMED");
    command.add("--add-modules");
    command.add("jdk.incubator.vector");
    command.add("--enable-native-access=ALL-UNNAMED");
    command.add("-cp");
    command.add(classpath);
    command.add(LSMTreeSortedBuildCrashTest.class.getName());
    command.add(mode);
    command.add(databasePath.toString());
    return new ProcessBuilder(command).redirectErrorStream(true).start();
  }

  private static int count(final IndexCursor cursor) {
    int count = 0;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      return count;
    } finally {
      cursor.close();
    }
  }

  private static List<Path> buildArtifacts(final Path databasePath) throws IOException {
    try (Stream<Path> paths = Files.list(databasePath)) {
      return paths.filter(path -> {
        final String name = path.getFileName().toString();
        return name.startsWith(".arcadedb-sorted-index-build-") || name.startsWith(".arcadedb-index-sort-");
      }).toList();
    }
  }

  private record ChildResult(int exitCode, String output) {
  }
}
