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
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexBuildTestHooks;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A process killed in the middle of {@code REBUILD INDEX} on a GEOSPATIAL or FULL_TEXT type index reopened without the index: the
 * rebuild dropped it first. These kinds are rebuilt in place now, so a crash at any point must reopen with the index present, valid
 * and complete, holding exactly the entries an index built from scratch holds. Each case kills a child JVM ({@code Runtime.halt}, as
 * a SIGKILL would) at one point of the rebuild: in the middle of the scan of the first bucket, after its sort (before its swap),
 * after the first swap, and after a swap was saved but before the previous file was dropped.
 * <p>
 * "From scratch" is an index of the same kind and settings created after the reopen on a copy of the indexed property.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildIndexInPlaceKindsCrashTest {
  private static final int    CRASH_EXIT_CODE = 73;
  private static final int    BUCKETS         = 4;
  private static final int    RECORDS         = 2_000;
  private static final String GEO_TYPE        = "CrashPlace";
  private static final String FT_TYPE         = "CrashArticle";

  @TempDir
  Path tempDirectory;

  @ParameterizedTest
  @ValueSource(strings = { "geo:mid-scan", "geo:after-sort", "geo:after-first-swap", "geo:after-swap-before-old-drop", "ft:mid-scan",
      "ft:after-sort", "ft:after-first-swap", "ft:after-swap-before-old-drop" })
  void aCrashDuringTheRebuildLeavesACompleteIndex(final String mode) throws Exception {
    final String kind = mode.substring(0, mode.indexOf(':'));
    final Path databasePath = tempDirectory.resolve(mode.replace(':', '-'));
    final ChildResult prepared = runChild("prepare:" + kind, databasePath);
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    final ChildResult crashed = runChild(mode, databasePath);
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CRASH_EXIT_CODE);

    final String typeName = typeOf(kind);
    final String indexName = typeName + "[" + propertyOf(kind) + "]";
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
      assertThat(index).as("the index survives the crash").isNotNull();
      assertThat(index.isValid()).isTrue();
      assertNoUnreferencedIndex(database);

      createFreshIndexOnTheCopy(database, kind);
      final TypeIndex fresh = (TypeIndex) database.getSchema().getIndexByName(typeName + ("ft".equals(kind) ? "[titleCopy,bodyCopy]" : "[copy]"));
      assertSameEntries(database, typeName, index, fresh);
      if ("ft".equals(kind)) {
        // the BM25 corpus counters the rebuild left are the ones of the index built from scratch
        final FullTextIndexMetadata metadata = LSMTreeFullTextIndex.metadataOf(index);
        final FullTextIndexMetadata freshMetadata = LSMTreeFullTextIndex.metadataOf(fresh);
        assertThat(metadata.getTotalDocs()).isEqualTo(freshMetadata.getTotalDocs());
        assertThat(metadata.getSumDocLength()).isEqualTo(freshMetadata.getSumDocLength());
      }

      // and the rebuild can run again to the end
      database.command("sql", "REBUILD INDEX `" + indexName + "`");
      assertSameEntries(database, typeName, (TypeIndex) database.getSchema().getIndexByName(indexName), fresh);
    }
    assertThat(buildArtifacts(databasePath)).isEmpty();
  }

  private static String typeOf(final String kind) {
    return switch (kind) {
      case "geo" -> GEO_TYPE;
      case "ft" -> FT_TYPE;
      default -> throw new IllegalArgumentException(kind);
    };
  }

  private static String propertyOf(final String kind) {
    return switch (kind) {
      case "geo" -> "coords";
      case "ft" -> "title,body";
      default -> throw new IllegalArgumentException(kind);
    };
  }

  private static void createFreshIndexOnTheCopy(final Database database, final String kind) {
    switch (kind) {
    case "geo" -> database.command("sql", "CREATE INDEX ON " + GEO_TYPE + " (copy) GEOSPATIAL");
    case "ft" -> database.command("sql", "CREATE INDEX ON " + FT_TYPE + " (titleCopy, bodyCopy) FULL_TEXT");
    default -> throw new IllegalArgumentException(kind);
    }
  }

  /** The same number of entries as the index built from scratch, bucket by bucket, and one bucket index per bucket. */
  private static void assertSameEntries(final Database database, final String typeName, final TypeIndex index, final TypeIndex fresh) {
    final DocumentType type = database.getSchema().getType(typeName);
    assertThat(index.getIndexesOnBuckets()).hasSize(BUCKETS);
    final Map<Integer, Long> expected = new HashMap<>();
    for (final IndexInternal onBucket : fresh.getIndexesOnBuckets())
      expected.put(onBucket.getAssociatedBucketId(), onBucket.countEntries());
    for (final Bucket bucket : type.getBuckets(false)) {
      IndexInternal onBucket = null;
      for (final IndexInternal candidate : index.getIndexesOnBuckets())
        if (candidate.getAssociatedBucketId() == bucket.getFileId())
          onBucket = candidate;
      assertThat(onBucket).as("bucket index of %s", bucket.getName()).isNotNull();
      assertThat(onBucket.countEntries()).as("entries of %s", bucket.getName()).isEqualTo(expected.get(bucket.getFileId()));
    }
  }

  private static void assertNoUnreferencedIndex(final Database database) {
    for (final Index index : database.getSchema().getIndexes())
      assertThat(index.getTypeName()).as("index %s belongs to a type", index.getName()).isNotNull();
  }

  public static void main(final String[] args) {
    if (args.length != 2)
      throw new IllegalArgumentException("Expected mode and database path");
    final String mode = args[0];
    final String kind = mode.substring(mode.indexOf(':') + 1);
    if (mode.startsWith("prepare:"))
      prepare(kind, args[1]);
    else
      crash(mode.substring(0, mode.indexOf(':')), mode.substring(mode.indexOf(':') + 1), args[1]);
  }

  private static void prepare(final String kind, final String databasePath) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.create()) {
      switch (kind) {
      case "geo" -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(GEO_TYPE).withTotalBuckets(BUCKETS).create();
        type.createProperty("coords", Type.STRING);
        type.createProperty("copy", Type.STRING);
        database.transaction(() -> {
          for (int i = 0; i < RECORDS; i++) {
            final String wkt = i % 10 == 0 ?
                "POLYGON ((%1$d.1 %2$d.1, %1$d.6 %2$d.1, %1$d.6 %2$d.6, %1$d.1 %2$d.6, %1$d.1 %2$d.1))".formatted(5 + i % 15, 35 + i % 12) :
                "POINT (%s %s)".formatted(5 + (i % 150) / 10.0, 35 + (i % 120) / 10.0);
            database.newDocument(GEO_TYPE).set("coords", wkt, "copy", wkt).save();
          }
        });
        database.command("sql", "CREATE INDEX ON " + GEO_TYPE + " (coords) GEOSPATIAL");
      }
      case "ft" -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(FT_TYPE).withTotalBuckets(BUCKETS).create();
        for (final String property : new String[] { "title", "body", "titleCopy", "bodyCopy" })
          type.createProperty(property, Type.STRING);
        final String[] words = { "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "omega" };
        database.transaction(() -> {
          for (int i = 0; i < RECORDS; i++) {
            final StringBuilder body = new StringBuilder();
            for (int w = 0; w < 3 + i % 9; w++)
              body.append(words[(i * 7 + w * 3) % words.length]).append(' ');
            final String title = words[i % words.length] + " " + words[(i / 3) % words.length];
            database.newDocument(FT_TYPE).set("title", title, "body", body.toString(), "titleCopy", title, "bodyCopy", body.toString())
                .save();
          }
        });
        database.command("sql", "CREATE INDEX ON " + FT_TYPE + " (title, body) FULL_TEXT");
      }
      default -> throw new IllegalArgumentException(kind);
      }
    }
  }

  private static void crash(final String kind, final String point, final String databasePath) {
    switch (point) {
    case "mid-scan" -> {
      final AtomicInteger records = new AtomicInteger();
      IndexBuildTestHooks.onRecordIndexed = (index, rid) -> {
        if (records.incrementAndGet() == 100)
          Runtime.getRuntime().halt(CRASH_EXIT_CODE);
      };
    }
    case "after-sort" -> LSMTreeIndexBulkLoader.setBuildTestHook((phase, index, completed) -> {
      if (phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_SORT)
        Runtime.getRuntime().halt(CRASH_EXIT_CODE);
    });
    case "after-first-swap" -> LSMTreeIndexBulkLoader.setBuildTestHook((phase, index, completed) -> {
      if (phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_ATTACHMENT)
        Runtime.getRuntime().halt(CRASH_EXIT_CODE);
    });
    case "after-swap-before-old-drop" -> LSMTreeIndex.setAfterSwapTestHook(() -> Runtime.getRuntime().halt(CRASH_EXIT_CODE));
    default -> throw new IllegalArgumentException("Unknown crash point " + point);
    }

    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.open()) {
      database.command("sql", "REBUILD INDEX `" + typeOf(kind) + "[" + propertyOf(kind) + "]`");
    }
    throw new AssertionError("The rebuild completed without reaching the crash point " + point);
  }

  private ChildResult runChild(final String mode, final Path databasePath) throws IOException, InterruptedException {
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    final List<String> command = new ArrayList<>(List.of(java, "-Xms128m", "-Xmx512m", "--add-exports",
        "java.management/sun.management=ALL-UNNAMED", "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED", "--add-opens",
        "java.base/java.nio.channels.spi=ALL-UNNAMED", "--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED",
        "-cp", classpath, RebuildIndexInPlaceKindsCrashTest.class.getName(), mode, databasePath.toString()));
    // to a file, not a pipe: reading a pipe blocks until the child closes it, so a hung child would never reach the timeout
    final Path outputFile = Files.createTempFile(tempDirectory, "child-", ".log");
    final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(outputFile.toFile()).start();
    final boolean finished = process.waitFor(60, TimeUnit.SECONDS);
    if (!finished)
      process.destroyForcibly().waitFor();
    final String output = Files.readString(outputFile, StandardCharsets.UTF_8);
    assertThat(finished).as(output).isTrue();
    return new ChildResult(process.exitValue(), output);
  }

  private static List<Path> buildArtifacts(final Path databasePath) throws IOException {
    try (final Stream<Path> paths = Files.list(databasePath)) {
      return paths.filter(path -> {
        final String name = path.getFileName().toString();
        return name.startsWith(".arcadedb-sorted-index-build-") || name.startsWith(".arcadedb-index-sort-");
      }).toList();
    }
  }

  private record ChildResult(int exitCode, String output) {
  }
}
