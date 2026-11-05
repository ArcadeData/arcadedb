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
package com.arcadedb.index.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexBuildTestHooks;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A process killed in the middle of {@code REBUILD INDEX} on an LSM_VECTOR type index reopened without the index: the rebuild
 * dropped it first. It is rebuilt in place now, so a crash must reopen with the index present and valid, with no record held twice
 * and no entry of a record that does not exist, and a rebuild run again must complete it. The index starts out of sync with its
 * records (some lost, one of a deleted record), so the rebuild has something to write. Each case kills a child JVM
 * ({@code Runtime.halt}, as a SIGKILL would): in the middle of the scan of the first bucket, and in the middle of the second bucket,
 * once the first one is reconciled and compacted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class RebuildVectorIndexInPlaceCrashTest {
  private static final int    CRASH_EXIT_CODE = 73;
  private static final int    BUCKETS         = 4;
  private static final int    RECORDS         = 2_000;
  private static final int    LOST            = 40;
  private static final int    DIMENSIONS      = 16;
  private static final String TYPE            = "CrashDoc";
  private static final String INDEX           = TYPE + "[embedding]";

  @TempDir
  Path tempDirectory;

  @ParameterizedTest
  @ValueSource(strings = { "mid-first-bucket", "mid-second-bucket" })
  void aCrashDuringTheRebuildLeavesAValidIndex(final String mode) throws Exception {
    final Path databasePath = tempDirectory.resolve(mode);
    final ChildResult prepared = runChild("prepare", databasePath);
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    final ChildResult crashed = runChild(mode, databasePath);
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CRASH_EXIT_CODE);

    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
      assertThat(index).as("the index survives the crash").isNotNull();
      assertThat(index.isValid()).isTrue();
      for (final Index any : database.getSchema().getIndexes())
        assertThat(any.getTypeName()).as("index %s belongs to a type", any.getName()).isNotNull();
      // the lost records are written again or not yet, the entry of the deleted record removed or not yet: never more than that
      assertThat(index.countEntries()).isBetween((long) RECORDS - LOST, (long) RECORDS + 1);
      for (final IndexInternal onBucket : index.getIndexesOnBuckets())
        assertThat(((LSMVectorIndex) onBucket).inPlaceRebuildRefusal()).as("bucket index %s is sound", onBucket.getName()).isNull();

      // and the rebuild can run again to the end
      database.command("sql", "REBUILD INDEX `" + INDEX + "`");
      final TypeIndex rebuilt = (TypeIndex) database.getSchema().getIndexByName(INDEX);
      assertThat(rebuilt).isSameAs(index);
      assertThat(rebuilt.countEntries()).isEqualTo(RECORDS);
      assertEveryRecordFindsItself(database, rebuilt);
    }
  }

  private static void assertEveryRecordFindsItself(final Database database, final TypeIndex index) {
    int checked = 0;
    for (final Iterator<Record> it = database.iterateType(TYPE, false); it.hasNext(); ) {
      final Document doc = it.next().asDocument();
      if (checked++ % 10 != 0)
        continue;
      LSMVectorIndex holder = null;
      for (final IndexInternal onBucket : index.getIndexesOnBuckets())
        if (onBucket.getAssociatedBucketId() == doc.getIdentity().getBucketId())
          holder = (LSMVectorIndex) onBucket;
      assertThat(holder).isNotNull();
      final List<Pair<RID, Float>> found = holder.findNeighborsFromVector((float[]) doc.get("embedding"), 1);
      assertThat(found).as("record %s is found by its own vector", doc.getIdentity()).isNotEmpty();
      assertThat(found.get(0).getFirst()).isEqualTo(doc.getIdentity());
    }
  }

  public static void main(final String[] args) {
    if (args.length != 2)
      throw new IllegalArgumentException("Expected mode and database path");
    if ("prepare".equals(args[0]))
      prepare(args[1]);
    else
      crash(args[0], args[1]);
  }

  private static void prepare(final String databasePath) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.create()) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(BUCKETS).create();
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      final Random random = new Random(42);
      final List<RID> rids = new ArrayList<>();
      final List<float[]> vectors = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++) {
          final float[] vector = randomVector(random);
          final MutableDocument doc = database.newDocument(TYPE).set("embedding", vector);
          doc.save();
          rids.add(doc.getIdentity());
          vectors.add(vector);
        }
      });
      database.command("sql", "CREATE INDEX ON " + TYPE + " (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\" }");
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);

      // out of sync: entries lost, spread over the buckets, and one left by a deleted record
      for (int i = 0; i < LOST; i++) {
        final RID rid = rids.get(i * 7);
        final float[] vector = vectors.get(i * 7);
        final IndexInternal holder = bucketIndexOf(index, rid.getBucketId());
        database.transaction(() -> holder.remove(new Object[] { vector }, rid));
      }
      final float[] ghostVector = randomVector(random);
      final RID[] ghost = new RID[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument(TYPE).set("embedding", ghostVector);
        doc.save();
        ghost[0] = doc.getIdentity();
      });
      database.transaction(() -> database.deleteRecord(database.lookupByRID(ghost[0], true)));
      final IndexInternal ghostHolder = bucketIndexOf(index, ghost[0].getBucketId());
      database.transaction(() -> ghostHolder.put(new Object[] { ghostVector }, new RID[] { ghost[0] }));
    }
  }

  private static void crash(final String point, final String databasePath) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.open()) {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
      final int firstBucketRecords = (int) database.countBucket(
          database.getSchema().getBucketById(index.getIndexesOnBuckets()[0].getAssociatedBucketId()).getName());
      final int haltAt = switch (point) {
        case "mid-first-bucket" -> firstBucketRecords / 2;
        case "mid-second-bucket" -> firstBucketRecords + 100;
        default -> throw new IllegalArgumentException("Unknown crash point " + point);
      };
      final AtomicInteger records = new AtomicInteger();
      IndexBuildTestHooks.onRecordIndexed = (onBucket, rid) -> {
        if (records.incrementAndGet() == haltAt)
          Runtime.getRuntime().halt(CRASH_EXIT_CODE);
      };
      database.command("sql", "REBUILD INDEX `" + INDEX + "`");
    }
    throw new AssertionError("The rebuild completed without reaching the crash point " + point);
  }

  private static IndexInternal bucketIndexOf(final TypeIndex index, final int bucketId) {
    for (final IndexInternal onBucket : index.getIndexesOnBuckets())
      if (onBucket.getAssociatedBucketId() == bucketId)
        return onBucket;
    throw new AssertionError("No bucket index for bucket " + bucketId);
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = random.nextFloat() * 2 - 1;
    return vector;
  }

  private ChildResult runChild(final String mode, final Path databasePath) throws IOException, InterruptedException {
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    final List<String> command = new ArrayList<>(List.of(java, "-Xms128m", "-Xmx512m", "--add-exports",
        "java.management/sun.management=ALL-UNNAMED", "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED", "--add-opens",
        "java.base/java.nio.channels.spi=ALL-UNNAMED", "--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED",
        "-cp", classpath, RebuildVectorIndexInPlaceCrashTest.class.getName(), mode, databasePath.toString()));
    // to a file, not a pipe: reading a pipe blocks until the child closes it, so a hung child would never reach the timeout
    final Path outputFile = Files.createTempFile(tempDirectory, "child-", ".log");
    final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(outputFile.toFile()).start();
    final boolean finished = process.waitFor(120, TimeUnit.SECONDS);
    if (!finished)
      process.destroyForcibly().waitFor();
    final String output = Files.readString(outputFile, StandardCharsets.UTF_8);
    assertThat(finished).as(output).isTrue();
    return new ChildResult(process.exitValue(), output);
  }

  private record ChildResult(int exitCode, String output) {
  }
}
