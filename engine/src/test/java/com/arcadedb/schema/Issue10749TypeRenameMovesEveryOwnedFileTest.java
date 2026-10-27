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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.timeseries.TimeSeriesBucket;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesShard;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Issue #10749: {@code ALTER TYPE ... NAME} on a TIMESERIES type moved none of the files its samples live in. The samples are kept
 * by the type's {@code TimeSeriesEngine}: a mutable bucket and a sealed store per shard and one tag dictionary, named after the type
 * but neither the type's buckets nor an index file, which were the only two things the rename walked. The statement was accepted,
 * the session kept reading the engine still bound to the old files, and the next open built a fresh, empty engine under the new
 * name, stranding every sample in files no component pointed at. Under HA the session that ships a rename recorded none of those
 * files either.
 * <p>
 * The census is over every concrete kind of type, enumerated from the classes of the schema package rather than from a list here:
 * write a record, rename, reopen, and the record is still read under the new name AND no file of the database still carries the old
 * one. Reading back in the same session, or checking only that the type answers to the new name, passes without the fix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10749TypeRenameMovesEveryOwnedFileTest extends TestHelper {
  private static final String OLD = "Old10749";
  private static final String NEW = "New10749";

  @Test
  void everyKindOfTypeKeepsItsRecordsAndLeavesNoFileUnderTheOldName() throws Exception {
    final Map<Class<?>, Kind> kinds = new LinkedHashMap<>();
    kinds.put(LocalDocumentType.class, new Kind(
        name -> database.command("sql", "CREATE DOCUMENT TYPE " + name),
        name -> database.transaction(() -> database.newDocument(name).set("id", 1).save())));
    kinds.put(LocalVertexType.class, new Kind(
        name -> database.command("sql", "CREATE VERTEX TYPE " + name),
        name -> database.transaction(() -> {
          final MutableVertex a = database.newVertex(name).set("id", 1).save();
          a.newEdge("Link10749", database.newVertex(name).set("id", 2).save()).save();
        })));
    kinds.put(LocalEdgeType.class, new Kind(
        name -> database.command("sql", "CREATE EDGE TYPE " + name),
        name -> database.transaction(() -> database.newVertex("Node10749").save().newEdge(name, database.newVertex("Node10749").save())
            .set("id", 1).save())));
    kinds.put(LocalTimeSeriesType.class, new Kind(
        name -> database.command("sql",
            "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2"),
        name -> database.command("sql", "INSERT INTO " + name + " SET ts = 1000, host = 'h1', v = 1.5")));

    // a new kind of type has to say here how it is created and written, or this fails: its files are what the census is about
    for (final Class<?> concrete : concreteTypeClasses())
      assertThat(kinds).as("a census entry for %s", concrete.getSimpleName()).containsKey(concrete);

    database.command("sql", "CREATE VERTEX TYPE Node10749");
    database.command("sql", "CREATE EDGE TYPE Link10749");

    int round = 0;
    for (final Map.Entry<Class<?>, Kind> kind : kinds.entrySet()) {
      final String oldName = OLD + "K" + round;
      final String newName = NEW + "K" + round++;
      kind.getValue().create().accept(oldName);
      kind.getValue().write().accept(oldName);
      final long records = count(oldName);
      assertThat(records).as("%s written", kind.getKey().getSimpleName()).isPositive();

      database.command("sql", "ALTER TYPE " + oldName + " NAME " + newName);
      assertThat(count(newName)).as("%s in the session of the rename", kind.getKey().getSimpleName()).isEqualTo(records);

      reopenDatabase();

      assertThat(database.getSchema().getType(newName)).as("%s", kind.getKey().getSimpleName()).isInstanceOf(kind.getKey());
      assertThat(count(newName)).as("%s after a reopen", kind.getKey().getSimpleName()).isEqualTo(records);
      assertThat(filesNaming(oldName)).as("files of %s still named after '%s'", kind.getKey().getSimpleName(), oldName).isEmpty();
    }
  }

  /**
   * The samples of a TIMESERIES type live in two layers per shard, and the rename has to carry both: the sealed blocks a compaction
   * wrote, which are not a component at all but a file beside the shard's mutable bucket, and the samples still in the mutable bucket.
   * A sample written under the new name lands in the same files, and the tag dictionary keeps resolving the tags of the old ones.
   */
  @Test
  void aTimeSeriesTypeKeepsItsSealedAndMutableSamplesAcrossARenameAndAReopen() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
    for (int i = 0; i < 6; i++)
      insert(OLD, 1000L * (i + 1), "h" + (i % 3), i);
    engineOf(OLD).compactAll();
    insert(OLD, 10_000L, "h1", 100);
    assertThat(sealedBlocks(OLD)).as("some samples are sealed").isPositive();

    database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW);

    assertThat(filesNaming(OLD)).as("no file is left under the old name, in the session of the rename").isEmpty();
    insert(NEW, 11_000L, "h2", 200);
    assertThat(count(NEW)).isEqualTo(8);

    reopenDatabase();

    assertThat(count(NEW)).isEqualTo(8);
    assertThat(sealedBlocks(NEW)).as("the sealed blocks are found under the new name").isPositive();
    assertThat(hosts(NEW)).containsExactlyInAnyOrder("h0", "h1", "h2");
    assertThat(sumOfV(NEW)).isEqualTo(0 + 1 + 2 + 3 + 4 + 5 + 100 + 200);
    assertThat(filesNaming(OLD)).isEmpty();

    // and back: the files follow the name whichever way it goes
    database.command("sql", "ALTER TYPE " + NEW + " NAME " + OLD);
    insert(OLD, 12_000L, "h0", 300);
    reopenDatabase();
    assertThat(count(OLD)).isEqualTo(9);
    assertThat(filesNaming(NEW)).isEmpty();

    // a type created under the name the first one left takes none of its files
    database.command("sql",
        "CREATE TIMESERIES TYPE " + NEW + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
    assertThat(count(NEW)).isZero();
    assertThat(count(OLD)).isEqualTo(9);

    database.command("sql", "DROP TYPE " + OLD);
    database.command("sql", "DROP TYPE " + NEW);
    assertThat(filesNaming(OLD)).as("the drop of a renamed type deletes every file it owns").isEmpty();
    assertThat(filesNaming(NEW)).isEmpty();
  }

  /**
   * Under HA the session that publishes a rename ships the renames it recorded, and a follower moves the same files. The engine's
   * components (the mutable bucket of each shard and the tag dictionary) are recorded with the type's buckets and index files.
   */
  @Test
  void theRenameRecordsTheComponentsOfTheEngine() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
    insert(OLD, 1000L, "h1", 1);
    final TimeSeriesEngine engine = engineOf(OLD);
    final List<Integer> engineFiles = new ArrayList<>();
    for (int i = 0; i < engine.getShardCount(); i++)
      engineFiles.add(engine.getShard(i).getMutableBucket().getFileId());
    engineFiles.add(engine.getTagDictionary().getFileId());

    final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
    final Map<Integer, String> recorded;
    assertThat(fileManager.startRecordingChanges()).isTrue();
    try {
      database.getSchema().getType(OLD).rename(NEW);
      recorded = fileManager.getRecordedRenames();
    } finally {
      fileManager.stopRecordingChanges();
    }

    for (final Integer fileId : engineFiles)
      assertThat(recorded).as("engine file %d", fileId).containsEntry(fileId, fileManager.getFile(fileId).getComponentName());
    assertThat(recorded.values()).allMatch(name -> name.startsWith(NEW + "_"));
  }

  /**
   * A follower moves the files a rename recorded through the component of each, by file id, before it loads the schema that names
   * them: the sealed store is not a component, so the move of the mutable bucket it belongs to has to carry it, or the follower's
   * engine opens an empty sealed store under the new name and every sealed sample of the type is lost there.
   */
  @Test
  void movingAShardBucketByItsComponentCarriesItsSealedStore() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    for (int i = 0; i < 4; i++)
      insert(OLD, 1000L * (i + 1), "h1", i);
    engineOf(OLD).compactAll();

    final TimeSeriesShard shard = engineOf(OLD).getShard(0);
    final TimeSeriesBucket bucket = shard.getMutableBucket();
    final int blocks = shard.getSealedStore().getBlockCount();
    assertThat(blocks).isPositive();

    final File databaseDirectory = new File(database.getDatabasePath());
    bucket.rename(NEW + "_shard_0");

    assertThat(new File(databaseDirectory, OLD + "_shard_0.ts.sealed")).doesNotExist();
    assertThat(new File(databaseDirectory, NEW + "_shard_0.ts.sealed")).exists();
    // the live store follows its file: the samples are still read
    assertThat(shard.getSealedStore().getBlockCount()).isEqualTo(blocks);
    assertThat(shard.countSamples()).isEqualTo(4);

    bucket.rename(OLD + "_shard_0");
    assertThat(new File(databaseDirectory, OLD + "_shard_0.ts.sealed")).exists();
    assertThat(count(OLD)).isEqualTo(4);
  }

  /**
   * A type whose engine could not start (issue #6356) is still renamed with its files: they are moved through their components,
   * by the names the engine would look them up by, so the repair that brings the engine back finds the samples under the new name.
   */
  @Test
  void aTypeWhoseEngineCouldNotStartIsRenamedWithItsFiles() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    for (int i = 0; i < 5; i++)
      insert(OLD, 1000L * (i + 1), "h" + i, i);
    engineOf(OLD).compactAll();
    insert(OLD, 9000L, "h1", 9);

    final File directory = new File(database.getDatabasePath());
    final Path oldSealed = new File(directory, OLD + "_shard_0.ts.sealed").toPath();
    database.close();
    final byte[] goodSealed = Files.readAllBytes(oldSealed);
    final byte[] garbage = new byte[goodSealed.length];
    Arrays.fill(garbage, (byte) 0x7F);
    Files.write(oldSealed, garbage);
    database = factory.open();

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(OLD);
    assertThat(broken.isEngineAvailable()).as("the engine must have failed to open, or this test proves nothing").isFalse();

    database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW);
    assertThat(filesNaming(OLD)).isEmpty();
    assertThat(new File(directory, NEW + "_shard_0.ts.sealed")).exists();

    // the repair, as the HA path installs a good sealed file: under the name the type now has
    Files.write(new File(directory, NEW + "_shard_0.ts.sealed").toPath(), goodSealed);
    broken.initEngine();
    assertThat(count(NEW)).isEqualTo(6);

    reopenDatabase();
    assertThat(count(NEW)).isEqualTo(6);
    assertThat(filesNaming(OLD)).isEmpty();
  }

  @Test
  void aRenameOntoTheFilesOfAnotherTypeIsRefusedBeforeAnythingMoves() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    insert(OLD, 1000L, "h1", 1);
    // a sealed file left in the directory under the target name, by nothing this database still knows
    final File stray = new File(database.getDatabasePath(), NEW + "_shard_0.ts.sealed");
    Files.write(stray.toPath(), new byte[] { 1, 2, 3 });

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW)).isInstanceOf(SchemaException.class)
        .hasMessageContaining(stray.getName()).hasMessageContaining("Nothing was changed");

    assertThat(database.getSchema().existsType(OLD)).isTrue();
    assertThat(database.getSchema().existsType(NEW)).isFalse();
    assertThat(filesNaming(OLD)).isNotEmpty();
    assertThat(count(OLD)).isEqualTo(1);
    Files.delete(stray.toPath());
  }

  /**
   * The sealed file cannot move (a file already holds the target name): the component is moved back, the sealed file stays where it
   * was and the open store goes on reading it there.
   */
  @Test
  void aSealedFileThatCannotMoveTakesTheBucketBack() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    for (int i = 0; i < 4; i++)
      insert(OLD, 1000L * (i + 1), "h1", i);
    engineOf(OLD).compactAll();
    final TimeSeriesShard shard = engineOf(OLD).getShard(0);
    final File stray = new File(database.getDatabasePath(), NEW + "_shard_0.ts.sealed");
    Files.write(stray.toPath(), new byte[] { 1, 2, 3 });

    assertThatThrownBy(() -> shard.getMutableBucket().rename(NEW + "_shard_0")).isInstanceOf(IOException.class)
        .hasMessageContaining("already exists");

    assertThat(shard.getMutableBucket().getName()).isEqualTo(OLD + "_shard_0");
    assertThat(new File(database.getDatabasePath(), OLD + "_shard_0.ts.sealed")).exists();
    assertThat(shard.countSamples()).isEqualTo(4);
    assertThat(count(OLD)).isEqualTo(4);
    Files.delete(stray.toPath());
  }

  /**
   * The engine stays open through a rename: samples appended, compactions sealing them and reads of them while the files move go on,
   * and every sample is there, after a reopen too.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS) // a hang detector for the waits on the appender, not a latency bound
  void samplesAppendedWhileTheTypeIsRenamedAreKept() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
    final TimeSeriesEngine engine = engineOf(OLD);
    for (int i = 0; i < 50; i++)
      engine.appendSamples(new long[] { 1L + i }, new Object[] { "h" + (i % 3) }, new Object[] { (double) i });
    engine.compactAll();

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong appended = new AtomicLong(50);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread appender = new Thread(() -> {
      long ts = 10_000;
      try {
        while (!stop.get()) {
          engine.appendSamples(new long[] { ts }, new Object[] { "h" + (ts % 3) }, new Object[] { (double) ts });
          ++ts;
          final long total = appended.incrementAndGet();
          if (total % 20 == 0) {
            engine.compactAll();
            assertThat(engine.countSamples()).isEqualTo(total);
          }
        }
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    appender.start();
    try {
      while (appended.get() < 100 && failure.get() == null)
        Thread.onSpinWait();
      database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW);
      final long atRename = appended.get();
      while (appended.get() < atRename + 50 && failure.get() == null)
        Thread.onSpinWait();
    } finally {
      stop.set(true);
      appender.join();
    }

    assertThat(failure.get()).as("an append failed while the type was renamed").isNull();
    assertThat(count(NEW)).isEqualTo(appended.get());
    reopenDatabase();
    assertThat(count(NEW)).isEqualTo(appended.get());
    assertThat(filesNaming(OLD)).isEmpty();
  }

  @Test
  void theMaintenanceOfARenamedTypeFollowsItsName() {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) RETENTION 1 DAYS");
    final var scheduler = ((LocalSchema) database.getSchema()).getTimeSeriesMaintenanceScheduler();
    assertThat(scheduler.isScheduled(OLD)).isTrue();

    database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW);

    assertThat(scheduler.isScheduled(NEW)).isTrue();
    assertThat(scheduler.isScheduled(OLD)).isFalse();

    // a type created under the old name schedules its own maintenance, and leaves the renamed type's alone
    database.command("sql",
        "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE)");
    assertThat(scheduler.isScheduled(OLD)).isTrue();
    assertThat(scheduler.isScheduled(NEW)).isTrue();
  }

  private record Kind(Consumer<String> create, Consumer<String> write) {
  }

  /** The concrete {@link LocalDocumentType} classes of the schema package: every kind of type the schema can hold. */
  private static List<Class<?>> concreteTypeClasses() throws Exception {
    // the classes directory of the main code, not the first entry of the classpath holding the package: the tests share the package
    final URL url = LocalDocumentType.class.getProtectionDomain().getCodeSource().getLocation();
    if (!"file".equals(url.getProtocol()) || !new File(url.toURI()).isDirectory())
      fail("The schema classes are not in a directory of the classpath (" + url + "), so they cannot be listed");

    final List<Class<?>> classes = new ArrayList<>();
    final File packageDirectory = new File(new File(url.toURI()), LocalDocumentType.class.getPackageName().replace('.', File.separatorChar));
    final File[] files = packageDirectory.listFiles((dir, name) -> name.endsWith(".class") && !name.contains("$"));
    assertThat(files).isNotNull();
    for (final File file : files) {
      final Class<?> c = Class.forName(LocalDocumentType.class.getPackageName() + "." + file.getName().replace(".class", ""), false,
          LocalDocumentType.class.getClassLoader());
      if (LocalDocumentType.class.isAssignableFrom(c) && !Modifier.isAbstract(c.getModifiers()) && !c.isInterface())
        classes.add(c);
    }
    assertThat(classes).contains(LocalDocumentType.class, LocalTimeSeriesType.class);
    return classes;
  }

  /** The files of the database directory whose name starts with the type name, followed by a delimiter of a component name. */
  private List<String> filesNaming(final String typeName) {
    final List<String> names = new ArrayList<>();
    final File[] files = new File(database.getDatabasePath()).listFiles();
    if (files != null)
      for (final File file : files)
        if (file.getName().startsWith(typeName + "_") || file.getName().startsWith(typeName + "."))
          names.add(file.getName());
    return names;
  }

  private TimeSeriesEngine engineOf(final String typeName) {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private int sealedBlocks(final String typeName) {
    final TimeSeriesEngine engine = engineOf(typeName);
    int blocks = 0;
    for (int i = 0; i < engine.getShardCount(); i++)
      blocks += engine.getShard(i).getSealedStore().getBlockCount();
    return blocks;
  }

  private void insert(final String typeName, final long ts, final String host, final double v) {
    database.command("sql", "INSERT INTO " + typeName + " SET ts = ?, host = ?, v = ?", ts, host, v);
  }

  private long count(final String typeName) {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + typeName)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private List<String> hosts(final String typeName) {
    final List<String> hosts = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT DISTINCT(host) AS h FROM " + typeName)) {
      while (rs.hasNext())
        hosts.add(rs.next().getProperty("h"));
    }
    return hosts;
  }

  private double sumOfV(final String typeName) {
    try (final ResultSet rs = database.query("sql", "SELECT sum(v) AS s FROM " + typeName)) {
      return rs.next().<Number>getProperty("s").doubleValue();
    }
  }
}
