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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.block.BlockCorruptionException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnarStoreTest {
  private static final int MAX_RECORDS_IN_PAGE = 128;

  @TempDir
  Path dir;

  private static List<String> namesIn(final Path directory) throws IOException {
    try (final Stream<Path> entries = Files.list(directory)) {
      return entries.map(p -> p.getFileName().toString()).sorted().toList();
    }
  }

  private static void touch(final Path directory, final String... names) throws IOException {
    for (final String name : names)
      Files.write(directory.resolve(name), new byte[] { 1, 2, 3 });
  }

  @Test
  void projectionDirIsInvisibleToFileManager() throws IOException {
    final String databasePath = "./target/databases/" + getClass().getSimpleName() + "-invisible";
    Database database = TestHelper.createDatabase(databasePath);
    try {
      database.getSchema().createDocumentType("Asset");
      final Path root = Path.of(database.getDatabasePath());
      final int registered = registeredFiles(database).size();
      database.close();

      // files whose extensions FileManager registers when they sit in the database directory itself
      final Path projection = Files.createDirectories(ColumnarStore.bucketDir(root, 3));
      touch(projection.getParent(), "Ghost_0.90.65536.v0.bucket");
      touch(projection, "Ghost_1.91.65536.v0.bucket", "Ghost_2.92.65536.v0.umtidx", SegmentFormat.segmentFileName(1),
          ColumnarManifest.FILE_NAME);

      database = new DatabaseFactory(databasePath).open();
      final List<String> files = registeredFiles(database);
      assertThat(files).hasSize(registered);
      assertThat(files).noneMatch(path -> path.contains(ColumnarStore.ROOT_DIR + "/") || path.contains("Ghost"));
    } finally {
      // never drop() a closed instance: its exception would mask the test's own failure
      if (database.isOpen())
        database.drop();
      else
        FileUtils.deleteRecursively(new File(databasePath));
    }
  }

  private static List<String> registeredFiles(final Database database) {
    final List<String> paths = new ArrayList<>();
    for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles())
      if (file != null)
        paths.add(file.getFilePath().replace('\\', '/'));
    return paths;
  }

  @Test
  void bucketDirIsKeyedByFileIdOnly() throws IOException {
    final Path databasePath = Path.of("/data/db");
    assertThat(ColumnarStore.bucketDir(databasePath, 7)).isEqualTo(databasePath.resolve("columnar").resolve("b7"));

    final Database database = TestHelper.createDatabase("./target/databases/" + getClass().getSimpleName() + "-rename");
    try {
      final DocumentType type = database.getSchema().buildDocumentType().withName("Asset").withTotalBuckets(1).create();
      type.createProperty("brand", Type.STRING);
      final int bucketFileId = type.getBuckets(false).getFirst().getFileId();
      final String bucketName = type.getBuckets(false).getFirst().getName();
      final Path root = Path.of(database.getDatabasePath());
      final Path bucketDir = ColumnarStore.createBucketDir(root, bucketFileId);
      final ColumnarManifest manifest = new ColumnarManifest(1, bucketFileId, bucketName, MAX_RECORDS_IN_PAGE, new ColumnDefinition[0],
          List.of(), List.of(), null, null, 0);
      manifest.publish(bucketDir);

      database.command("sql", "ALTER TYPE Asset NAME Item");
      assertThat(database.getSchema().existsType("Asset")).isFalse();
      // the bucket keeps its file id through the rename, so the projection directory is the same one
      assertThat(database.getSchema().getBucketById(bucketFileId)).isNotNull();
      final ColumnarManifest loaded = ColumnarManifest.load(ColumnarStore.bucketDir(root, bucketFileId));
      assertThat(loaded.bucketName()).isEqualTo(bucketName).isEqualTo("Asset_0");
      assertThat(loaded.bucketFileId()).isEqualTo(bucketFileId);
    } finally {
      database.drop();
    }
  }

  @Test
  void unreferencedFilesAreDeletedButHeldOnesStay() throws IOException {
    final String live = SegmentFormat.segmentFileName(5);
    final String held = SegmentFormat.segmentFileName(4);
    final String heldDead = SegmentFormat.deadFileName(4, 2);
    final String orphan = SegmentFormat.segmentFileName(3);
    final String oldDictionary = "dict-11.0.dic";
    final String dictionary = "dict-11.1.dic";
    touch(dir, live, held, heldDead, orphan, oldDictionary, dictionary, ColumnarManifest.FILE_NAME, ColumnarStore.NOWAL_MARKER,
        ColumnarManifest.FILE_NAME + ColumnarStore.TMP_SUFFIX);
    Files.createDirectory(dir.resolve("sub"));

    final List<String> deleted = ColumnarStore.deleteUnreferenced(dir, Set.of(live, dictionary), Set.of(held, heldDead));
    assertThat(deleted).containsExactlyInAnyOrder(orphan, oldDictionary);
    assertThat(namesIn(dir)).containsExactlyInAnyOrder(live, held, heldDead, dictionary, ColumnarManifest.FILE_NAME,
        ColumnarStore.NOWAL_MARKER, ColumnarManifest.FILE_NAME + ColumnarStore.TMP_SUFFIX, "sub");

    // the readers let go: the held files go too, the markers never
    assertThat(ColumnarStore.deleteUnreferenced(dir, Set.of(live, dictionary), Set.of())).containsExactlyInAnyOrder(held, heldDead);
    assertThat(ColumnarStore.deleteUnreferenced(dir, Set.of(live, dictionary), Set.of())).isEmpty();
    assertThat(ColumnarStore.deleteUnreferenced(dir.resolve("absent"), Set.of(), Set.of())).isEmpty();
  }

  @Test
  void deleteUnreferencedReturnsDeletedNames() throws IOException {
    touch(dir, "a", "b", "c", ColumnarStore.NOWAL_MARKER, ColumnarManifest.FILE_NAME);
    assertThat(ColumnarStore.deleteUnreferenced(dir, Set.of("a"), Set.of("b"))).containsExactly("c");
    assertThat(namesIn(dir)).containsExactlyInAnyOrder("a", "b", ColumnarStore.NOWAL_MARKER, ColumnarManifest.FILE_NAME);
  }

  @Test
  void crashLeftPartialFilesWithTheNextIdDoNotBreakTheNextPublish() throws IOException {
    final ColumnDefinition[] columns = { new ColumnDefinition(11, "size", Type.LONG, ColumnKind.LONG, false, null) };
    final RowValues row = new RowValues(1);
    row.present[0] = true;
    row.longs[0] = 42;

    // generation 1 names segment 1 and dictionary generation 0
    final GlobalDictionary dictionary = GlobalDictionary.create(dir.resolve("dict-11.0.dic"));
    dictionary.getOrAdd("x");
    final GlobalDictionary.DurableState durable = dictionary.fsync();
    dictionary.close();
    final SegmentReader.Meta first = writeSegment(1, columns, row);
    final ColumnarManifest generation1 = new ColumnarManifest(1, 9, "Asset_0", MAX_RECORDS_IN_PAGE, columns,
        List.of(entry(first, 0, 0)), List.of(new ColumnarManifest.DictionaryEntry(11, 0, "dict-11.0.dic", durable.entries(),
        durable.bytes())), null, null, 0);
    generation1.publish(dir);

    // a crashed flush left a partial segment 2, its dead set and a new dictionary generation; ids restart from the manifest
    touch(dir, SegmentFormat.segmentFileName(2), SegmentFormat.deadFileName(2, 1), "dict-11.1.dic");
    assertThatThrownBy(() -> writeSegment(2, columns, row)).isInstanceOf(FileAlreadyExistsException.class);

    // the writable open removes them, so the next flush reuses the names
    final ColumnarManifest loaded = ColumnarManifest.load(dir);
    assertThat(ColumnarStore.deleteUnreferenced(dir, loaded.files(), Set.of())).containsExactlyInAnyOrder(
        SegmentFormat.segmentFileName(2), SegmentFormat.deadFileName(2, 1), "dict-11.1.dic");

    final SegmentReader.Meta second = writeSegment(2, columns, row);
    final PositionBitmap dead = new PositionBitmap(MAX_RECORDS_IN_PAGE);
    dead.set(3);
    DeadSetFile.write(dir.resolve(SegmentFormat.deadFileName(2, 1)), dead);
    final GlobalDictionary nextGeneration = GlobalDictionary.create(dir.resolve("dict-11.1.dic"));
    nextGeneration.getOrAdd("x");
    nextGeneration.getOrAdd("y");
    final GlobalDictionary.DurableState nextDurable = nextGeneration.fsync();
    nextGeneration.close();
    final ColumnarManifest generation2 = new ColumnarManifest(2, 9, "Asset_0", MAX_RECORDS_IN_PAGE, columns,
        List.of(entry(first, 0, 0), entry(second, 1, 1)), List.of(new ColumnarManifest.DictionaryEntry(11, 1, "dict-11.1.dic",
        nextDurable.entries(), nextDurable.bytes())), null, null, 0);
    generation2.publish(dir);

    final ColumnarManifest reloaded = ColumnarManifest.load(dir);
    assertThat(reloaded.generation()).isEqualTo(2);
    assertThat(reloaded.segments()).hasSize(2);
    try (final SegmentReader reader = SegmentReader.open(dir.resolve(reloaded.segments().get(1).file()),
        DeadSetFile.read(dir.resolve(reloaded.segments().get(1).deadFile()), MAX_RECORDS_IN_PAGE))) {
      assertThat(reader.live().cardinality()).isEqualTo(0);
    }
    final ColumnarManifest.DictionaryEntry dictionaryEntry = reloaded.dictionaries().getFirst();
    assertThat(dictionaryEntry.file()).isEqualTo("dict-11.1.dic");
    final GlobalDictionary reopened = GlobalDictionary.open(dir.resolve(dictionaryEntry.file()), dictionaryEntry.bytes(),
        dictionaryEntry.entries());
    assertThat(reopened.size()).isEqualTo(2);
    assertThat(reopened.valueOf(1)).isEqualTo("y");
    reopened.close();
    // generation 0 of the dictionary is no longer named: the next cleanup removes it
    assertThat(ColumnarStore.deleteUnreferenced(dir, reloaded.files(), Set.of())).containsExactly("dict-11.0.dic");
  }

  private SegmentReader.Meta writeSegment(final long segmentId, final ColumnDefinition[] columns, final RowValues row)
      throws IOException {
    try (final SegmentWriter writer = new SegmentWriter(dir.resolve(SegmentFormat.segmentFileName(segmentId)), segmentId,
        MAX_RECORDS_IN_PAGE, columns, 64)) {
      writer.addRow(3, 1, row);
      return writer.finish();
    }
  }

  private static ColumnarManifest.SegmentEntry entry(final SegmentReader.Meta meta, final int deadVersion, final long deadRows) {
    return new ColumnarManifest.SegmentEntry(meta.segmentId(), SegmentFormat.segmentFileName(meta.segmentId()), meta.rows(), deadVersion,
        deadRows, meta.minPage(), meta.maxPage());
  }

  @Test
  void snapshotInstallPreparationWipesAndMarks() throws IOException {
    final Path bucketDir = Files.createDirectories(ColumnarStore.bucketDir(dir, 4));
    touch(bucketDir, "x", ColumnarManifest.FILE_NAME);
    touch(bucketDir.getParent(), "x");
    assertThat(ColumnarStore.readRebuildReason(dir)).isNull();

    ColumnarStore.prepareForSnapshotInstall(dir);
    final Path root = dir.resolve(ColumnarStore.ROOT_DIR);
    assertThat(namesIn(root)).containsExactly(ColumnarStore.REBUILD_MARKER);
    assertThat(ColumnarStore.readRebuildReason(dir)).isEqualTo("snapshot_install");

    // idempotent, and works on a database that never had a projection
    ColumnarStore.prepareForSnapshotInstall(dir);
    assertThat(namesIn(root)).containsExactly(ColumnarStore.REBUILD_MARKER);
    final Path fresh = Files.createDirectory(dir.resolve("fresh"));
    ColumnarStore.prepareForSnapshotInstall(fresh);
    assertThat(ColumnarStore.readRebuildReason(fresh)).isEqualTo("snapshot_install");

    ColumnarStore.clearRebuildReason(dir);
    assertThat(ColumnarStore.readRebuildReason(dir)).isNull();
    assertThat(namesIn(root)).isEmpty();
    ColumnarStore.clearRebuildReason(dir);
  }

  @Test
  void isProjectionEntryMatchesOnlyTheRoot() {
    assertThat(ColumnarStore.isProjectionEntry("columnar")).isTrue();
    assertThat(ColumnarStore.isProjectionEntry("columnar/a")).isTrue();
    assertThat(ColumnarStore.isProjectionEntry("columnar/b7/seg-0000000000000001.acs")).isTrue();
    assertThat(ColumnarStore.isProjectionEntry("columnar\\a")).isTrue();
    assertThat(ColumnarStore.isProjectionEntry("columnar.bucket")).isFalse();
    assertThat(ColumnarStore.isProjectionEntry("columnarx/a")).isFalse();
    assertThat(ColumnarStore.isProjectionEntry("Asset_0.1.65536.v0.bucket")).isFalse();
    assertThat(ColumnarStore.isProjectionEntry("db/columnar/a")).isFalse();
    assertThat(ColumnarStore.isProjectionEntry("")).isFalse();
  }

  @Test
  void externalOwnerMapFileRoundTrips() {
    final ColumnarManifest with = ColumnarManifest.fromJSON(ColumnarManifestTest.sample(1, "ext.own").toJSON());
    assertThat(with.externalOwnerMapFile()).isEqualTo("ext.own");
    assertThat(with.files()).contains("ext.own");
    final ColumnarManifest without = ColumnarManifest.fromJSON(ColumnarManifestTest.sample(1, null).toJSON());
    assertThat(without.externalOwnerMapFile()).isNull();
    assertThat(without.toJSON().has("externalOwnerMapFile")).isFalse();
    assertThat(without.files()).noneMatch(Objects::isNull);
  }

  @Test
  void writeAtomicallyReplacesTheTarget() throws IOException {
    final Path target = dir.resolve("NOWAL");
    ColumnarStore.writeAtomically(target, new byte[] { 1 });
    ColumnarStore.writeAtomically(target, new byte[] { 2, 3 });
    assertThat(Files.readAllBytes(target)).containsExactly(2, 3);
    assertThat(namesIn(dir)).containsExactly("NOWAL");
  }

  @Test
  void deadSetRoundTrip() throws IOException {
    final PositionBitmap dead = new PositionBitmap(MAX_RECORDS_IN_PAGE);
    for (final long position : new long[] { 0, 63, 64, 127, 128, 5L * MAX_RECORDS_IN_PAGE + 77, 100_000L * MAX_RECORDS_IN_PAGE + 1 })
      dead.set(position);
    final Path file = dir.resolve(SegmentFormat.deadFileName(1, 1));
    DeadSetFile.write(file, dead);
    final PositionBitmap read = DeadSetFile.read(file, MAX_RECORDS_IN_PAGE);
    assertThat(read.pages()).containsExactly(dead.pages());
    assertThat(read.cardinality()).isEqualTo(7);
    assertThat(read.andCardinality(dead)).isEqualTo(7);
    assertThat(read.get(62)).isFalse();
    assertThat(read.get(5L * MAX_RECORDS_IN_PAGE + 76)).isFalse();

    final Path empty = dir.resolve(SegmentFormat.deadFileName(1, 2));
    DeadSetFile.write(empty, new PositionBitmap(MAX_RECORDS_IN_PAGE));
    assertThat(DeadSetFile.read(empty, MAX_RECORDS_IN_PAGE).isEmpty()).isTrue();

    // a dead version is written once: an existing name is refused
    assertThatThrownBy(() -> DeadSetFile.write(file, dead)).isInstanceOf(FileAlreadyExistsException.class);
  }

  @Test
  void deadSetCorruptionIsDetected() throws IOException {
    final PositionBitmap dead = new PositionBitmap(MAX_RECORDS_IN_PAGE);
    dead.set(5);
    dead.set(3L * MAX_RECORDS_IN_PAGE + 9);
    final Path file = dir.resolve("seg.dead.1");
    DeadSetFile.write(file, dead);
    final byte[] bytes = Files.readAllBytes(file);

    // every flipped byte, header or payload, is caught by a CRC or a format check
    for (int i = 0; i < bytes.length; i++) {
      final byte[] damaged = bytes.clone();
      damaged[i] ^= 0x10;
      final Path copy = dir.resolve("damaged." + i);
      Files.write(copy, damaged);
      assertThatThrownBy(() -> DeadSetFile.read(copy, MAX_RECORDS_IN_PAGE)).as("byte " + i).isInstanceOf(BlockCorruptionException.class);
    }
    for (final int length : new int[] { 0, 7, bytes.length - 1 }) {
      final Path truncated = dir.resolve("truncated." + length);
      Files.write(truncated, Arrays.copyOf(bytes, length));
      assertThatThrownBy(() -> DeadSetFile.read(truncated, MAX_RECORDS_IN_PAGE)).isInstanceOf(BlockCorruptionException.class);
    }
    final Path longer = dir.resolve("longer");
    Files.write(longer, Arrays.copyOf(bytes, bytes.length + 1));
    assertThatThrownBy(() -> DeadSetFile.read(longer, MAX_RECORDS_IN_PAGE)).isInstanceOf(BlockCorruptionException.class);
    // a dead set of another page size
    assertThatThrownBy(() -> DeadSetFile.read(file, 2 * MAX_RECORDS_IN_PAGE)).isInstanceOf(BlockCorruptionException.class);
  }
}
