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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11274: a sealed file recorded no owner, so the engine of a type adopted whatever file sat at the path it opens - through the
 * schema load above all, which no creation-time check reaches. A type now draws an owner id at creation, keeps it in the schema and in
 * the header of every sealed file of its shards, and refuses at open a file written for another type, another shard or another
 * database, leaving the type registered without an engine and the foreign file untouched.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11274SealedFileOwnerTest extends TestHelper {

  @Test
  void aSealedFileOfAnotherTypeAtThePathIsNotAdoptedAtOpen() throws IOException {
    createSealedType("A", 1.0);
    createSealedType("B", 2.0);
    final LocalTimeSeriesType a = (LocalTimeSeriesType) database.getSchema().getType("A");
    final LocalTimeSeriesType b = (LocalTimeSeriesType) database.getSchema().getType("B");
    assertThat(a.getSealedOwner()).isNotEqualTo(TimeSeriesSealedStore.NO_OWNER).isNotEqualTo(b.getSealedOwner());
    assertThat(b.getSealedOwner()).isNotEqualTo(TimeSeriesSealedStore.NO_OWNER);
    assertThat(values("B")).containsExactly(2.0, 2.0, 2.0);

    final Path directory = Path.of(database.getDatabasePath());
    final Path aFile = directory.resolve(TimeSeriesSealedStore.sealedFileNameFor("A", 0));
    final Path bFile = directory.resolve(TimeSeriesSealedStore.sealedFileNameFor("B", 0));
    assertThat(aFile).isRegularFile();
    assertThat(bFile).isRegularFile();
    final Path bOriginal = directory.resolve("B_shard_0.original");

    // the shape of a file restored by hand: A's sealed file copied over B's, the schema left as it is
    database.close();
    Files.move(bFile, bOriginal);
    Files.copy(aFile, bFile);
    final byte[] foreign = Files.readAllBytes(bFile);
    database = factory.open();

    final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType("B");
    assertThat(reopened.isEngineAvailable()).as("the engine of B must not adopt A's samples").isFalse();
    assertThat(reopened.getEngineUnavailableReason()).contains("is not read").contains("not for this shard");
    final Throwable refused = catchThrowable(() -> values("B"));
    assertThat(refused).isNotNull();
    assertThat(refused.getMessage()).contains("has no storage engine available");
    // A is untouched, and so is the foreign file: nothing was read from it or written to it
    assertThat(values("A")).containsExactly(1.0, 1.0, 1.0);
    database.close();
    assertThat(Files.readAllBytes(bFile)).isEqualTo(foreign);

    // B's own file back: B reads its own samples again
    Files.move(bOriginal, bFile, StandardCopyOption.REPLACE_EXISTING);
    database = factory.open();
    assertThat(((LocalTimeSeriesType) database.getSchema().getType("B")).isEngineAvailable()).isTrue();
    assertThat(values("B")).containsExactly(2.0, 2.0, 2.0);
  }

  @Test
  void theOwnerIsKeptInTheSchemaAndAcrossARename() {
    createSealedType("R", 3.0);
    final long owner = ((LocalTimeSeriesType) database.getSchema().getType("R")).getSealedOwner();
    assertThat(owner).isNotEqualTo(TimeSeriesSealedStore.NO_OWNER);

    database.command("sql", "ALTER TYPE R NAME R2");
    assertThat(((LocalTimeSeriesType) database.getSchema().getType("R2")).getSealedOwner()).isEqualTo(owner);
    reopenDatabase();

    final LocalTimeSeriesType renamed = (LocalTimeSeriesType) database.getSchema().getType("R2");
    assertThat(renamed.getSealedOwner()).isEqualTo(owner);
    assertThat(renamed.isEngineAvailable()).isTrue();
    assertThat(values("R2")).containsExactly(3.0, 3.0, 3.0);

    // and a further rewrite of the whole file keeps it: the samples are still read after a compaction and a reopen
    database.command("sql", "INSERT INTO R2 SET ts = 10000, value = 4.0");
    database.command("sql", "COMPACT TIMESERIES TYPE R2");
    reopenDatabase();
    assertThat(values("R2")).containsExactly(3.0, 3.0, 3.0, 4.0);
  }

  /**
   * A schema written before the owner existed has no {@code sealedOwner}: the type opens with no owner and reads its files as
   * before, whatever they record, and saving the schema again does not invent one.
   */
  @Test
  void aSchemaWithoutTheOwnerOpensItsTypeAsBefore() throws IOException {
    createSealedType("L", 5.0);
    final Path schemaFile = Path.of(database.getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
    database.close();

    final JSONObject schema = new JSONObject(Files.readString(schemaFile));
    final JSONObject type = schema.getJSONObject("types").getJSONObject("L");
    assertThat(type.has("sealedOwner")).isTrue();
    type.remove("sealedOwner");
    Files.writeString(schemaFile, schema.toString());

    database = factory.open();
    final LocalTimeSeriesType legacy = (LocalTimeSeriesType) database.getSchema().getType("L");
    assertThat(legacy.getSealedOwner()).isEqualTo(TimeSeriesSealedStore.NO_OWNER);
    assertThat(legacy.isEngineAvailable()).isTrue();
    assertThat(values("L")).containsExactly(5.0, 5.0, 5.0);

    // a schema change saves the schema: still no owner, and the samples are still read after a reopen
    database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
    reopenDatabase();
    final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType("L");
    assertThat(reopened.getSealedOwner()).isEqualTo(TimeSeriesSealedStore.NO_OWNER);
    assertThat(values("L")).containsExactly(5.0, 5.0, 5.0);
    assertThat(new JSONObject(Files.readString(schemaFile)).getJSONObject("types").getJSONObject("L").has("sealedOwner")).isFalse();
  }

  @Test
  void aStoreRefusesTheFileAndTheImageOfAnotherOwner() throws IOException {
    final String directory = database.getDatabasePath() + File.separator + "raw11274";
    new File(directory).mkdirs();
    final String path = directory + File.separator + "store";
    final List<ColumnDefinition> columns = List.of(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
    final long first = TimeSeriesSealedStore.newOwner();
    final long second = first + 1_000;

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns, 1 << 20, first)) {
      appendBlock(store, 1_000L, 1.0);
      store.flushHeader();
    }
    try (final RandomAccessFile raf = new RandomAccessFile(path + TimeSeriesSealedStore.FILE_EXTENSION, "r")) {
      raf.seek(4);
      assertThat(raf.readByte()).isEqualTo((byte) TimeSeriesSealedStore.CURRENT_VERSION);
      raf.seek(27);
      assertThat(raf.readLong()).as("the owner, after the 27 bytes every version shares").isEqualTo(first);
    }

    // another owner refuses it, at open
    final Throwable refused = catchThrowable(() -> new TimeSeriesSealedStore(path, columns, 1 << 20, second).close());
    assertThat(refused).isInstanceOf(IOException.class).hasMessageContaining(Long.toHexString(first))
        .hasMessageContaining(Long.toHexString(second));
    // its own owner, and a store told none, read it
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns, 1 << 20, first)) {
      assertThat(store.getBlockCount()).isEqualTo(1);
    }
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(1);
    }

    // an image written for another owner is refused before it replaces the file in place, which stays readable
    final String otherPath = directory + File.separator + "other";
    try (final TimeSeriesSealedStore other = new TimeSeriesSealedStore(otherPath, columns, 1 << 20, second)) {
      appendBlock(other, 5_000L, 9.0);
      appendBlock(other, 6_000L, 9.0);
      other.flushHeader();
    }
    final File image = new File(directory, "image.incoming");
    Files.copy(Path.of(otherPath + TimeSeriesSealedStore.FILE_EXTENSION), image.toPath());
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns, 1 << 20, first)) {
      final Throwable install = catchThrowable(() -> store.installSealedFile(image));
      assertThat(install).isInstanceOf(IOException.class).hasMessageContaining("is not read");
      assertThat(image).as("the image is left to the caller").exists();
      assertThat(store.getBlockCount()).isEqualTo(1);
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }

    // the image itself is whole: a store told no owner reads it
    try (final TimeSeriesSealedStore anyOwner = new TimeSeriesSealedStore(otherPath, columns)) {
      assertThat(anyOwner.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
      assertThat(anyOwner.getBlockCount()).isEqualTo(2);
    }
    FileUtils.deleteRecursively(new File(directory));
  }

  @Test
  void aLegacyFileHeaderIsRewrittenInPlaceInItsOwnLayout() throws IOException {
    final String directory = database.getDatabasePath() + File.separator + "legacy11274";
    new File(directory).mkdirs();
    final String path = directory + File.separator + "store";
    final List<ColumnDefinition> columns = List.of(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    // a version-3 file: the 27-byte header, its first block right after it
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      appendBlock(store, 1_000L, 1.0);
      store.flushHeader();
    }
    downgradeToVersion3(path + TimeSeriesSealedStore.FILE_EXTENSION);

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(1);
      appendBlock(store, 2_000L, 2.0);
      store.flushHeader();
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }
    try (final RandomAccessFile raf = new RandomAccessFile(path + TimeSeriesSealedStore.FILE_EXTENSION, "r")) {
      raf.seek(4);
      assertThat(raf.readByte()).as("an in-place header rewrite keeps the 27-byte layout").isEqualTo((byte) 3);
    }
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(2);
      // a rewrite of the whole file writes the current layout
      store.truncateBefore(1_500L);
      assertThat(store.getBlockCount()).isEqualTo(1);
    }
    try (final RandomAccessFile raf = new RandomAccessFile(path + TimeSeriesSealedStore.FILE_EXTENSION, "r")) {
      raf.seek(4);
      assertThat(raf.readByte()).isEqualTo((byte) TimeSeriesSealedStore.CURRENT_VERSION);
    }
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(1);
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }

    // a store told an owner refuses a file older than owners: its type wrote every file of the shard with one
    final Throwable refused = catchThrowable(() -> {
      downgradeToVersion3(path + TimeSeriesSealedStore.FILE_EXTENSION);
      new TimeSeriesSealedStore(path, columns, 1 << 20, TimeSeriesSealedStore.newOwner()).close();
    });
    assertThat(refused).isInstanceOf(IOException.class).hasMessageContaining("records no owner");
    FileUtils.deleteRecursively(new File(directory));
  }

  /** Rewrites a version-4 file as the version-3 file holding the same blocks: the owner taken out, version byte 3. */
  private static void downgradeToVersion3(final String file) throws IOException {
    final byte[] bytes = Files.readAllBytes(Path.of(file));
    final byte[] legacy = new byte[bytes.length - 8];
    System.arraycopy(bytes, 0, legacy, 0, 27);
    System.arraycopy(bytes, 35, legacy, 27, bytes.length - 35);
    legacy[4] = 3;
    Files.write(Path.of(file), legacy);
  }

  private static void appendBlock(final TimeSeriesSealedStore store, final long ts, final double value) throws IOException {
    store.appendBlock(1, ts, ts, new byte[][] { DeltaOfDeltaCodec.encode(new long[] { ts }), GorillaXORCodec.encode(new double[] { value }) },
        new double[] { Double.NaN, value }, new double[] { Double.NaN, value }, new double[] { Double.NaN, value }, new long[] { 0, 1 }, null);
  }

  private void createSealedType(final String name, final double value) {
    database.command("sql", "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");
    for (int i = 0; i < 3; i++)
      database.command("sql", "INSERT INTO " + name + " SET ts = ?, value = ?", 1000L * (i + 1), value);
    database.command("sql", "COMPACT TIMESERIES TYPE " + name);
  }

  private List<Double> values(final String type) {
    final List<Double> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT value FROM " + type + " ORDER BY ts")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(((Number) row.getProperty("value")).doubleValue());
      }
    }
    return values;
  }
}
