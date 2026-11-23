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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12234: a build before 26.11.1 cannot decode the fixed-width float and double arrays (binary types 36 and 37) and,
 * opening a database that holds them, reads them as null and drops them on the record's next update. Every build since 26.8.1
 * refuses at open a dictionary whose file is labelled with a format version above 1, so a database that may hold the form
 * carries the label {@link Dictionary#FIXED_WIDTH_FLOAT_ARRAYS_VERSION}: such a build then refuses the database instead of
 * losing data.
 * <br>
 * The label follows the setting {@code arcadedb.serializer.fixedWidthFloatArrays}, not the values: it is there from the moment
 * a read-write database opens with the setting on, whatever the database goes on to write or read.
 */
class FixedWidthFloatArraysRollbackFenceTest extends TestHelper {
  private static final int DIM = 64;

  @BeforeEach
  void createType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("emb", Type.ARRAY_OF_FLOATS);
      database.getSchema().getType("Doc").createProperty("dbl", Type.ARRAY_OF_DOUBLES);
    });
  }

  /** The reproduction of the issue: a database this build writes fixed-width arrays into is one an older build refuses. */
  @Test
  void aNewDatabaseIsBornWithTheLabelBeforeItHoldsAnyArray() {
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);

    final float[] vector = vector(DIM, 1F);
    final RID rid = saveInTransaction(() -> database.newDocument("Doc").set("emb", vector).set("dbl", new double[] { 0.1, 0.2 }));
    assertThat(storedSize(rid)).as("the fixed-width form, 4 bytes per float").isLessThan(4 * DIM + 64);

    reopenDatabase();
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
    final Document read = database.lookupByRID(rid, true).asDocument();
    assertThat((float[]) read.get("emb")).containsExactly(vector);
    assertThat((double[]) read.get("dbl")).containsExactly(0.1, 0.2);

    // THE LABELLED DICTIONARY KEEPS WORKING: NEW NAMES, AND A REOPEN AFTER THEM
    database.transaction(() -> database.newDocument("Doc").set("anotherName", 1).set("emb", new float[] { 4F }).save());
    reopenDatabase();
    assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc WHERE anotherName = 1").nextIfAvailable().<Long>getProperty("c"))
        .isEqualTo(1L);
  }

  /**
   * A database written by 26.11.x holds the fixed-width form under a version 1 dictionary, as does a replica whose files came
   * from such a node. It is relabelled when it opens, before anything is read or written: a node that only applies replicated
   * pages, or only answers vector searches, never decodes a value through the serializer and must be fenced all the same.
   */
  @Test
  void aDatabaseWrittenBy26_11_xIsRelabelledWhenItOpens() {
    final RID rid = saveInTransaction(new float[] { 7F, 8F });

    closeAndLabelTheDictionaryAs26_11_1();
    assertThat(dictionaryFileVersion()).isEqualTo(1);
    database = factory.open();
    assertThat(dictionaryFileVersion()).as("relabelled by the open alone").isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);

    assertThat((float[]) database.lookupByRID(rid, true).asDocument().get("emb")).containsExactly(7F, 8F);
    reopenDatabase();
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
    assertThat((float[]) database.lookupByRID(rid, true).asDocument().get("emb")).containsExactly(7F, 8F);
  }

  /**
   * The label follows the setting, not the content: a database that holds no float or double array is relabelled by its first
   * read-write open too. Whether a database holds the form cannot be told without reading every record, and a database that
   * does not hold it yet can receive it at any moment, from a write or from a replicated page.
   */
  @Test
  void aDatabaseWithoutAnyArrayIsRelabelledWhenItOpens() {
    database.transaction(() -> database.newDocument("Doc").set("name", "plain").save());
    closeAndLabelTheDictionaryAs26_11_1();
    assertThat(dictionaryFileVersion()).isEqualTo(1);

    database = factory.open();
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
    assertThat(database.query("sql", "SELECT name FROM Doc").nextIfAvailable().<String>getProperty("name")).isEqualTo("plain");
  }

  /** The way to keep a rollback possible: with the setting off the label stays 1 and every array is written as a varint. */
  @Test
  void withTheSettingOffTheDictionaryStaysVersion1AndArraysStayVarint() {
    openAsADatabaseKeptOpenableByOlderBuilds();
    assertThat(dictionaryFileVersion()).isEqualTo(1);

    final float[] vector = vector(DIM, 1F);
    final RID rid = saveInTransaction(vector);
    assertThat(dictionaryFileVersion()).isEqualTo(1);
    assertThat(storedSize(rid)).as("the varint form, 5 bytes per float").isGreaterThanOrEqualTo(5 * DIM);

    reopenDatabase();
    assertThat(dictionaryFileVersion()).isEqualTo(1);
    assertThat((float[]) database.lookupByRID(rid, true).asDocument().get("emb")).containsExactly(vector);
  }

  /** Turning the setting on labels the database at once, before the first array written under it. */
  @Test
  void turningTheSettingOnRelabelsBeforeTheNextArrayIsWritten() {
    openAsADatabaseKeptOpenableByOlderBuilds();
    final float[] vector = vector(DIM, 1F);
    final RID varint = saveInTransaction(vector);

    database.command("sql", "ALTER DATABASE `arcadedb.serializer.fixedWidthFloatArrays` true");
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);

    final RID fixed = saveInTransaction(vector);
    assertThat(storedSize(fixed)).as("the fixed-width form once labelled").isLessThan(4 * DIM + 64);

    reopenDatabase();
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
    assertThat((float[]) database.lookupByRID(varint, true).asDocument().get("emb")).containsExactly(vector);
    assertThat((float[]) database.lookupByRID(fixed, true).asDocument().get("emb")).containsExactly(vector);
  }

  /** The label is never taken back: the arrays already stored are still in a form an older build cannot read. */
  @Test
  void turningTheSettingOffKeepsTheLabel() {
    final float[] vector = vector(DIM, 1F);
    final RID fixed = saveInTransaction(vector);

    database.command("sql", "ALTER DATABASE `arcadedb.serializer.fixedWidthFloatArrays` false");
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
    final RID varint = saveInTransaction(vector);
    assertThat(storedSize(varint)).as("the varint form with the setting off").isGreaterThanOrEqualTo(5 * DIM);

    reopenDatabase();
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
    assertThat((float[]) database.lookupByRID(fixed, true).asDocument().get("emb")).containsExactly(vector);
    assertThat((float[]) database.lookupByRID(varint, true).asDocument().get("emb")).containsExactly(vector);
  }

  /**
   * The fence never fails an open or a write: when the dictionary cannot be relabelled, arrays are written in the varint form
   * every build reads and the label stays 1, with no further attempt per value, until the database is reopened.
   */
  @Test
  void aRelabelThatFailsWritesTheVarintFormAndKeepsVersion1() {
    closeAndLabelTheDictionaryAs26_11_1();
    // A NON-EMPTY DIRECTORY UNDER THE NAME THE RELABEL MOVES THE FILE TO MAKES THE RENAME FAIL
    final File obstruction = new File(dictionaryFile().getParentFile(), dictionaryFile().getName().replaceFirst("\\.v1\\.", ".v2."));
    assertThat(obstruction.mkdir()).isTrue();
    assertThat(new File(obstruction, "occupied").mkdir()).isTrue();
    try {
      database = factory.open();
      assertThat(dictionaryFileVersion()).isEqualTo(1);

      final float[] vector = vector(DIM, 1F);
      final RID first = saveInTransaction(vector);
      assertThat(dictionaryFileVersion()).isEqualTo(1);
      assertThat(storedSize(first)).as("the varint form, 5 bytes per float").isGreaterThanOrEqualTo(5 * DIM);

      // THE OBSTRUCTION IS GONE, BUT A WRITE DOES NOT RETRY THE RENAME: STILL THE VARINT FORM, STILL VERSION 1
      FileUtils.deleteRecursively(obstruction);
      final RID second = saveInTransaction(vector);
      assertThat(dictionaryFileVersion()).isEqualTo(1);
      assertThat(storedSize(second)).as("the varint form until the next open").isGreaterThanOrEqualTo(5 * DIM);

      assertThat((float[]) database.lookupByRID(first, true).asDocument().get("emb")).containsExactly(vector);
      assertThat((float[]) database.lookupByRID(second, true).asDocument().get("emb")).containsExactly(vector);

      // A REOPEN TRIES AGAIN, AND NOW THE RELABEL SUCCEEDS BEFORE THE FIXED-WIDTH FORM IS WRITTEN
      reopenDatabase();
      assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
      final RID third = saveInTransaction(vector);
      assertThat(storedSize(third)).as("the fixed-width form once relabelled").isLessThan(4 * DIM + 64);
      assertThat((float[]) database.lookupByRID(first, true).asDocument().get("emb")).containsExactly(vector);
    } finally {
      FileUtils.deleteRecursively(obstruction);
    }
  }

  /** A read-only open never relabels its files, and still decodes the fixed-width form under a version 1 dictionary. */
  @Test
  void aReadOnlyOpenDecodesTheFixedWidthFormWithoutRelabelling() {
    final RID rid = saveInTransaction(new float[] { 7F, 8F });
    closeAndLabelTheDictionaryAs26_11_1();

    database = factory.open(ComponentFile.MODE.READ_ONLY);
    assertThat(dictionaryFileVersion()).isEqualTo(1);
    assertThat((float[]) database.lookupByRID(rid, true).asDocument().get("emb")).containsExactly(7F, 8F);
    assertThat((float[]) database.query("sql", "SELECT emb FROM Doc").nextIfAvailable().getProperty("emb")).containsExactly(7F, 8F);
    assertThat(dictionaryFileVersion()).isEqualTo(1);

    // BACK TO READ-WRITE, FOR THE INTEGRITY CHECK THAT RUNS AFTER EVERY TEST
    reopenDatabase();
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
  }

  /**
   * The relabel of a live database, with writers committing arrays and readers decoding the fixed-width form meanwhile: one
   * relabel, every value written and read back, and no error on either side.
   */
  @Test
  void aLiveRelabelUnderConcurrentWritesAndReads() throws Exception {
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newDocument("Doc").set("seed", i).set("emb", new float[] { i, -i }).save();
    });
    openAsADatabaseKeptOpenableByOlderBuilds();
    assertThat(dictionaryFileVersion()).isEqualTo(1);

    final int writers = 4;
    final int readers = 4;
    final int perWriter = 25;
    final CyclicBarrier start = new CyclicBarrier(writers + readers + 1);
    final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    final List<Thread> threads = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      final int writer = w;
      threads.add(new Thread(() -> {
        try {
          start.await(30, TimeUnit.SECONDS);
          for (int i = 0; i < perWriter; i++) {
            final int n = i;
            database.transaction(() -> database.newDocument("Doc").set("writer", writer).set("n", n)
                .set("emb", new float[] { writer, n }).set("dbl", new double[] { writer, n }).save(), false, 50);
          }
        } catch (final Throwable t) {
          errors.add(t);
        }
      }));
    }
    for (int r = 0; r < readers; r++)
      threads.add(new Thread(() -> {
        try {
          start.await(30, TimeUnit.SECONDS);
          for (int round = 0; round < 10; round++)
            try (final ResultSet rs = database.query("sql", "SELECT seed, emb FROM Doc WHERE seed IS NOT NULL")) {
              int seen = 0;
              while (rs.hasNext()) {
                final Result row = rs.next();
                final int seed = row.<Integer>getProperty("seed");
                assertThat(row.<float[]>getProperty("emb")).containsExactly(seed, -seed);
                seen++;
              }
              assertThat(seen).isEqualTo(50);
            }
        } catch (final Throwable t) {
          errors.add(t);
        }
      }));

    for (final Thread t : threads)
      t.start();
    start.await(30, TimeUnit.SECONDS);
    database.command("sql", "ALTER DATABASE `arcadedb.serializer.fixedWidthFloatArrays` true");
    for (final Thread t : threads)
      t.join(TimeUnit.MINUTES.toMillis(2));

    assertThat(threads).noneMatch(Thread::isAlive);
    assertThat(errors).isEmpty();
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);

    reopenDatabase();
    assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);
    try (final ResultSet rs = database.query("sql", "SELECT writer, n, emb, dbl FROM Doc WHERE writer IS NOT NULL")) {
      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        final int writer = row.<Integer>getProperty("writer");
        final int n = row.<Integer>getProperty("n");
        assertThat(row.<float[]>getProperty("emb")).containsExactly(writer, n);
        assertThat(row.<double[]>getProperty("dbl")).containsExactly(writer, n);
        count++;
      }
      assertThat(count).isEqualTo(writers * perWriter);
    }
  }

  /**
   * A record brings a new property name, and the database is relabelled right after: the name's dictionary page is committed,
   * and still queued for the flush, when the file is renamed. The rename used to report the file closed for an instant, the
   * flush thread dropped the page, and after a reopen the name was gone and its id was handed to the next new name, so every
   * record holding it decoded under the wrong name. Repeated because the page has to be in the flush queue at that instant.
   */
  @Test
  void aNameRegisteredRightBeforeALiveRelabelSurvivesAReopen() {
    for (int i = 0; i < 20; i++) {
      openAsADatabaseKeptOpenableByOlderBuilds();
      final String name = "fresh" + i;
      final int n = i;
      final RID rid = saveInTransaction(() -> database.newDocument("Doc").set(name, n).set("emb", new float[] { n }));
      database.command("sql", "ALTER DATABASE `arcadedb.serializer.fixedWidthFloatArrays` true");
      assertThat(dictionaryFileVersion()).isEqualTo(Dictionary.FIXED_WIDTH_FLOAT_ARRAYS_VERSION);

      reopenDatabase();
      // A NAME THE NEXT OPEN DID NOT FIND WOULD BE HANDED THE LOST ONE'S ID
      database.transaction(() -> database.newDocument("Doc").set("other" + n, n).save());
      final Document read = database.lookupByRID(rid, true).asDocument();
      assertThat(read.getPropertyNames()).as("iteration %d", i).contains(name).doesNotContain("other" + n);
      assertThat(read.<Integer>get(name)).isEqualTo(n);
      assertThat((float[]) read.get("emb")).containsExactly(n);
    }
  }

  /**
   * Nodes compare a dictionary relabelled by this fence under the label it is created with, which a build without the fence
   * still reports; a dictionary not relabelled, every other file, and a name that is not labelled, as they are.
   */
  @Test
  void comparableFileNameFoldsOnlyTheFenceLabelBack() {
    assertThat(Dictionary.comparableFileName("dictionary.0.65536.v2.dict")).isEqualTo("dictionary.0.65536.v1.dict");
    assertThat(Dictionary.comparableFileName("dictionary.0.65536.v12.dict")).isEqualTo("dictionary.0.65536.v1.dict");
    assertThat(Dictionary.comparableFileName("dictionary.0.65536.v1.dict")).isEqualTo("dictionary.0.65536.v1.dict");
    assertThat(Dictionary.comparableFileName("dictionary.0.65536.v0.dict")).isEqualTo("dictionary.0.65536.v0.dict");
    assertThat(Dictionary.comparableFileName("dictionary.0.65536.dict")).isEqualTo("dictionary.0.65536.dict");
    assertThat(Dictionary.comparableFileName("dictionary.0.65536.v.dict")).isEqualTo("dictionary.0.65536.v.dict");
    assertThat(Dictionary.comparableFileName("dictionary.0.65536.vx.dict")).isEqualTo("dictionary.0.65536.vx.dict");
    assertThat(Dictionary.comparableFileName("Doc_0.1.65536.v2.bucket")).isEqualTo("Doc_0.1.65536.v2.bucket");
    assertThat(Dictionary.comparableFileName(".dict")).isEqualTo(".dict");
    assertThat(Dictionary.comparableFileName(null)).isNull();
  }

  /** One node matches its own dictionary across a relabel whatever label it had: the name without the label. */
  @Test
  void unlabelledFileNameDropsOnlyADictionaryLabel() {
    assertThat(Dictionary.unlabelledFileName("dictionary.0.65536.v0.dict")).isEqualTo("dictionary.0.65536.dict");
    assertThat(Dictionary.unlabelledFileName("dictionary.0.65536.v1.dict")).isEqualTo("dictionary.0.65536.dict");
    assertThat(Dictionary.unlabelledFileName("dictionary.0.65536.v2.dict")).isEqualTo("dictionary.0.65536.dict");
    assertThat(Dictionary.unlabelledFileName("dictionary.0.65536.dict")).isEqualTo("dictionary.0.65536.dict");
    assertThat(Dictionary.unlabelledFileName("dictionary.0.65536.vx.dict")).isEqualTo("dictionary.0.65536.vx.dict");
    assertThat(Dictionary.unlabelledFileName("Doc_0.1.65536.v0.bucket")).isEqualTo("Doc_0.1.65536.v0.bucket");
    assertThat(Dictionary.unlabelledFileName(null)).isNull();
  }

  private RID saveInTransaction(final float[] emb) {
    return saveInTransaction(() -> database.newDocument("Doc").set("emb", emb));
  }

  private RID saveInTransaction(final Supplier<MutableDocument> document) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = document.get().save().getIdentity());
    return rid[0];
  }

  private int storedSize(final RID rid) {
    return ((DatabaseInternal) database).getSchema().getBucketById(rid.getBucketId()).getRecord(rid).size();
  }

  private static float[] vector(final int dim, final float base) {
    final float[] v = new float[dim];
    for (int i = 0; i < dim; i++)
      v[i] = base + i * 0.25F;
    return v;
  }

  /**
   * Leaves the database open as one an operator kept openable by an older build: the setting turned off, and the dictionary
   * under the label 1 it had before this build first opened it.
   */
  private void openAsADatabaseKeptOpenableByOlderBuilds() {
    database.command("sql", "ALTER DATABASE `arcadedb.serializer.fixedWidthFloatArrays` false");
    closeAndLabelTheDictionaryAs26_11_1();
    database = factory.open();
  }

  /** Closes the database and puts the dictionary back under the label 26.11.1 left on files holding the fixed-width form. */
  private void closeAndLabelTheDictionaryAs26_11_1() {
    database.close();
    final File current = dictionaryFile();
    final File v1 = new File(current.getParentFile(), current.getName().replaceFirst("\\.v\\d+\\.", ".v1."));
    if (!current.equals(v1))
      assertThat(current.renameTo(v1)).isTrue();
  }

  private File dictionaryFile() {
    final File[] files = new File(getDatabasePath()).listFiles(file -> file.isFile() && file.getName().endsWith("." + Dictionary.DICT_EXT));
    assertThat(files).hasSize(1);
    return files[0];
  }

  /** The format version an opening build reads, which lives in the file name: {@code dictionary.<id>.<pageSize>.v<N>.dict}. */
  private int dictionaryFileVersion() {
    final String name = dictionaryFile().getName();
    final String prefix = name.substring(0, name.lastIndexOf('.'));
    return Integer.parseInt(prefix.substring(prefix.lastIndexOf('.') + 2));
  }
}
