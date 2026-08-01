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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #5713: a HASH index accepted any page size, but {@link HashIndexBucket} addresses everything
 * inside a bucket page with 16-bit fields (the slot directory, {@code BUCKET_DATA_END}, {@code BUCKET_ENTRY_COUNT}).
 * Above 65536 bytes the data offsets truncate to 16 bits, the slots point at garbage and the overflow chain closes
 * into a loop, which surfaced as the cycle detector reporting "corrupted index" with a wrapped page number instead of
 * the invalid configuration it was.
 *
 * <p>The oversized value was reachable for every page size except one: {@code HashIndexFactoryHandler} treated a
 * requested page size of exactly {@link LSMTreeIndexAbstract#DEF_PAGE_SIZE} as "the caller did not specify one" (the
 * initial value of {@code IndexBuilder.pageSize}), so 262144 - the most natural oversized value to try - was the only
 * one silently clamped to something safe, and hid the defect.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5713HashIndexPageSizeTest extends TestHelper {
  private static final String TYPE_NAME = "Entry";

  /**
   * Enough entries to fill a 65536-byte bucket page to the top of its addressable range and split it: at ~13 bytes per
   * entry the first bucket reaches a {@code dataEnd} in the 65000s, which is exactly where the 16-bit fields used to
   * wrap. A smaller fixture would pass on an unfixed build.
   */
  private static final int ENTRIES = 20_000;

  /**
   * The reported case: 131072 was accepted at creation and corrupted the index on insert.
   */
  @Test
  void pageSizeAboveTheSixteenBitLimitIsRefused() {
    database.transaction(() -> {
      createType();

      assertThatThrownBy(() -> database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true)
          .withPageSize(131_072)
          .create())
          .isInstanceOf(IndexException.class)
          .hasMessageContaining("131072")
          .hasMessageContaining(String.valueOf(HashIndexBucket.MAX_PAGE_SIZE));

      // the refusal must happen before anything is written: no half-created index is left behind
      assertThat(database.getSchema().getIndexes()).isEmpty();
    });
  }

  /**
   * The LSM default is above the limit too, and it used to be the one value silently remapped to 65536. It is now
   * refused like any other oversized value, so "I asked for 262144 and got 65536" cannot happen unnoticed.
   */
  @Test
  void theLsmDefaultPageSizeIsNoLongerSilentlyRemapped() {
    database.transaction(() -> {
      createType();

      assertThatThrownBy(() -> database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true)
          .withPageSize(LSMTreeIndexAbstract.DEF_PAGE_SIZE)
          .create())
          .isInstanceOf(IndexException.class)
          .hasMessageContaining(String.valueOf(LSMTreeIndexAbstract.DEF_PAGE_SIZE));
    });
  }

  /**
   * A page size too small to even describe the index on the metadata page is refused as well.
   */
  @Test
  void pageSizeBelowTheMinimumIsRefused() {
    database.transaction(() -> {
      createType();

      assertThatThrownBy(() -> database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true)
          .withPageSize(HashIndexBucket.MIN_PAGE_SIZE - 1)
          .create())
          .isInstanceOf(IndexException.class)
          .hasMessageContaining(String.valueOf(HashIndexBucket.MIN_PAGE_SIZE));

      assertThat(database.getSchema().getIndexes()).isEmpty();
    });
  }

  /**
   * Not asking for a page size still yields the hash default, not LSM's.
   */
  @Test
  void anUnspecifiedPageSizeYieldsTheHashDefault() {
    database.transaction(() -> {
      createType();
      final Index index = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();

      assertThat(((IndexInternal) index).getPageSize()).isEqualTo(HashIndexBucket.DEF_PAGE_SIZE);
    });
  }

  /**
   * SQL has no PAGESIZE clause, and {@code CreateIndexStatement} used to pin the builder to the LSM default on every
   * statement. With "unset" now meaningful, that line had to go or every {@code UNIQUE_HASH} statement would be
   * refused by the new check.
   */
  @Test
  void sqlCreateIndexUsesTheHashDefaultPageSize() {
    database.transaction(() -> {
      createType();
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (k) UNIQUE_HASH");
    });

    final IndexInternal index = hashIndexOf(database);
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.HASH);
    assertThat(index.getPageSize()).isEqualTo(HashIndexBucket.DEF_PAGE_SIZE);

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument(TYPE_NAME);
      doc.set("k", "sql-key");
      doc.save();
    });
    assertThat(index.get(new Object[] { "sql-key" }).hasNext()).isTrue();
  }

  /**
   * Propagating an index to a sub type used to hardcode the LSM default as its page size, which the new check would
   * refuse outright. The inherited index must carry over the page size of the index it comes from.
   */
  @Test
  void anInheritedIndexKeepsThePageSizeOfTheIndexItComesFrom() {
    final int pageSize = 4_096;
    database.transaction(() -> {
      final DocumentType parent = database.getSchema().createDocumentType("Parent");
      parent.createProperty("k", Type.STRING);
      database.getSchema().buildTypeIndex("Parent", new String[] { "k" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true)
          .withPageSize(pageSize)
          .create();

      database.getSchema().createDocumentType("Child").addSuperType("Parent");
    });

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Child");
      doc.set("k", "child-key");
      doc.save();
    });

    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof HashIndex hashIndex)
        assertThat(hashIndex.getPageSize()).as("index '%s'", hashIndex.getName()).isEqualTo(pageSize);

    assertThat(database.getSchema().getType("Child").getAllIndexes(true)).isNotEmpty();
  }

  /**
   * The unset marker is a property of {@link IndexBuilder} itself, and it applies to every index type - not just HASH -
   * so it is asserted directly rather than only through what some index implementation makes of it. A non-positive
   * page size resolves to "unset"; before, it was passed straight through to the component, where the page arithmetic
   * divides by it.
   */
  @Test
  void aNonPositivePageSizeResolvesToUnset() {
    database.transaction(() -> {
      createType();
      final IndexBuilder<?> builder = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" });

      // never asked: each implementation gets to choose
      assertThat(builder.getPageSize()).isEqualTo(LSMTreeIndexAbstract.DEF_PAGE_SIZE);
      assertThat(builder.getPageSize(HashIndexBucket.DEF_PAGE_SIZE)).isEqualTo(HashIndexBucket.DEF_PAGE_SIZE);

      for (final int nonPositive : new int[] { 0, -1, IndexBuilder.PAGE_SIZE_UNSET }) {
        builder.withPageSize(nonPositive);
        assertThat(builder.getPageSize()).as("withPageSize(%d)", nonPositive)
            .isEqualTo(LSMTreeIndexAbstract.DEF_PAGE_SIZE);
        assertThat(builder.getPageSize(HashIndexBucket.DEF_PAGE_SIZE)).as("withPageSize(%d)", nonPositive)
            .isEqualTo(HashIndexBucket.DEF_PAGE_SIZE);
      }

      // an explicit positive value is answered verbatim by both getters, including the LSM default itself
      for (final int explicit : new int[] { 4_096, HashIndexBucket.DEF_PAGE_SIZE, LSMTreeIndexAbstract.DEF_PAGE_SIZE }) {
        builder.withPageSize(explicit);
        assertThat(builder.getPageSize()).as("withPageSize(%d)", explicit).isEqualTo(explicit);
        assertThat(builder.getPageSize(HashIndexBucket.DEF_PAGE_SIZE)).as("withPageSize(%d)", explicit).isEqualTo(explicit);
      }
    });
  }

  /**
   * The largest legal page size must still work end to end, and it is the value that used to be indistinguishable
   * from "unset" only because it happened to equal the default.
   */
  @Test
  @Tag("slow")
  void theLargestLegalPageSizeIsAcceptedAndUsable() {
    createIndexAndFill(HashIndexBucket.MAX_PAGE_SIZE);
  }

  /**
   * A page size smaller than the LSM default was always legal and must keep being honoured verbatim.
   */
  @Test
  @Tag("slow")
  void aSmallPageSizeIsHonouredVerbatim() {
    createIndexAndFill(4_096);
  }

  /**
   * The floor is not merely "does not throw": {@link HashIndexBucket#MIN_PAGE_SIZE} claims to leave a bucket page room
   * to host entries, so the boundary value has to survive splits and an overflow chain. At 256 bytes a bucket page has
   * 238 bytes for data plus slots, roughly 15 entries, so a few hundred keys exercise both.
   */
  @Test
  void theSmallestLegalPageSizeIsGenuinelyUsable() {
    final int entries = 300;
    database.transaction(() -> {
      createType();
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true)
          .withPageSize(HashIndexBucket.MIN_PAGE_SIZE)
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < entries; i++) {
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("k", "key-" + i);
        doc.save();
      }
    });

    final IndexInternal index = hashIndexOf(database);
    assertThat(index.getPageSize()).isEqualTo(HashIndexBucket.MIN_PAGE_SIZE);
    assertThat(index.countEntries()).isEqualTo(entries);
    // 300 entries at ~15 per page cannot fit without splitting well past the initial single bucket
    assertThat(((PaginatedComponent) index.getComponent()).getTotalPages()).isGreaterThan(20);
    assertThat(index.getStats().get("globalDepth")).as("the bucket must have split and doubled the directory")
        .isGreaterThan(0L);
    assertThat(index.checkIntegrity()).isEmpty();

    database.transaction(() -> {
      for (int i = 0; i < entries; i++)
        assertThat(index.get(new Object[] { "key-" + i }).hasNext()).as("key-%d not found", i).isTrue();
    });
  }

  /**
   * A database created before the creation-time check can still carry an index whose file declares an unaddressable
   * page size (the page size lives in the component file name). Opening it must not throw - that would make the whole
   * database unopenable for one bad index - but CHECK DATABASE has to name the page size, so the operator is not sent
   * chasing a "corrupted overflow chain" that is only the symptom.
   */
  @Test
  void anExistingIndexWithAnIllegalPageSizeIsReportedByCheckDatabase() throws Exception {
    final String dbName = "Issue5713ReopenDB";
    TestHelper.dropDatabase(dbName);

    final String databasePath;
    try (final Database db = TestHelper.createDatabase(dbName)) {
      databasePath = db.getDatabasePath();
      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType(TYPE_NAME);
        type.createProperty("k", Type.STRING);
        db.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" })
            .withType(Schema.INDEX_TYPE.HASH).withUnique(true)
            .withPageSize(HashIndexBucket.MAX_PAGE_SIZE)
            .create();
      });
    }

    final int illegalPageSize = HashIndexBucket.MAX_PAGE_SIZE * 2;
    declareIllegalPageSizeOnDisk(databasePath, illegalPageSize);

    try (final Database reopened = new DatabaseFactory(databasePath).open()) {
      final IndexInternal index = hashIndexOf(reopened);
      assertThat(index.getPageSize()).isEqualTo(illegalPageSize);
      assertThat(index.checkIntegrity())
          .anyMatch(problem -> problem.contains("unsupported page size=" + illegalPageSize));
    } finally {
      TestHelper.dropDatabase(dbName);
    }
  }

  /**
   * The corruption message tells the operator to rebuild the index, so a rebuild has to actually repair it. It nearly
   * did the opposite: {@code RebuildIndexStatement} DROPS the index and then recreates it carrying the old page size
   * over, so an unaddressable one would be refused by the new guard - deleting the index and failing to rebuild it.
   * The rebuild now falls back to the default page size, and the entries come back from the records.
   */
  @Test
  void rebuildRepairsAnIndexWithAnIllegalPageSizeInsteadOfDroppingIt() throws Exception {
    withLegacyIllegalPageSizeDatabase("Issue5713RebuildDB", 200, (db, entries) -> {
      db.command("sql", "REBUILD INDEX `" + hashIndexOf(db).getName() + "`");

      final IndexInternal rebuilt = hashIndexOf(db);
      assertThat(rebuilt.getPageSize()).as("the rebuild must pick a legal page size")
          .isEqualTo(HashIndexBucket.DEF_PAGE_SIZE);
      assertThat(rebuilt.checkIntegrity()).isEmpty();
      assertThat(rebuilt.countEntries()).isEqualTo(entries);

      db.transaction(() -> {
        for (int i = 0; i < entries; i++)
          assertThat(rebuilt.get(new Object[] { "key-" + i }).hasNext()).as("key-%d lost by the rebuild", i).isTrue();
      });
    });
  }

  /**
   * The two out-of-range directions are not the same condition and must not be reported as if they were. Above the
   * ceiling the 16-bit offsets have wrapped and the index really is damaged; below the floor nothing has wrapped - the
   * old creation path accepted any positive page size, so such an index may well have been working - and calling it
   * corrupted would send an operator hunting for damage that is not there.
   */
  @Test
  void anUndersizedLegacyIndexIsNotReportedAsCorrupted() throws Exception {
    withLegacyIllegalPageSizeDatabase("Issue5713UndersizedDB", 20, HashIndexBucket.MIN_PAGE_SIZE / 2, (db, entries) -> {
      final List<String> problems = hashIndexOf(db).checkIntegrity();

      assertThat(problems).anyMatch(p -> p.contains("unsupported page size=" + (HashIndexBucket.MIN_PAGE_SIZE / 2)));
      assertThat(problems).as("an undersized index has not wrapped anything, so it must not be called damaged")
          .noneMatch(p -> p.contains("damaged"));
      assertThat(problems).anyMatch(p -> p.contains("may still be working"));
    });

    withLegacyIllegalPageSizeDatabase("Issue5713OversizedDB", 20, HashIndexBucket.MAX_PAGE_SIZE * 2, (db, entries) -> {
      assertThat(hashIndexOf(db).checkIntegrity())
          .as("an oversized index HAS wrapped its offsets, so it must be called damaged")
          .anyMatch(p -> p.contains("damaged"));
    });
  }

  /**
   * {@code TRUNCATE TYPE} drops every index of the type and recreates it from a captured definition, so it has the same
   * drop-then-recreate shape as a rebuild: an unaddressable page size carried into the definition would leave the type
   * without the index it had.
   */
  @Test
  void truncateTypeRecreatesAnIndexWithAnIllegalPageSize() throws Exception {
    withLegacyIllegalPageSizeDatabase("Issue5713TruncateDB", 50, (db, entries) -> {
      // TRUNCATE TYPE scans the type, so it needs an active transaction
      db.transaction(() -> db.command("sql", "TRUNCATE TYPE " + TYPE_NAME));

      final IndexInternal recreated = hashIndexOf(db);
      assertThat(recreated.getPageSize()).as("TRUNCATE TYPE must recreate the index with a legal page size")
          .isEqualTo(HashIndexBucket.DEF_PAGE_SIZE);
      assertThat(recreated.checkIntegrity()).isEmpty();
      assertThat(recreated.countEntries()).isZero();

      // and the index is functional afterwards
      db.transaction(() -> {
        final MutableDocument doc = db.newDocument(TYPE_NAME);
        doc.set("k", "after-truncate");
        doc.save();
      });
      assertThat(recreated.get(new Object[] { "after-truncate" }).hasNext()).isTrue();
    });
  }

  /**
   * The worst of the drop-then-recreate sites: {@code CHECK DATABASE ... FIX} rebuilds every index it flagged, and an
   * unaddressable page size is now one of the things {@code checkIntegrity()} flags. Carrying that page size into the
   * rebuild would make the automated repair destroy precisely the index it had just diagnosed.
   */
  @Test
  void checkDatabaseFixRepairsAnIndexWithAnIllegalPageSize() throws Exception {
    withLegacyIllegalPageSizeDatabase("Issue5713CheckFixDB", 100, (db, entries) -> {
      db.command("sql", "CHECK DATABASE FIX");

      final IndexInternal repaired = hashIndexOf(db);
      assertThat(repaired.getPageSize()).as("CHECK DATABASE FIX must not leave the index it flagged behind")
          .isEqualTo(HashIndexBucket.DEF_PAGE_SIZE);
      assertThat(repaired.checkIntegrity()).isEmpty();

      db.transaction(() -> {
        for (int i = 0; i < entries; i++)
          assertThat(repaired.get(new Object[] { "key-" + i }).hasNext()).as("key-%d lost by CHECK DATABASE FIX", i)
              .isTrue();
      });
    });
  }

  @FunctionalInterface
  private interface LegacyDatabaseTest {
    void accept(Database database, int entries) throws Exception;
  }

  /**
   * Builds a database whose HASH index declares, on disk, a page size the bucket cannot address - the shape of an index
   * created before the creation-time guard - reopens it and hands it to the test.
   */
  private void withLegacyIllegalPageSizeDatabase(final String dbName, final int entries, final LegacyDatabaseTest test)
      throws Exception {
    withLegacyIllegalPageSizeDatabase(dbName, entries, HashIndexBucket.MAX_PAGE_SIZE * 2, test);
  }

  private void withLegacyIllegalPageSizeDatabase(final String dbName, final int entries, final int illegalPageSize,
      final LegacyDatabaseTest test) throws Exception {
    TestHelper.dropDatabase(dbName);

    final String databasePath;
    try (final Database db = TestHelper.createDatabase(dbName)) {
      databasePath = db.getDatabasePath();
      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType(TYPE_NAME);
        type.createProperty("k", Type.STRING);
        db.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" })
            .withType(Schema.INDEX_TYPE.HASH).withUnique(true)
            .withPageSize(HashIndexBucket.MAX_PAGE_SIZE)
            .create();
      });
      db.transaction(() -> {
        for (int i = 0; i < entries; i++) {
          final MutableDocument doc = db.newDocument(TYPE_NAME);
          doc.set("k", "key-" + i);
          doc.save();
        }
      });
    }

    declareIllegalPageSizeOnDisk(databasePath, illegalPageSize);

    try (final Database reopened = new DatabaseFactory(databasePath).open()) {
      assertThat(hashIndexOf(reopened).getPageSize()).isEqualTo(illegalPageSize);
      test.accept(reopened, entries);
    } finally {
      TestHelper.dropDatabase(dbName);
    }
  }

  /**
   * Rewrites the component file name so it declares a page size the bucket cannot address, exactly as an index created
   * by a version without the creation-time guard would look on disk. The page size lives in the file name, so this is
   * the only way to produce one now that creation refuses it.
   */
  private static void declareIllegalPageSizeOnDisk(final String databasePath, final int illegalPageSize) throws Exception {
    final File[] indexFiles = new File(databasePath).listFiles(
        (dir, fileName) -> fileName.endsWith("." + HashIndexBucket.UNIQUE_INDEX_EXT));
    assertThat(indexFiles).hasSize(1);

    final File mangled = new File(indexFiles[0].getPath()
        .replace("." + HashIndexBucket.MAX_PAGE_SIZE + ".", "." + illegalPageSize + "."));
    // fail here, not on an inscrutable assertion later, if the component-file naming scheme ever stops carrying the
    // page size as a dot-delimited segment
    assertThat(mangled).as("page size not found in the component file name '%s'", indexFiles[0].getName())
        .isNotEqualTo(indexFiles[0]);
    assertThat(indexFiles[0].renameTo(mangled)).isTrue();

    try (final RandomAccessFile raf = new RandomAccessFile(mangled, "rw")) {
      // round the file up to a whole number of the newly declared pages, so the page manager can read page 0
      raf.setLength(((raf.length() + illegalPageSize - 1) / illegalPageSize) * (long) illegalPageSize);
    }
  }

  /**
   * The schema index map holds both the {@code TypeIndex} wrapper and the bucket-level {@link HashIndex}, in no
   * particular order, so pick the one that actually owns the file.
   */
  private static IndexInternal hashIndexOf(final Database db) {
    for (final Index index : db.getSchema().getIndexes())
      if (index instanceof HashIndex hashIndex)
        return hashIndex;
    throw new AssertionError("no HASH index found in the schema");
  }

  private void createIndexAndFill(final int pageSize) {
    database.transaction(() -> {
      createType();
      final Index index = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true)
          .withPageSize(pageSize)
          .create();
      assertThat(((IndexInternal) index).getPageSize()).isEqualTo(pageSize);
    });

    database.transaction(() -> {
      for (int i = 0; i < ENTRIES; i++) {
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("k", "key-" + i);
        doc.save();
      }
    });

    final IndexInternal index = hashIndexOf(database);
    assertThat(index.getPageSize()).isEqualTo(pageSize);
    assertThat(index.countEntries()).isEqualTo(ENTRIES);

    database.transaction(() -> {
      for (int i = 0; i < ENTRIES; i++) {
        final IndexCursor cursor = index.get(new Object[] { "key-" + i });
        assertThat(cursor.hasNext()).as("key-%d not found", i).isTrue();
        assertThat(cursor.next().asDocument().getString("k")).isEqualTo("key-" + i);
      }
    });

    // the page size is carried by the component file name: it must survive a reopen, and the index must still resolve
    reopenDatabase();

    final IndexInternal reloaded = hashIndexOf(database);
    assertThat(reloaded.getPageSize()).isEqualTo(pageSize);
    assertThat(reloaded.countEntries()).isEqualTo(ENTRIES);
    assertThat(reloaded.checkIntegrity()).isEmpty();

    database.transaction(() -> {
      for (int i = 0; i < ENTRIES; i++)
        assertThat(reloaded.get(new Object[] { "key-" + i }).hasNext()).as("key-%d not found after reopen", i).isTrue();
    });
  }

  private void createType() {
    final DocumentType type = database.getSchema().getOrCreateDocumentType(TYPE_NAME);
    if (!type.existsProperty("k"))
      type.createProperty("k", Type.STRING);
  }
}
