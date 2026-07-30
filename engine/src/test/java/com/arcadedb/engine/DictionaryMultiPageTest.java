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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The dictionary used to live in page 0 alone, which capped a database at whatever names fitted in one page. These tests pin the
 * behaviour that replaces that cap: names roll over to a new page, ids stay put across the rollover and across a reopen, and a
 * database written before multi-page support (which is exactly a dictionary of one page) still reads byte for byte.
 * <p>
 * In package {@code com.arcadedb.engine} on purpose: verifying the on-page layout needs the component's file id and page size.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DictionaryMultiPageTest extends TestHelper {
  /**
   * Long enough that a few hundred names cross several pages, short enough to stay a plausible identifier length.
   */
  private static final int NAME_LENGTH = 500;

  private static String name(final int i) {
    final String prefix = "p" + i + "_";
    return prefix + "x".repeat(NAME_LENGTH - prefix.length());
  }

  private Dictionary dictionary() {
    return database.getSchema().getDictionary();
  }

  /**
   * Adds names until the dictionary spans at least {@code pages} pages, and returns how many it added.
   */
  private int fillPages(final int pages) {
    final Dictionary dictionary = dictionary();
    int added = 0;
    while (dictionary.getTotalPages() < pages) {
      dictionary.getIdByName(name(added), true);
      ++added;
    }
    return added;
  }

  private void assertAllNamesResolve(final int total) {
    final Dictionary dictionary = dictionary();
    for (int i = 0; i < total; ++i) {
      assertThat(dictionary.getIdByName(name(i), false)).as("id of entry %d", i).isEqualTo(i);
      assertThat(dictionary.getNameById(i)).as("name of id %d", i).isEqualTo(name(i));
    }
  }

  @Test
  void namesRollOverToNewPagesInsteadOfBeingRefused() {
    final Dictionary dictionary = dictionary();
    assertThat(dictionary.getTotalPages()).isEqualTo(1);

    final int added = fillPages(3);

    assertThat(dictionary.getTotalPages()).isGreaterThanOrEqualTo(3);
    // MORE NAMES THAN ONE PAGE COULD EVER HOLD, WHICH IS THE WHOLE POINT
    assertThat((long) added * NAME_LENGTH).isGreaterThan(dictionary.getPageSize());
    assertThat(dictionary.getDictionaryMap()).hasSize(added);
    assertAllNamesResolve(added);
  }

  @Test
  void idsSurviveAReopen() {
    final int added = fillPages(3);
    final Map<String, Integer> before = new LinkedHashMap<>(dictionary().getDictionaryMap());

    reopenDatabase();

    assertThat(dictionary().getDictionaryMap()).isEqualTo(before);
    assertAllNamesResolve(added);
    assertThat(dictionary().getTotalPages()).isGreaterThanOrEqualTo(3);
  }

  /**
   * The compatibility guarantee: page 0 is written exactly as a pre-multi-page ArcadeDB wrote it, so an existing database (whose
   * dictionary is one page) loads unchanged. Reads page 0 with the old single-page algorithm and checks it yields the first
   * names, in order, starting at id 0.
   */
  @Test
  void pageZeroKeepsTheLegacySinglePageLayout() {
    final int added = fillPages(2);
    final Dictionary dictionary = dictionary();

    final List<String> onPageZero = new ArrayList<>();
    database.transaction(() -> {
      try {
        final BasePage page = ((DatabaseInternal) database).getTransaction()
            .getPage(new PageId((DatabaseInternal) database, dictionary.getFileId(), 0), dictionary.getPageSize());
        // THE PRE-MULTI-PAGE READER: SKIP THE 4 BYTE LEGACY COUNTER, THEN READ STRINGS UP TO THE CONTENT SIZE
        page.setBufferPosition(Binary.INT_SERIALIZED_SIZE);
        while (page.getBufferPosition() < page.getContentSize())
          onPageZero.add(page.readString());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });

    assertThat(onPageZero).isNotEmpty();
    assertThat(onPageZero.size()).isLessThan(added);
    for (int i = 0; i < onPageZero.size(); ++i)
      assertThat(onPageZero.get(i)).as("legacy read of id %d", i).isEqualTo(name(i));
  }

  /**
   * A name is never split over two pages, so one that cannot fit an empty page can never be stored. That has to be said clearly
   * rather than surface as a page-boundary error, and it must leave the dictionary usable.
   */
  @Test
  void aNameTooBigForAnEmptyPageIsRejectedAndLeavesTheDictionaryUsable() {
    final Dictionary dictionary = dictionary();
    dictionary.getIdByName("before", true);

    final String tooBig = "y".repeat(dictionary.getPageSize());

    assertThatThrownBy(() -> dictionary.getIdByName(tooBig, true))
        .isInstanceOf(DatabaseMetadataException.class)
        .hasMessageContaining("cannot fit");

    assertThat(dictionary.getIdByName(tooBig, false)).isEqualTo(-1);
    assertThat(dictionary.getIdByName("before", false)).isNotEqualTo(-1);
    dictionary.getIdByName("after", true);
    assertThat(dictionary.getNameById(dictionary.getIdByName("after", false))).isEqualTo("after");
  }

  /**
   * Rollover has to survive the serializer round trip, not just the dictionary API: property names are stored as dictionary ids
   * inside every record.
   */
  @Test
  void documentsWithPropertyNamesSpanningPagesReadBack() {
    final int propertiesPerDocument = 20;
    final int documents = 30;

    final DocumentType type = database.getSchema().createDocumentType("Wide");
    assertThat(type).isNotNull();

    database.transaction(() -> {
      for (int d = 0; d < documents; ++d) {
        final MutableDocument doc = database.newDocument("Wide");
        doc.set("docId", d);
        for (int p = 0; p < propertiesPerDocument; ++p)
          doc.set(name(d * propertiesPerDocument + p), "v" + d + "_" + p);
        doc.save();
      }
    });

    assertThat(dictionary().getTotalPages()).isGreaterThan(1);

    reopenDatabase();

    database.transaction(() -> {
      database.scanType("Wide", true, record -> {
        final int d = record.asDocument().getInteger("docId");
        for (int p = 0; p < propertiesPerDocument; ++p)
          assertThat(record.asDocument().getString(name(d * propertiesPerDocument + p))).isEqualTo("v" + d + "_" + p);
        return true;
      });
    });
  }

  /**
   * updateName rewrites the whole dictionary. Across pages that means re-laying it out from page 0 and emptying whatever pages
   * the shorter content no longer reaches, otherwise a stale tail page would re-add its old names on the next reload.
   */
  @Test
  void updateNameRewritesEveryPage() {
    final int added = fillPages(3);
    final Dictionary dictionary = dictionary();
    final int renamedId = dictionary.getIdByName(name(1), false);

    database.transaction(() -> dictionary.updateName(name(1), "short"));

    assertThat(dictionary.getNameById(renamedId)).isEqualTo("short");
    assertThat(dictionary.getIdByName("short", false)).isEqualTo(renamedId);
    assertThat(dictionary.getIdByName(name(1), false)).isEqualTo(-1);

    // EVERY OTHER ENTRY KEPT ITS ID
    for (int i = 0; i < added; ++i)
      if (i != renamedId)
        assertThat(dictionary.getIdByName(name(i), false)).as("id of entry %d after the rename", i).isEqualTo(i);

    reopenDatabase();

    assertThat(dictionary().getDictionaryMap()).hasSize(added);
    assertThat(dictionary().getNameById(renamedId)).isEqualTo("short");
    for (int i = 0; i < added; ++i)
      if (i != renamedId)
        assertThat(dictionary().getIdByName(name(i), false)).as("id of entry %d after reopen", i).isEqualTo(i);
  }

  /**
   * The other half of the rewrite: renaming to a name that no longer fits the pages the dictionary already has must ADD one.
   * The shrink branch is covered above; this is the {@code pageNumber >= getTotalPages()} branch of resetPageForRewrite.
   */
  @Test
  void updateNameGrowingBeyondTheExistingPagesAddsOne() {
    final int added = fillPages(3);
    final Dictionary dictionary = dictionary();
    final int pagesBefore = dictionary.getTotalPages();
    final int renamedId = dictionary.getIdByName(name(1), false);

    // ALMOST A WHOLE PAGE ON ITS OWN, SO THE RE-LAYOUT CANNOT POSSIBLY FIT IN THE PAGES THAT ARE ALREADY THERE
    final String huge = "z".repeat(dictionary.getPageSize() - BasePage.PAGE_HEADER_SIZE - Binary.INT_SERIALIZED_SIZE - 8);

    database.transaction(() -> dictionary.updateName(name(1), huge));

    assertThat(dictionary.getTotalPages()).as("the rewrite had to grow the file").isGreaterThan(pagesBefore);
    assertThat(dictionary.getNameById(renamedId)).isEqualTo(huge);
    assertThat(dictionary.getIdByName(huge, false)).isEqualTo(renamedId);

    for (int i = 0; i < added; ++i)
      if (i != renamedId)
        assertThat(dictionary.getIdByName(name(i), false)).as("id of entry %d after the rename", i).isEqualTo(i);

    reopenDatabase();

    assertThat(dictionary().getDictionaryMap()).hasSize(added);
    assertThat(dictionary().getNameById(renamedId)).isEqualTo(huge);
    for (int i = 0; i < added; ++i)
      if (i != renamedId)
        assertThat(dictionary().getIdByName(name(i), false)).as("id of entry %d after reopen", i).isEqualTo(i);
  }

  /**
   * The reason reload() walks the COMMITTED page count instead of getTotalPages(). A rolled-back transaction still has its own
   * page counter set when TransactionContext.rollback() reloads the dictionary, so reading that counter would walk a page that
   * was never committed and rebuild the dictionary from content that is being thrown away.
   * <p>
   * updateName is what reaches this: it runs inside the CALLER's transaction and both modifies existing pages and adds new
   * ones, so a dictionary page lands in modifiedPages (which is what arms the reload on rollback) while the growth is still
   * uncommitted. An append through getIdByName cannot, because it only ever adds a page and never modifies one.
   */
  @Test
  void aRolledBackTransactionThatGrewTheDictionaryLeavesItIntact() {
    final int added = fillPages(2);
    final Dictionary dictionary = dictionary();
    final int pagesBefore = dictionary.getTotalPages();

    final String huge = "z".repeat(dictionary.getPageSize() - BasePage.PAGE_HEADER_SIZE - Binary.INT_SERIALIZED_SIZE - 8);

    assertThatThrownBy(() -> database.transaction(() -> {
      dictionary.updateName(name(1), huge);
      assertThat(dictionary.getTotalPages()).as("the rename has to grow the dictionary for this test to mean anything")
          .isGreaterThan(pagesBefore);
      throw new IllegalStateException("rolled back on purpose");
    })).isInstanceOf(IllegalStateException.class);

    // THE GROWTH IS GONE AND THE IN-RAM VIEW, WHICH updateName HAD ALREADY EDITED, WAS REPAIRED BY THE ROLLBACK RELOAD
    assertThat(dictionary.getTotalPages()).isEqualTo(pagesBefore);
    assertThat(dictionary.getIdByName(huge, false)).isEqualTo(-1);
    assertAllNamesResolve(added);

    reopenDatabase();

    assertThat(dictionary().getTotalPages()).isEqualTo(pagesBefore);
    assertThat(dictionary().getDictionaryMap()).hasSize(added);
    assertAllNamesResolve(added);
  }

  /**
   * The compatibility guarantee in the forward direction, which {@link #pageZeroKeepsTheLegacySinglePageLayout} only covers in
   * reverse: a single-page dictionary carrying the pre-multi-page format version is opened by this code, read in full, and then
   * rolled over. The file is relabelled rather than synthesised because the two formats are byte-identical for one page - the
   * version in the name is the only thing that differs, which is precisely the claim being tested.
   */
  @Test
  void aDictionaryLabelledWithThePreviousFormatVersionLoadsAndThenRollsOver() {
    final int before = 40;
    for (int i = 0; i < before; ++i)
      dictionary().getIdByName(name(i), true);
    assertThat(dictionary().getTotalPages()).as("the fixture has to still be a single page").isEqualTo(1);

    database.close();

    final File databaseDirectory = new File(getDatabasePath());
    final File[] dictionaryFiles = databaseDirectory.listFiles((dir, fileName) -> fileName.endsWith("." + Dictionary.DICT_EXT));
    assertThat(dictionaryFiles).hasSize(1);
    final File current = dictionaryFiles[0];
    final File legacy = new File(databaseDirectory, current.getName().replaceFirst("\\.v\\d+\\.", ".v0."));
    assertThat(current.renameTo(legacy)).isTrue();

    database = factory.open();

    // IT LOADED, IN FULL, WITH NO MIGRATION
    assertThat(dictionary().getDictionaryMap()).hasSize(before);
    assertAllNamesResolve(before);

    // AND IT GROWS FROM THERE, KEEPING EVERY ID IT ARRIVED WITH
    int added = before;
    while (dictionary().getTotalPages() < 3) {
      dictionary().getIdByName(name(added), true);
      ++added;
    }
    assertAllNamesResolve(added);

    reopenDatabase();
    assertThat(dictionary().getDictionaryMap()).hasSize(added);
    assertAllNamesResolve(added);
  }

  /**
   * A dictionary written by a future ArcadeDB would be read as if it had this layout, silently. Opening the database has to fail
   * loudly instead. Exercised the way it would really happen, through the load path, by renaming the file to a higher version.
   */
  @Test
  void aDictionaryFromANewerFormatIsRefusedInsteadOfMisread() {
    dictionary().getIdByName("beforeTheDowngrade", true);
    database.close();

    final File databaseDirectory = new File(getDatabasePath());
    final File[] dictionaryFiles = databaseDirectory.listFiles((dir, fileName) -> fileName.endsWith("." + Dictionary.DICT_EXT));
    assertThat(dictionaryFiles).hasSize(1);

    final File current = dictionaryFiles[0];
    final File fromTheFuture = new File(databaseDirectory, current.getName().replaceFirst("\\.v\\d+\\.", ".v99."));
    assertThat(current.renameTo(fromTheFuture)).isTrue();

    try {
      assertThatThrownBy(() -> factory.open()).hasStackTraceContaining("format version 99");
      assertThat(DatabaseFactory.getActiveDatabaseInstance(getDatabasePath())).isNull();
    } finally {
      // PUT IT BACK AND REOPEN, SO THE HARNESS CAN DROP THE DATABASE AT THE END OF THE TEST
      assertThat(fromTheFuture.renameTo(current)).isTrue();
      database = factory.open();
    }

    assertThat(dictionary().getIdByName("beforeTheDowngrade", false)).isNotEqualTo(-1);
  }

  /**
   * updateName validates before it mutates, and this is the case that proves it has to. Renaming a name that is also a type
   * name is refused with an IllegalArgumentException, which is not an IOException, so updateName's own repairing catch does not
   * fire; and no dictionary page has been written yet, so the rollback does not arm its reload either. Nothing would repair a
   * half-applied rename, so the rename must not start.
   */
  @Test
  void aRenameRefusedBecauseTheNameIsATypeLeavesTheDictionaryConsistent() {
    fillPages(2);
    database.getSchema().createDocumentType("Shared");

    final Dictionary dictionary = dictionary();
    final int id = dictionary.getIdByName("Shared", false);
    assertThat(id).as("the type name is in the dictionary").isNotEqualTo(-1);

    assertThatThrownBy(() -> database.transaction(() -> dictionary.updateName("Shared", "Renamed")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("used as a type name");

    // BOTH DIRECTIONS STILL AGREE, WITHOUT AN UNRELATED RELOAD HAVING TO COME ALONG AND REPAIR THEM
    assertThat(dictionary.getIdByName("Shared", false)).isEqualTo(id);
    assertThat(dictionary.getNameById(id)).isEqualTo("Shared");
    assertThat(dictionary.getIdByName("Renamed", false)).isEqualTo(-1);
  }

  /**
   * The class reasons carefully about threads: appends serialise on this component's monitor while reload() runs on threads
   * that do not hold it. Nothing exercised that under contention, and a rollover is where it would hurt, since two threads
   * computing the tail page at once is what would renumber entries.
   */
  @Test
  void concurrentAppendsAcrossARolloverKeepEveryIdUnique() throws Exception {
    final Dictionary dictionary = dictionary();
    final int threads = 4;
    final int namesPerThread = 250;

    final CountDownLatch startLine = new CountDownLatch(1);
    final List<Thread> workers = new ArrayList<>();
    final Map<String, Integer> assigned = new ConcurrentHashMap<>();
    final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

    for (int t = 0; t < threads; ++t) {
      final int worker = t;
      final Thread thread = new Thread(() -> {
        try {
          startLine.await();
          for (int i = 0; i < namesPerThread; ++i) {
            final String candidate = name(worker * namesPerThread + i);
            assigned.put(candidate, dictionary.getIdByName(candidate, true));
          }
        } catch (final Throwable e) {
          failures.add(e);
        }
      }, "dictionary-appender-" + t);
      workers.add(thread);
      thread.start();
    }

    startLine.countDown();
    for (final Thread thread : workers)
      thread.join();

    assertThat(failures).isEmpty();

    final int total = threads * namesPerThread;
    assertThat(assigned).hasSize(total);
    assertThat(dictionary.getTotalPages()).as("the fixture has to actually roll over").isGreaterThan(1);
    // NO TWO NAMES SHARE AN ID, AND THE IDS COVER EXACTLY 0..total-1 WITH THE ENTRIES THAT WERE ALREADY THERE
    assertThat(new HashSet<>(assigned.values())).hasSize(total);

    for (final Map.Entry<String, Integer> entry : assigned.entrySet())
      assertThat(dictionary.getNameById(entry.getValue())).isEqualTo(entry.getKey());

    // AND THE IDS HANDED OUT UNDER CONTENTION MATCH THE ORDER THE NAMES ACTUALLY LANDED IN ON THE PAGES
    dictionary.reload();
    for (final Map.Entry<String, Integer> entry : assigned.entrySet()) {
      assertThat(dictionary.getIdByName(entry.getKey(), false)).as("id of '%s' after reload", entry.getKey())
          .isEqualTo(entry.getValue());
      assertThat(dictionary.getNameById(entry.getValue())).isEqualTo(entry.getKey());
    }
  }

  /**
   * A page the count claims but that is not on disk must stop the load, not be invented. Inventing it is not merely masked
   * corruption: an empty page contributes zero names, so every name after it would come back with an id lower by however many
   * the missing page held, and those ids are embedded in records. A partial replication replay, which writes a high page number
   * without the ones before it, is how the count gets ahead of the file.
   * <p>
   * Runs against its own database, dropped at the end, because it deliberately leaves the page count inconsistent.
   */
  @Test
  void aDictionaryPageThatIsClaimedButMissingFailsLoudly() {
    final Database other = TestHelper.createDatabase(getDatabasePath() + "_claimedButMissing");
    try {
      final Dictionary dictionary = other.getSchema().getDictionary();
      dictionary.getIdByName("aNameOnPageZero", true);
      assertThat(dictionary.getTotalPages()).isEqualTo(1);

      // CLAIM TWO PAGES THAT WERE NEVER WRITTEN
      dictionary.updatePageCount(3);

      assertThatThrownBy(dictionary::reload)
          .isInstanceOf(DatabaseMetadataException.class)
          .hasMessageContaining("is truncated")
          .hasMessageContaining("page 1 of 3");
    } finally {
      other.drop();
    }
  }

  /**
   * The follower path: a replicated transaction that carries a brand new dictionary page has to create it and make its names
   * visible, which only happens if the post-apply reload walks every page rather than page 0.
   */
  @Test
  void aReplicatedNewDictionaryPageIsVisibleAfterApplyChanges() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final Dictionary dictionary = dictionary();

    final int entriesBefore = dictionary.getDictionaryMap().size();
    final int newPageNumber = dictionary.getTotalPages();

    // BUILD THE PAGE IMAGE THE LEADER WOULD HAVE SHIPPED: THE 4 BYTE LEGACY COUNTER FOLLOWED BY ONE NAME
    final String replicated = "replicatedName";
    final Binary image = new Binary();
    image.putInt(0);
    image.putBytes(replicated.getBytes(DatabaseFactory.getDefaultCharset()));
    image.flip();
    final byte[] content = image.toByteArray();

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = dictionary.getFileId();
    walPage.pageNumber = newPageNumber;
    walPage.currentPageVersion = 1;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + content.length - 1;
    walPage.currentPageSize = content.length;
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.txId = 987654;
    walTx.timestamp = System.currentTimeMillis();
    walTx.pages = new WALFile.WALPage[] { walPage };

    assertThat(db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false)).isTrue();

    assertThat(dictionary.getTotalPages()).isEqualTo(newPageNumber + 1);
    assertThat(dictionary.getDictionaryMap()).hasSize(entriesBefore + 1);
    assertThat(dictionary.getIdByName(replicated, false)).isEqualTo(entriesBefore);
    assertThat(dictionary.getNameById(entriesBefore)).isEqualTo(replicated);

    // AND THE NEXT LOCALLY ADDED NAME CONTINUES AFTER IT INSTEAD OF OVERWRITING IT
    assertThat(dictionary.getIdByName("afterReplication", true)).isEqualTo(entriesBefore + 1);
    assertThat(dictionary.getNameById(entriesBefore)).isEqualTo(replicated);
  }
}
