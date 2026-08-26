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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.DatabaseMetadataException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6351: {@link Dictionary#reload()} used to decide whether there was anything to read from the SIZE OF THE FILE, while
 * the loop right below it argues at length that the file is the wrong source for exactly that question and counts with
 * {@code pageCount}, which only advances on commit.
 * <p>
 * The two disagree in a window that exists by construction: a commit advances {@code pageCount} and hands the page to the flush
 * thread, so until that thread gets to it the file is shorter than the count claims - and while the dictionary's FIRST page is
 * in flight, the file is empty and the old guard returned without reading anything at all. What that costs is not "nothing is
 * loaded": {@code reload()} is the repair path, so leaving the in-RAM view untouched means the thing being repaired survives.
 * <p>
 * These tests construct that state directly - a file behind its committed page count - because it is the state, not the way it
 * arises, that the guard reads. Truncating the file is also how a real one looks after a kill mid-write.
 * <p>
 * In package {@code com.arcadedb.engine} on purpose: reaching the component's file needs package access.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DictionaryFileSizeLagTest extends TestHelper {
  /**
   * Empties the dictionary's file on disk after draining everything pending, leaving {@code pageCount} claiming the pages that
   * are committed and the file claiming none - the same disagreement the flush lag opens, and one that stays open because
   * nothing else is going to write that file until the next commit.
   */
  private void emptyTheFileBehindTheCommittedPages(final DatabaseInternal db, final Dictionary dictionary) throws IOException {
    assertThat(db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db))
        .as("the pending pages have to be on disk before the file is emptied, or the flush thread refills it")
        .isTrue();

    try (final RandomAccessFile raw = new RandomAccessFile(dictionary.getOSFile(), "rw")) {
      raw.setLength(0);
    }

    assertThat(dictionary.getComponentFile().getSize()).as("the premise: the file is empty").isZero();
    assertThat(dictionary.getTotalPages()).as("the premise: the committed count still claims its pages").isPositive();
  }

  /**
   * The repair the window costs: {@code updateName()} publishes the renamed in-RAM view BEFORE rewriting the pages, and a
   * rollback of the transaction that was rewriting them puts the committed names back through {@code reload()}. With the guard
   * reading the file, that rollback returned without reading anything and the rename survived in RAM with no page ever
   * rewritten - the dictionary then answers with a name that is in no page of the database.
   */
  @Test
  void aRolledBackRenameIsUndoneWhileTheFileIsBehindTheCommittedPages() throws Exception {
    final Database other = TestHelper.createDatabase(getDatabasePath() + "_renameRolledBack");
    try {
      final DatabaseInternal db = (DatabaseInternal) other;
      final Dictionary dictionary = db.getSchema().getDictionary();

      final int id = dictionary.getIdByName("committedName", true);
      assertThat(dictionary.getNameById(id)).isEqualTo("committedName");

      emptyTheFileBehindTheCommittedPages(db, dictionary);

      db.begin();
      dictionary.updateName("committedName", "renamedName");
      // THE RENAME IS LIVE IN RAM WHILE ITS PAGES ARE STILL THE TRANSACTION'S OWN, UNCOMMITTED COPIES
      assertThat(dictionary.getNameById(id)).isEqualTo("renamedName");
      db.rollback();

      assertThat(dictionary.getNameById(id)).as("the committed name is back").isEqualTo("committedName");
      assertThat(dictionary.getIdByName("committedName", false)).isEqualTo(id);
      assertThat(dictionary.getIdByName("renamedName", false)).as("the rolled-back name resolves to nothing").isEqualTo(-1);
    } finally {
      other.drop();
    }
  }

  /**
   * The other half: the truncation guard that refuses a page the count claims but nobody wrote lives INSIDE the branch the old
   * check skipped, so in the same window a dictionary claiming pages that do not exist loaded silently instead of failing.
   */
  @Test
  void aClaimedButMissingPageStillFailsLoudlyWhileTheFileIsBehindTheCommittedPages() throws Exception {
    final Database other = TestHelper.createDatabase(getDatabasePath() + "_claimedWhileFileEmpty");
    try {
      final DatabaseInternal db = (DatabaseInternal) other;
      final Dictionary dictionary = db.getSchema().getDictionary();

      dictionary.getIdByName("committedName", true);

      emptyTheFileBehindTheCommittedPages(db, dictionary);

      // CLAIM A SECOND PAGE THAT WAS NEVER WRITTEN, ON TOP OF A FILE THAT HOLDS NOTHING
      dictionary.updatePageCount(2);

      assertThatThrownBy(dictionary::reload)
          .isInstanceOf(DatabaseMetadataException.class)
          .hasMessageContaining("is truncated")
          .hasMessageContaining("page 1 of 2");
    } finally {
      other.drop();
    }
  }

  /**
   * And the case the old guard was written for still behaves: a dictionary whose file really is empty AND whose committed count
   * really is zero has an empty in-RAM view, which is the correct one. That is the state a database killed before its header
   * page reached disk opens in, so this also pins that such a database still opens.
   */
  @Test
  void anEmptyDictionaryWithNothingCommittedReloadsToAnEmptyView() throws Exception {
    reopenWithADictionaryFileTruncatedTo(0, "_nothingCommitted");
  }

  /**
   * The one case where the file size and the committed count give different answers at OPEN, which is why the two sibling checks
   * moved with {@code reload()}: a file holding LESS than one whole page, i.e. a database killed part way through writing the
   * header page. There is no complete page 0 in there to keep, so the count says zero and the header page is written over those
   * bytes; the file-size check saw bytes and left the stub for the first append to deal with.
   */
  @Test
  void aDictionaryFileShorterThanOnePageIsRepairedAtOpen() throws Exception {
    reopenWithADictionaryFileTruncatedTo(37, "_shorterThanOnePage");
  }

  /**
   * Creates a database with one dictionary name, truncates its dictionary file to {@code length} bytes with the database closed,
   * then reopens it and checks the dictionary is empty, whole and usable again.
   */
  private void reopenWithADictionaryFileTruncatedTo(final long length, final String suffix) throws Exception {
    final Database other = TestHelper.createDatabase(getDatabasePath() + suffix);
    final String path = other.getDatabasePath();
    try {
      final DatabaseInternal db = (DatabaseInternal) other;
      final Dictionary dictionary = db.getSchema().getDictionary();
      dictionary.getIdByName("committedName", true);
      final File dictFile = dictionary.getOSFile();
      db.close();

      try (final RandomAccessFile raw = new RandomAccessFile(dictFile, "rw")) {
        raw.setLength(length);
      }

      final Database reopened = new DatabaseFactory(path).open();
      try {
        final Dictionary reloaded = ((DatabaseInternal) reopened).getSchema().getDictionary();
        assertThat(reloaded.getDictionaryMap()).isEmpty();

        // THE HEADER PAGE IS BACK BEFORE ANYTHING IS APPENDED, WHICH IS THE POINT: THE OPEN ITSELF REPAIRED THE FILE INSTEAD OF
        // LEAVING A STUB FOR WHATEVER HAPPENED TO WRITE NEXT
        assertThat(reloaded.getTotalPages()).isEqualTo(1);
        assertThat(((DatabaseInternal) reopened).getPageManager().waitAllPagesOfDatabaseAreFlushed(reopened)).isTrue();
        assertThat(reloaded.getComponentFile().getSize())
            .as("the file holds exactly the header page, with no truncated remainder")
            .isEqualTo(reloaded.getPageSize());

        // AND THE DICTIONARY IS USABLE: THE NEXT NAME LANDS AT ID 0
        assertThat(reloaded.getIdByName("afterRepair", true)).isZero();
      } finally {
        reopened.drop();
      }
    } finally {
      if (other.isOpen())
        other.drop();
    }
  }
}
