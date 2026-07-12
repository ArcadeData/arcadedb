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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/** Writes an ordered key/RID stream into the compacted LSM series format used by normal compaction. */
final class LSMTreeIndexCompactedStreamWriter {
  private static final RID[] ROOT_SENTINEL = new RID[] { new RID(0, 0) };

  private final LSMTreeIndex          mainIndex;
  private final LSMTreeIndexMutable   mutableIndex;
  private final LSMTreeIndexCompacted compactedIndex;
  private final DatabaseInternal      database;
  private final Binary                keyValueContent = new Binary();

  private MutablePage     rootPage;
  private TrackableBinary rootPageBuffer;
  private MutablePage     lastPage;
  private TrackableBinary lastPageBuffer;
  private AtomicInteger   pageNumberInSeries;
  private Object[]        lastPageMaxKey;
  private List<MutablePage> newPagesInSeries;

  LSMTreeIndexCompactedStreamWriter(final LSMTreeIndex mainIndex, final LSMTreeIndexCompacted compactedIndex) {
    this.mainIndex = mainIndex;
    this.mutableIndex = mainIndex.getMutableIndex();
    this.compactedIndex = compactedIndex;
    this.database = mutableIndex.getDatabase();
  }

  MutablePage startSeries() {
    if (rootPage != null)
      throw new IllegalStateException("A compacted series is already active for index '" + mainIndex.getName() + "'");

    rootPage = compactedIndex.createNewPage(0);
    rootPageBuffer = rootPage.getTrackable();
    lastPage = null;
    lastPageBuffer = null;
    pageNumberInSeries = new AtomicInteger(1);
    lastPageMaxKey = null;
    newPagesInSeries = new ArrayList<>();
    return rootPage;
  }

  void append(final Object[] keys, final RID[] rids) throws IOException, InterruptedException {
    if (rootPage == null)
      throw new IllegalStateException("No compacted series is active for index '" + mainIndex.getName() + "'");

    final List<MutablePage> newPages = compactedIndex.appendDuringCompaction(keyValueContent, lastPage, lastPageBuffer,
        pageNumberInSeries, keys, rids);

    if (!newPages.isEmpty()) {
      lastPage = newPages.getLast();
      lastPageBuffer = lastPage.getTrackable();

      for (final MutablePage newPage : newPages) {
        final int newPageNumber = newPage.getPageId().getPageNumber();
        final List<MutablePage> newRootPages = compactedIndex.appendDuringCompaction(keyValueContent, rootPage,
            rootPageBuffer, pageNumberInSeries, keys, new RID[] { new RID(0, newPageNumber) });

        LogManager.instance().log(mainIndex, Level.FINE,
            "- Creating a new entry in index '%s' root page %s->%d (entry in page=%d threadId=%d)", null, mutableIndex,
            Arrays.toString(keys), newPageNumber, mutableIndex.getCount(rootPage) - 1, Thread.currentThread().threadId());

        if (!newRootPages.isEmpty())
          throw new UnsupportedOperationException("Root index page overflow");
      }
      newPagesInSeries.addAll(newPages);
    }

    lastPageMaxKey = keys;
  }

  void finishSeries() throws IOException, InterruptedException {
    if (rootPage == null)
      throw new IllegalStateException("No compacted series is active for index '" + mainIndex.getName() + "'");

    if (lastPageMaxKey != null) {
      final List<MutablePage> overflow = compactedIndex.appendDuringCompaction(keyValueContent, rootPage, rootPageBuffer,
          pageNumberInSeries, lastPageMaxKey, ROOT_SENTINEL);
      if (!overflow.isEmpty())
        throw new UnsupportedOperationException("Root index page overflow");

      LogManager.instance().log(mainIndex, Level.FINE,
          "- Creating last entry in index '%s' root page %s (entriesInRootPage=%d threadId=%d)", null, mutableIndex,
          Arrays.toString(lastPageMaxKey), compactedIndex.getCount(rootPage), Thread.currentThread().threadId());
    }

    final List<MutablePage> modifiedPages = new ArrayList<>(newPagesInSeries);
    modifiedPages.add(database.getPageManager().updatePageVersion(rootPage, true));
    database.getPageManager().writePages(modifiedPages, false);

    rootPage = null;
    rootPageBuffer = null;
    lastPage = null;
    lastPageBuffer = null;
    pageNumberInSeries = null;
    lastPageMaxKey = null;
    newPagesInSeries = null;
  }

  int getRootEntryCount() {
    return rootPage != null ? compactedIndex.getCount(rootPage) : 0;
  }
}
