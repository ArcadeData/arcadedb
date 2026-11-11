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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A full schema reload of a LIVE database builds a new {@link Dictionary} on the same file, and a component built from a file
 * counts its pages from the file's size. The file lags a commit until the flush thread writes the page, so a dictionary page
 * committed but still queued for flushing was invisible to the new instance: its page count was short, the names on that page
 * were not loaded, and when the missing page was page 0 (a brand-new database) {@code createHeaderPageIfMissing()} wrote a
 * fresh page 0 over the committed one and failed its commit with a {@code ConcurrentModificationException} (seen as a flaky
 * {@code ColumnarIndexManagerLifecycleTest.registerIsIdempotentPerIndexFileId}). The new instance has to take the committed
 * page count of the one it replaces BEFORE it reads its names or decides that the header page is missing.
 * <p>
 * Made deterministic two ways: by suspending the page flush, so a new dictionary page stays committed and off the disk, and,
 * for page 0, by emptying the file behind the committed page as {@link DictionaryFileSizeLagTest} does, which is the state the
 * flush lag of a brand-new database opens.
 */
class SchemaReloadDictionaryPagesNotFlushedTest extends TestHelper {

  @Test
  void aReloadSeesTheDictionaryPagesTheFlushHasNotWrittenYet() throws IOException {
    // everything created so far reaches the disk, so the file covers exactly the pages committed until now
    reopenDatabase();

    final LocalSchema schema = ((DatabaseInternal) database).getSchema().getEmbedded();
    final Dictionary before = schema.getDictionary();
    final int pagesOnDisk = before.getCommittedPageCount();

    assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(database, true))
        .as("flush suspension must engage or the new pages could reach disk, invalidating the test").isTrue();
    try {
      // enough names to roll the dictionary onto a page the flush never writes
      int i = 0;
      while (before.getCommittedPageCount() == pagesOnDisk)
        before.getIdByName("dictionaryReloadName_%06d_paddingPaddingPaddingPadding".formatted(i++), true);
      final Map<String, Integer> expected = new HashMap<>(before.getDictionaryMap());

      schema.load(ComponentFile.MODE.READ_WRITE, true);

      final Dictionary after = schema.getDictionary();
      assertThat(after).as("the reload built a new dictionary instance").isNotSameAs(before);
      assertThat(after.getCommittedPageCount()).isEqualTo(before.getCommittedPageCount());
      assertThat(after.wasHeaderPageRecreated()).isFalse();
      assertThat(after.getDictionaryMap()).as("no committed name is lost by the reload").hasSameSizeAs(expected);
      assertThat(after.getDictionaryMap()).as("every committed name keeps its id across the reload").isEqualTo(expected);

      // a name interned after the reload must not reuse an id already written into records
      final int next = after.getIdByName("dictionaryReloadNameAfterReload", true);
      assertThat(next).isEqualTo(expected.size());
    } finally {
      PageManager.INSTANCE.getFlushThread().setSuspended(database, false);
    }
  }

  @Test
  void aReloadWhileTheHeaderPageIsNotOnDiskKeepsTheCommittedPage() throws IOException {
    database.getSchema().createDocumentType("HeaderPageNotOnDisk");
    database.transaction(() -> database.newDocument("HeaderPageNotOnDisk").set("headerPageName", "v").save());

    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalSchema schema = db.getSchema().getEmbedded();
    final Dictionary before = schema.getDictionary();
    final Map<String, Integer> expected = new HashMap<>(before.getDictionaryMap());
    assertThat(expected).containsKey("headerPageName");

    assertThat(db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db)).isTrue();
    try (final RandomAccessFile raw = new RandomAccessFile(before.getOSFile(), "rw")) {
      raw.setLength(0);
    }
    assertThat(before.getComponentFile().getSize()).as("the premise: the file is empty").isZero();
    assertThat(before.getCommittedPageCount()).as("the premise: the committed count still claims its page").isEqualTo(1);

    // before the fix: ConcurrentModificationException on PageId(.../0) "current v.0 <> database v.N"
    schema.load(ComponentFile.MODE.READ_WRITE, true);

    final Dictionary after = schema.getDictionary();
    assertThat(after).isNotSameAs(before);
    assertThat(after.wasHeaderPageRecreated()).as("a committed header page is not a missing one").isFalse();
    assertThat(after.getCommittedPageCount()).isEqualTo(1);
    assertThat(after.getDictionaryMap()).isEqualTo(expected);
  }
}
