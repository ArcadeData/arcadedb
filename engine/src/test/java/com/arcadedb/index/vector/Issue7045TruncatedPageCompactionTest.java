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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7045: a parse failure in the middle of a vector index page dropped every entry from the
 * failing one to the end of that page, silently, and a compaction then rewrote the data file from that truncated
 * set and reclaimed the source - so the vectors it lacked were gone from every later search while the index kept
 * reporting itself healthy. The parse is now strict when it feeds a compaction: the failure propagates, the
 * compaction is aborted before any file is created or replaced, and the lenient load/rebuild paths keep their
 * recovery fallbacks.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7045TruncatedPageCompactionTest {
  private static final String DB_PATH     = "./target/databases/Issue7045TruncatedPageCompactionTest";
  private static final int    DIMENSIONS  = 8;
  private static final int    NUM_VECTORS = 300;

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void compactionAbortsInsteadOfShippingATruncatedLiveSet() throws Exception {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random rng = new Random(7045);

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

        db.transaction(() -> {
          final DocumentType t = db.getSchema().createDocumentType("Doc");
          t.createProperty("id", Type.INTEGER);
          t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        });
        db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
            + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\" }");

        for (int batch = 0; batch < 3; batch++) {
          final int from = batch * NUM_VECTORS / 3;
          db.transaction(() -> {
            for (int i = from; i < from + NUM_VECTORS / 3; i++)
              db.newDocument("Doc").set("id", i).set("embedding", randomVector(rng)).save();
          });
        }

        final DatabaseInternal internal = (DatabaseInternal) db;
        final LSMVectorIndex lsm = vectorIndex(db);
        final int fileIdBefore = lsm.getFileId();
        final int totalPages = lsm.getTotalPages();
        final int pageSize = lsm.getPageSize();

        assertThat(parse(internal, fileIdBefore, totalPages, pageSize, false))
            .as("precondition: every entry is readable before the corruption").hasSize(NUM_VECTORS);

        final int corruptedEntry = NUM_VECTORS / 2;
        corruptEntry(internal, fileIdBefore, pageSize, corruptedEntry);

        // The lenient parse is what the issue describes: the page is cut short at the corrupted entry and the parse
        // reports fewer entries than the page holds, with nothing but a WARNING to tell.
        final int lenientlyParsed = parse(internal, fileIdBefore, totalPages, pageSize, false).size();
        assertThat(lenientlyParsed).as("the corruption must cut the page short, or this test proves nothing")
            .isLessThan(NUM_VECTORS);

        // The strict parse, the one a compaction runs, must refuse instead.
        assertThatThrownBy(() -> parse(internal, fileIdBefore, totalPages, pageSize, true))
            .isInstanceOf(IndexException.class)
            .hasMessageContaining("page 0")
            .hasMessageContaining("of the page not read");

        // And the compaction itself must abort, leaving the source file - and every vector in it - in place.
        assertThat(lsm.scheduleCompaction()).isTrue();
        assertThatThrownBy(lsm::compact)
            .as("a compaction fed a truncated live set must fail rather than reclaim the source file")
            .hasStackTraceContaining("Error parsing page 0");

        assertThat(lsm.getFileId()).as("the source data file must not have been replaced").isEqualTo(fileIdBefore);
        assertThat(lsm.countEntries()).as("no vector may have been dropped by the aborted compaction")
            .isEqualTo(NUM_VECTORS);
        assertThat(lsm.scheduleCompaction()).as("the aborted compaction must hand its scheduling slot back")
            .isTrue();
      } finally {
        db.drop();
      }
    }
  }

  private static List<LSMVectorIndexPageParser.VectorEntry> parse(final DatabaseInternal db, final int fileId,
      final int totalPages, final int pageSize, final boolean strict) {
    final List<LSMVectorIndexPageParser.VectorEntry> entries = new ArrayList<>();
    LSMVectorIndexPageParser.parsePages(db, fileId, totalPages, pageSize, false, strict, entries::add);
    return entries;
  }

  /**
   * Corrupts entry {@code entryIndex} of page 0 the way a damaged page would: its quantization type byte claims an
   * INT8 payload as long as the whole page, so the parse of the next entry starts past the end of the page.
   */
  private static void corruptEntry(final DatabaseInternal db, final int fileId, final int pageSize, final int entryIndex) {
    db.transaction(() -> {
      final MutablePage page;
      try {
        page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
      assertThat(page.readInt(LSMVectorIndex.OFFSET_NUM_ENTRIES)).as("every entry must be on page 0")
          .isEqualTo(NUM_VECTORS);

      int offset = LSMVectorIndex.HEADER_BASE_SIZE;
      for (int i = 0; i < entryIndex; i++) {
        offset = skipIdsAndDeletedFlag(page, offset);
        offset = LSMVectorIndexPageParser.skipQuantizationData(page, offset);
      }
      offset = skipIdsAndDeletedFlag(page, offset);
      page.writeByte(offset, (byte) VectorQuantizationType.INT8.ordinal());
      page.writeInt(offset + 1, pageSize);
    });
  }

  private static int skipIdsAndDeletedFlag(final MutablePage page, int offset) {
    offset += (int) page.readNumberAndSize(offset)[1]; // vector id
    offset += (int) page.readNumberAndSize(offset)[1]; // bucket id
    offset += (int) page.readNumberAndSize(offset)[1]; // position
    return offset + 1; // deleted flag
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
  }

  private static float[] randomVector(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) rng.nextGaussian();
    return v;
  }
}
