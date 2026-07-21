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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.TestHelper;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * An LSM index whose pages are physically sorted in a different order than the one the lookup applies keeps serving
 * incomplete results and raises no error: that is what made #5120/#5321 so hard to spot, and an index built by an
 * older build stays in that state after the upgrade until it is rebuilt. These tests cover the two places that now
 * report it: the bounded root-page check run when the index is loaded, and the full key-order walk CHECK DATABASE
 * performs through {@code IndexInternal.checkIntegrity()}.
 * <p>
 * The disorder is injected by swapping two pointers of a page's entry array, which is precisely the state an index
 * written under a different comparator ends up in, without depending on the old comparator being available.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeIndexKeyOrderCheckTest extends TestHelper {

  private static final String TYPE_NAME = "OrderCheck";

  @Override
  public void beforeTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // Two of these tests leave a deliberately disordered index behind, which the new check correctly reports.
    return false;
  }

  private LSMTreeIndex createAndPopulate() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("name", String.class);
    type.createProperty("lang", String.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "name", "lang" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false).withPageSize(4096).create();

    final String[] names = { "Müller", "Muster", "Straße", "Strasse", "Grün", "Gruppe", "Köln", "Kosten" };
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newDocument(TYPE_NAME).set("name", names[i % names.length]).set("lang", "de").save();
    });

    return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
  }

  /** Swaps the first two entries of every page holding at least two of them, leaving the entries themselves intact. */
  private void disorderPages(final LSMTreeIndexAbstract index) {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.transaction(() -> {
      for (int pageNumber = 0; pageNumber < index.getTotalPages(); ++pageNumber) {
        try {
          final MutablePage page = db.getTransaction()
              .getPageToModify(new PageId(database, index.getFileId(), pageNumber), index.getPageSize(), false);

          if (index.getCount(page) < 2)
            continue;

          final int startIndexArray = index.getHeaderSize(pageNumber);
          final int first = page.readInt(startIndexArray);
          final int second = page.readInt(startIndexArray + INT_SERIALIZED_SIZE);
          page.writeInt(startIndexArray, second);
          page.writeInt(startIndexArray + INT_SERIALIZED_SIZE, first);
        } catch (final Exception e) {
          throw new IllegalStateException("Cannot alter page " + pageNumber, e);
        }
      }
    });
  }

  @Test
  void healthyIndexReportsNoProblem() throws Exception {
    final LSMTreeIndex index = createAndPopulate();

    assertThat(index.checkIntegrity()).as("mutable pages are correctly ordered").isEmpty();

    if (index.scheduleCompaction())
      index.compact();

    final LSMTreeIndexCompacted subIndex = index.getMutableIndex().getSubIndex();
    assertThat(subIndex).isNotNull();
    assertThat(subIndex.checkRootPagesKeyOrder(2, 10)).as("root pages of a freshly compacted index are ordered").isEmpty();
    assertThat(index.checkIntegrity()).as("compacted pages are correctly ordered").isEmpty();
  }

  @Test
  void mutablePagesOutOfOrderAreReported() {
    final LSMTreeIndex index = createAndPopulate();

    disorderPages(index.getMutableIndex());

    assertThat(index.checkIntegrity()).as("checkIntegrity() reports the disorder")
        .isNotEmpty()
        .anyMatch(problem -> problem.contains("physical key order"));
  }

  @Test
  void compactedPagesOutOfOrderAreReportedByBothChecks() throws Exception {
    final LSMTreeIndex index = createAndPopulate();

    if (index.scheduleCompaction())
      index.compact();

    final LSMTreeIndexCompacted subIndex = index.getMutableIndex().getSubIndex();
    assertThat(subIndex).isNotNull();

    disorderPages(subIndex);

    // The bounded check the open path runs: it only reads the root page of the most recent series.
    assertThat(subIndex.checkRootPagesKeyOrder(2, 10)).as("root-page check used at load time").isNotEmpty();

    // The full walk CHECK DATABASE runs, which also lists the index among the corrupted ones.
    assertThat(index.checkIntegrity()).isNotEmpty();

    final ResultSet rs = database.command("sql", "CHECK DATABASE");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat((Collection<String>) result.getProperty("corruptedIndexes")).isNotEmpty();
    assertThat((Collection<String>) result.getProperty("warnings")).anyMatch(w -> w.contains("physical key order"));
  }

  /** The load-time check runs while the index is being wired up: it must report, never stop the database from opening. */
  @Test
  void aDisorderedIndexStillOpens() throws Exception {
    final LSMTreeIndex index = createAndPopulate();

    if (index.scheduleCompaction())
      index.compact();

    disorderPages(index.getMutableIndex().getSubIndex());

    reopenDatabase();

    assertThat(database.isOpen()).isTrue();
    assertThat(database.getSchema().existsType(TYPE_NAME)).as("the index is reported, not dropped").isTrue();

    final LSMTreeIndex reloaded = (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
    assertThat(reloaded.checkIntegrity()).isNotEmpty();
  }
}
