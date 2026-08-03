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
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5802: an in-place binary upgrade leaves indexes written under the pre-#5321 key order physically sorted the wrong
 * way. The load-time check reports it, but only as a WARNING naming the physical bucket sub-index
 * ({@code Paper_0_84306331895885}) - an operator who did not read the startup log has no way to learn which indexes
 * need a {@code REBUILD INDEX}, and the name in the log is not the one they would act on.
 * <p>
 * The outcome of that check is now an {@link IndexInternal#getUpgradeWarning() upgrade warning}, so it reaches the
 * same surfaces every other "this index should be rebuilt" advisory does: one deduplicated log line per database open
 * naming the LOGICAL index, and the {@code upgradeWarning} property of {@code schema:indexes} and
 * {@code schema:index:<name>} - which is what Studio renders and what makes the affected set queryable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5802KeyOrderUpgradeWarningTest extends TestHelper {

  private static final String TYPE_NAME = "Paper";

  @Override
  public void beforeTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // Most of these tests deliberately leave a disordered index behind, which the integrity check correctly reports.
    return false;
  }

  private void createAndPopulate(final int totalBuckets) {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(totalBuckets)
        .create();
    type.createProperty("author", String.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "author" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false).withPageSize(4096).create();

    // Accented names are the shape #5321 mis-sorted: their UTF-8 lead byte is negative as a Java byte.
    final String[] authors = { "Á Rodríguez-Lescure", "Àngels Sahuquillo", "Zihao Zou", "Zi-Xuan Guo", "Ziyi Xu",
        "Zobaida Lahari", "Zuohua Xie", "Muster" };
    // Per bucket, because a sub-index is only created once its mutable index holds more than one page.
    final int records = 2_000 * totalBuckets;
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        database.newDocument(TYPE_NAME).set("author", authors[i % authors.length]).save();
    });
  }

  private TypeIndex typeIndex() {
    return (TypeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next();
  }

  private List<LSMTreeIndex> bucketIndexes() {
    final List<LSMTreeIndex> indexes = new ArrayList<>();
    for (final Index bucketIndex : typeIndex().getIndexesOnBuckets())
      indexes.add((LSMTreeIndex) bucketIndex);
    return indexes;
  }

  /** Compacts every bucket sub-index and returns those that ended up with a compacted sub-index to disorder. */
  private List<LSMTreeIndex> compactAll() throws Exception {
    final List<LSMTreeIndex> compacted = new ArrayList<>();
    for (final LSMTreeIndex index : bucketIndexes()) {
      if (index.scheduleCompaction())
        index.compact();
      if (index.getMutableIndex().getSubIndex() != null)
        compacted.add(index);
    }
    return compacted;
  }

  /**
   * Swaps the first two entries of every page holding at least two of them. That is the same physical state an index
   * written under the pre-#5321 signed byte order ends up in, reproduced without needing the old comparator.
   */
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

  private List<Result> schemaIndexesWithUpgradeWarning() {
    final List<Result> rows = new ArrayList<>();
    final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes");
    while (rs.hasNext()) {
      final Result row = rs.next();
      if (row.getProperty("upgradeWarning") != null)
        rows.add(row);
    }
    return rows;
  }

  @Test
  void aHealthyIndexCarriesNoUpgradeWarning() throws Exception {
    createAndPopulate(1);
    compactAll();
    reopenDatabase();

    assertThat(bucketIndexes().getFirst().getUpgradeWarning()).isNull();
    assertThat(typeIndex().getUpgradeWarning()).isNull();
    assertThat(schemaIndexesWithUpgradeWarning()).isEmpty();
  }

  @Test
  void aDisorderedCompactedIndexIsReportedAsAnUpgradeWarning() throws Exception {
    createAndPopulate(1);
    compactAll();
    disorderPages(bucketIndexes().getFirst().getMutableIndex().getSubIndex());

    reopenDatabase();

    final String warning = bucketIndexes().getFirst().getUpgradeWarning();
    assertThat(warning).as("the load-time key-order check is exposed as an upgrade warning").isNotNull();
    assertThat(warning).containsIgnoringCase("key order");
  }

  /**
   * The point of the fix: the affected set is queryable. {@code typeIndexName} carries the name a
   * {@code REBUILD INDEX} takes, which the physical sub-index name in the log line does not.
   */
  @Test
  void theAffectedIndexesAreDiscoverableFromSchemaIndexes() throws Exception {
    createAndPopulate(1);
    compactAll();
    disorderPages(bucketIndexes().getFirst().getMutableIndex().getSubIndex());

    reopenDatabase();

    // schema:indexes lists both the logical type index and its bucket sub-index, and every flagged row must carry the
    // name a REBUILD INDEX takes - which the physical sub-index name in the log line is not.
    final List<Result> flagged = schemaIndexesWithUpgradeWarning();
    assertThat(flagged).isNotEmpty();
    assertThat(flagged).allMatch(row -> typeIndex().getName().equals(row.getProperty("typeIndexName")));
    assertThat(flagged.stream().map(row -> (String) row.getProperty("name")))
        .contains(typeIndex().getName(), bucketIndexes().getFirst().getName());

    final ResultSet detail = database.query("sql", "SELECT FROM schema:index:`" + typeIndex().getName() + "`");
    assertThat(detail.hasNext()).isTrue();
    assertThat((String) detail.next().getProperty("upgradeWarning")).isNotNull();
  }

  /**
   * A key-order mismatch is PHYSICAL state, so it can affect any bucket sub-index independently - unlike the
   * definition-derived warnings the type index used to answer for by asking only its first bucket.
   */
  @Test
  void aTypeIndexReportsAWarningRaisedByANonFirstBucket() throws Exception {
    createAndPopulate(3);

    final List<LSMTreeIndex> compacted = compactAll();
    assertThat(compacted).as("more than one bucket sub-index was compacted").hasSizeGreaterThan(1);
    assertThat(compacted.getLast()).isNotSameAs(bucketIndexes().getFirst());
    disorderPages(compacted.getLast().getMutableIndex().getSubIndex());

    reopenDatabase();

    assertThat(bucketIndexes().getFirst().getUpgradeWarning()).as("the first bucket is healthy").isNull();
    assertThat(typeIndex().getUpgradeWarning()).as("the type index answers for every bucket").isNotNull();
  }

  /**
   * A full-text index reaches {@code IndexInternal} by composition, not inheritance, so before the delegate was added
   * it silently answered the interface default of {@code null}. Its keys are analyzed user text - the one place
   * non-ASCII characters are guaranteed to turn up - which makes it the half of the report most likely to be missed.
   */
  @Test
  void aDisorderedFullTextIndexReportsThroughTheCompositionWrapper() throws Exception {
    final String ftType = "Article";
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(ftType).withTotalBuckets(1).create();
      database.getSchema().getType(ftType).createProperty("title", String.class);
      database.getSchema().buildTypeIndex(ftType, new String[] { "title" }).withType(Schema.INDEX_TYPE.FULL_TEXT)
          .withFullTextType().withPageSize(4096).create();
    });

    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newDocument(ftType).set("title", "sécurité robuste entropie término " + i).save();
    });

    final TypeIndex ftIndex = (TypeIndex) database.getSchema().getIndexByName(ftType + "[title]");
    final IndexInternal ftBucketIndex = ftIndex.getIndexesOnBuckets()[0];
    assertThat(ftBucketIndex).isInstanceOf(LSMTreeFullTextIndex.class);

    if (ftBucketIndex.scheduleCompaction())
      ftBucketIndex.compact();

    final LSMTreeIndexCompacted subIndex = compactedSubIndexOf(ftBucketIndex);
    assertThat(subIndex).as("the full-text index was compacted").isNotNull();
    disorderPages(subIndex);

    reopenDatabase();

    final TypeIndex reloaded = (TypeIndex) database.getSchema().getIndexByName(ftType + "[title]");
    assertThat(reloaded.getIndexesOnBuckets()[0].getUpgradeWarning())
        .as("the wrapper delegates instead of answering the interface default").isNotNull();
    assertThat(reloaded.getUpgradeWarning()).isNotNull();
  }

  /**
   * The compacted component of an index, reached through its file ids rather than a typed accessor, so this works
   * for a wrapper that holds its LSM index by composition just as well as for a plain one.
   */
  private LSMTreeIndexCompacted compactedSubIndexOf(final IndexInternal index) {
    for (final int fileId : index.getFileIds())
      if (database.getSchema().getFileById(fileId) instanceof final LSMTreeIndexCompacted compacted)
        return compacted;
    return null;
  }

  @Test
  void rebuildingTheIndexClearsTheWarning() throws Exception {
    createAndPopulate(1);
    compactAll();
    disorderPages(bucketIndexes().getFirst().getMutableIndex().getSubIndex());

    reopenDatabase();
    assertThat(typeIndex().getUpgradeWarning()).isNotNull();

    database.command("sql", "REBUILD INDEX `" + typeIndex().getName() + "`");

    assertThat(typeIndex().getUpgradeWarning()).isNull();
    assertThat(schemaIndexesWithUpgradeWarning()).isEmpty();
  }
}
