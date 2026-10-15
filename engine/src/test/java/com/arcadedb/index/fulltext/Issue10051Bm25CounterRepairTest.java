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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10051 (#10019): the repair of drifted BM25 corpus counters done by the first search used to be lost on a restart, so
 * the same whole-type scan ran again on the first search of every session. Its warning named only the type, and the counters
 * could not be inspected without triggering that repair.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10051Bm25CounterRepairTest extends TestHelper {
  private static final int DOCS = 12;

  private void createFillAndDrift() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 3");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.questions STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      database.command("sql", "CREATE INDEX ON Doc (questions) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma', questions = 'why'");
    });
    // The drift a crash leaves behind: counters written to the schema that do not match the data
    metadata("Doc[content]").applyCorpusDelta(7, 40);
    ((DatabaseInternal) database).getSchema().getEmbedded().saveConfiguration();
    reopenDatabase();
  }

  private FullTextIndexMetadata metadata(final String indexName) {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName(indexName));
  }

  private void search() {
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
  }

  @Test
  void aRepairMadeByASearchSurvivesARestart() {
    createFillAndDrift();
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS + 7L);

    search();

    assertThat(metadata("Doc[content]").getScans()).isEqualTo(1L);
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata("Doc[content]").getSumDocLength()).isEqualTo(DOCS * 3L);

    reopenDatabase();
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);

    search();

    // Paid once: the second session found the counters exact and scanned nothing
    assertThat(metadata("Doc[content]").getScans()).isZero();
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata("Doc[content]").getSumDocLength()).isEqualTo(DOCS * 3L);
  }

  /** What the repair of one index found and fixed is no business of its siblings on the same type. */
  @Test
  void aRepairLeavesTheOtherFullTextIndexOfTheTypeAlone() {
    createFillAndDrift();

    search();

    assertThat(metadata("Doc[questions]").getScans()).isZero();
    assertThat(metadata("Doc[questions]").getTotalDocs()).isEqualTo(DOCS);
  }

  @Test
  void theWarningNamesTheLogicalIndexAndTheExactRepair() {
    final String message = LSMTreeFullTextIndex.corpusDriftMessage("Doc[questions]", "Doc", 19L, 12L, false);

    assertThat(message).contains("'Doc[questions]'").contains("type 'Doc'").contains("persisted=19").contains("live=12");
    assertThat(message).contains("REBUILD INDEX `Doc[questions]` WITH statsOnly = true");
  }

  @Test
  void theWarningSaysWhenTheCountsAgreeAndOnlyAnInexactScanIsBeingRedone() {
    final String message = LSMTreeFullTextIndex.corpusDriftMessage("Doc[content]", "Doc", 12L, 12L, true);

    assertThat(message).contains("raced concurrent writers").doesNotContain("diverged");
    assertThat(message).contains("REBUILD INDEX `Doc[content]` WITH statsOnly = true");
  }

  /** Looking at the counters must not repair them: the drift is what the operator came to see. */
  @Test
  void schemaIndexShowsTheCountersWithoutRepairingThem() {
    createFillAndDrift();

    final ResultSet resultSet = database.query("sql", "SELECT FROM schema:index:`Doc[content]`");
    assertThat(resultSet.hasNext()).isTrue();
    final Result detail = resultSet.next();
    resultSet.close();

    final Map<String, Object> bm25 = detail.getProperty("bm25");
    assertThat(bm25).isNotNull();
    assertThat(bm25.get("totalDocs")).isEqualTo(DOCS + 7L);
    assertThat(bm25.get("sumDocLength")).isEqualTo(DOCS * 3L + 40L);
    assertThat((double) bm25.get("avgDocLength")).isEqualTo((DOCS * 3.0 + 40) / (DOCS + 7));
    assertThat(bm25.get("countersValid")).isEqualTo(true);
    assertThat(bm25.get("staleCheckDone")).isEqualTo(false);
    assertThat(bm25.get("scans")).isEqualTo(0L);

    assertThat(metadata("Doc[content]").getScans()).isZero();
    assertThat(metadata("Doc[content]").isStaleCheckDone()).isFalse();
  }

  @Test
  void schemaIndexOfAnIndexThatIsNotFullTextHasNoCounters() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Plain");
      database.command("sql", "CREATE PROPERTY Plain.name STRING");
      database.command("sql", "CREATE INDEX ON Plain (name) NOTUNIQUE");
    });

    final ResultSet resultSet = database.query("sql", "SELECT FROM schema:index:`Plain[name]`");
    final Result detail = resultSet.next();
    resultSet.close();

    assertThat(detail.hasProperty("bm25")).isFalse();
  }

  @Test
  void theCountersTheTypeIndexReportsAreTheOnesItsBucketIndexesShare() {
    createFillAndDrift();

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
    final FullTextIndexMetadata viaType = LSMTreeFullTextIndex.metadataOf(typeIndex);
    assertThat(viaType).isNotNull();
    assertThat(typeIndex.getIndexesOnBuckets()).allSatisfy(
        bucketIndex -> assertThat(LSMTreeFullTextIndex.metadataOf(bucketIndex)).isSameAs(viaType));
  }

  /**
   * A crash between commits and the close: the schema was last written before the inserts, the WAL replay restores the pages but
   * carries no corpus deltas, so the counters read from the schema lag the data. The first search finds the live count disagrees
   * and repairs them.
   */
  @Test
  void afterACrashTheFirstSearchRepairsTheCountersTheLogDoesNotCarry() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 3");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.questions STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);

    ((DatabaseInternal) database).kill();
    database.close(); // unregisters the killed instance, as the ACID crash tests do
    database = factory.open();

    assertThat(database.countType("Doc", false)).as("the pages were restored").isEqualTo(DOCS);
    assertThat(metadata("Doc[content]").getTotalDocs()).as("the counters come from the last schema write").isLessThan(DOCS);

    search();

    assertThat(metadata("Doc[content]").getScans()).isEqualTo(1L);
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata("Doc[content]").getSumDocLength()).isEqualTo(DOCS * 3L);
  }

  /** A database opened read-only repairs its counters in memory and never writes its schema. */
  @Test
  void aRepairOnAReadOnlyDatabaseWritesNothing() throws IOException {
    createFillAndDrift();
    final Path schemaFile = new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME).toPath();
    final String before = Files.readString(schemaFile);

    reopenDatabaseInReadOnlyMode();
    search();
    assertThat(metadata("Doc[content]").getScans()).isEqualTo(1L);
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);
    database.close();

    assertThat(Files.readString(schemaFile)).isEqualTo(before);
  }
}
