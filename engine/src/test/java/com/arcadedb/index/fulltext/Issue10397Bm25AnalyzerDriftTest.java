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
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10397: the drift check of the BM25 corpus counters compared the document count only, so it could not see a corpus
 * whose record count was right and whose total length was wrong. The sum of the lengths cannot be compared with the live data
 * without analyzing it, so the check compares what the sum depends on: the counters are persisted with the analyzer (and the
 * rule that counts tokens) they were measured with, and a different one arms the rescan.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10397Bm25AnalyzerDriftTest extends TestHelper {
  private static final int    DOCS             = 10;
  private static final String KEYWORD_ANALYZER = "org.apache.lucene.analysis.core.KeywordAnalyzer";
  private static final String INDEX            = "Doc[content]";

  private void createAndFill() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, DOCS * 3L);
  }

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName(INDEX));
  }

  private void search() {
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('" + INDEX + "', 'alpha') = true").close();
  }

  private Path schemaFile() {
    return new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME).toPath();
  }

  /** Edits the definition of the index while the database is closed, as an operator editing {@code schema.json} would. */
  private void editIndexDefinition(final Consumer<JSONObject> edit) throws IOException {
    database.close();
    final JSONObject schema = new JSONObject(Files.readString(schemaFile()));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Doc").getJSONObject("indexes");
    for (final String name : indexes.keySet())
      edit.accept(indexes.getJSONObject(name));
    Files.writeString(schemaFile(), schema.toString());
    reopenDatabase();
  }

  @Test
  void anAnalyzerChangedBetweenSessionsRescansEvenWhenTheCountIsUnchanged() throws IOException {
    createAndFill();

    editIndexDefinition(index -> index.put("index_analyzer", KEYWORD_ANALYZER));
    // THE RECORD COUNT STILL AGREES WITH THE PERSISTED ONE: ONLY THE LENGTHS ARE WRONG NOW
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);

    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    // A KeywordAnalyzer MAKES ONE TOKEN OF A WHOLE VALUE
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, (long) DOCS);
  }

  @Test
  void anUnchangedAnalyzerScansNothing() {
    createAndFill();

    for (int session = 0; session < 3; session++) {
      reopenDatabase();
      search();

      assertThat(metadata().getScans()).as("session %d", session).isZero();
      assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, DOCS * 3L);
    }
  }

  @Test
  void countersPersistedBeforeTheSignatureExistedAreTrustedAndAdopted() throws IOException {
    createAndFill();
    database.getSchema().getEmbedded().saveConfiguration();

    editIndexDefinition(index -> index.remove("ft_lengthSignature"));
    search();

    assertThat(metadata().getScans()).isZero();
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, DOCS * 3L);
    // ADOPTED: THE NEXT SAVE RECORDS WHAT THEY ARE MEASURED WITH FROM NOW ON
    ((DatabaseInternal) database).getSchema().getEmbedded().saveConfiguration();
    assertThat(indexDefinition().has("ft_lengthSignature")).isTrue();
  }

  /**
   * A schema save between the change and the first search must not record the new analyzer next to the old counters: after a
   * restart the check would then find them consistent, and nothing would ever repair them.
   */
  @Test
  void aSchemaSaveBeforeTheFirstSearchDoesNotForgetTheChange() throws IOException {
    createAndFill();

    editIndexDefinition(index -> index.put("index_analyzer", KEYWORD_ANALYZER));
    ((DatabaseInternal) database).getSchema().getEmbedded().saveConfiguration();
    reopenDatabase();
    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, (long) DOCS);
  }

  @Test
  void aRescanRecordsTheAnalyzerItMeasuredWith() throws IOException {
    createAndFill();
    editIndexDefinition(index -> index.put("index_analyzer", KEYWORD_ANALYZER));
    search();
    assertThat(metadata().getScans()).isEqualTo(1L);

    reopenDatabase();
    search();

    assertThat(metadata().getScans()).as("the repair is paid once").isZero();
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, (long) DOCS);
  }

  @Test
  void anExplicitRecomputeRecordsTheCurrentAnalyzer() throws IOException {
    createAndFill();
    editIndexDefinition(index -> index.put("index_analyzer", KEYWORD_ANALYZER));

    database.command("sql", "REBUILD INDEX `" + INDEX + "` WITH statsOnly = true").close();
    reopenDatabase();
    search();

    assertThat(metadata().getScans()).isZero();
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, (long) DOCS);
  }

  @Test
  void theDescriptionOfTheCountersSaysWhetherTheyAreMeasuredWithTheCurrentAnalyzer() throws IOException {
    createAndFill();
    assertThat(describe().get("measuredWithCurrentAnalyzer")).isEqualTo(true);

    editIndexDefinition(index -> index.put("index_analyzer", KEYWORD_ANALYZER));
    assertThat(describe().get("measuredWithCurrentAnalyzer")).isEqualTo(false);

    search();
    assertThat(describe().get("measuredWithCurrentAnalyzer")).isEqualTo(true);
  }

  /**
   * Pins the rule that counts the tokens of a document to its version. If this fails because the rule changed (what a null
   * value, an empty text or a token counts for), bump {@code FullTextIndexMetadata.LENGTH_MEASURE_VERSION} with the new numbers:
   * the counters persisted by the old rule are then rescanned instead of trusted.
   */
  @Test
  void theRuleThatCountsTheTokensIsPinnedToItsVersion() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.tag STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
      database.command("sql", "INSERT INTO Doc SET content = 'one'");
      database.command("sql", "INSERT INTO Doc SET content = 'the quick brown fox', tag = 'x'");
      database.command("sql", "INSERT INTO Doc SET tag = 'no content'");
    });

    assertThat(metadata().getCountersPair()).as("documents and tokens: a null value is a document of no token").containsExactly(4L, 8L);
    assertThat(FullTextIndexMetadata.LENGTH_MEASURE_VERSION).as("the version of the rule these numbers are of").isEqualTo(1);
  }

  private Map<String, Object> describe() {
    return LSMTreeFullTextIndex.describeCorpusCounters(database.getSchema().getIndexByName(INDEX));
  }

  private JSONObject indexDefinition() throws IOException {
    final JSONObject schema = new JSONObject(Files.readString(schemaFile()));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Doc").getJSONObject("indexes");
    return indexes.getJSONObject(indexes.keySet().iterator().next());
  }
}
