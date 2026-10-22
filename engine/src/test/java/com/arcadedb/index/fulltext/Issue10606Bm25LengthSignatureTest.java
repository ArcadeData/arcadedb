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
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10606: the persisted BM25 pair (document count, sum of the document lengths) was guarded by the count and by a
 * signature, {@code analyzer class name + '#' + LENGTH_MEASURE_VERSION}, standing in for the sum, which nothing compares. Three
 * routes broke the inference and left every score wrong with the count right and nothing arming a rescan:
 * <ol>
 *   <li>the version was a constant somebody had to remember to bump;</li>
 *   <li>the class NAME stood for the analyzer, so an analyzer whose tokenization changed under the same name (a Lucene upgrade) was
 *   invisible;</li>
 *   <li>the sum was clamped at zero with the count and the signature intact, so an arithmetic error became a permanently wrong
 *   average length.</li>
 * </ol>
 * The signature is now derived from what it describes (the lengths the real analyzer and the real rule produce over a fixed
 * corpus, plus the Lucene version), a clamp is counted and arms a rescan, and the one regime in which the sum can be read without a
 * scan (an empty corpus holds no token) is checked.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10606Bm25LengthSignatureTest extends TestHelper {
  private static final int    DOCS  = 10;
  private static final String INDEX = "Doc[content]";

  /**
   * An analyzer whose behaviour is switched from outside while its class name stays the same: what a library upgrade does to
   * {@code StandardAnalyzer} between two runs of the same application.
   */
  public static final class MovingAnalyzer extends Analyzer {
    static volatile boolean wholeValueIsOneToken = false;

    public MovingAnalyzer() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
      return new TokenStreamComponents(wholeValueIsOneToken ? new KeywordTokenizer() : new WhitespaceTokenizer());
    }
  }

  @AfterEach
  void restoreTheMovingAnalyzer() {
    MovingAnalyzer.wholeValueIsOneToken = false;
  }

  // ---- route 2: the analyzer is told by what it does, not by its name

  @Test
  void anAnalyzerThatChangesUnderTheSameNameChangesTheSignature() {
    // ONE INSTANCE PER STATE: A LUCENE ANALYZER KEEPS THE COMPONENTS IT BUILT FIRST, AS AN APPLICATION RESTARTED ON ANOTHER LIBRARY DOES
    final String before;
    try (final MovingAnalyzer analyzer = new MovingAnalyzer()) {
      before = FullTextLengthSignature.of(analyzer);
    }

    MovingAnalyzer.wholeValueIsOneToken = true;
    try (final MovingAnalyzer analyzer = new MovingAnalyzer()) {
      assertThat(FullTextLengthSignature.of(analyzer)).isNotEqualTo(before);
    }
  }

  @Test
  void theSignatureOfAnUnchangedAnalyzerIsStable() {
    try (final StandardAnalyzer first = new StandardAnalyzer(); final StandardAnalyzer second = new StandardAnalyzer()) {
      assertThat(FullTextLengthSignature.of(first)).isEqualTo(FullTextLengthSignature.of(second)).isEqualTo(FullTextLengthSignature.of(first));
    }
  }

  @Test
  void aLuceneUpgradeIsPartOfTheSignature() {
    try (final StandardAnalyzer analyzer = new StandardAnalyzer()) {
      assertThat(FullTextLengthSignature.of(analyzer)).contains(Version.LATEST.toString());
    }
  }

  // ---- route 1: the length rule is told by what it counts, not by a number somebody bumps

  @Test
  void aChangeOfTheLengthRuleChangesTheSignature() {
    try (final StandardAnalyzer analyzer = new StandardAnalyzer()) {
      final String current = FullTextLengthSignature.of(analyzer);

      // a rule that counts the null token of a null value as a token: the numbers of a four-document fixture need not move
      final String countsNulls = FullTextLengthSignature.of(analyzer, List::size);
      // a rule that counts one token less per value
      final String countsOneLess = FullTextLengthSignature.of(analyzer, tokens -> Math.max(0, tokens.size() - 1));

      assertThat(countsNulls).isNotEqualTo(current);
      assertThat(countsOneLess).isNotEqualTo(current).isNotEqualTo(countsNulls);
    }
  }

  @Test
  void theMetadataBindsTheRuleOfItsIndexIntoTheSignature() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    final String unbound = metadata.getLengthSignature();

    metadata.bindLengthRule("lucene-x:abc");

    assertThat(metadata.getLengthSignature()).isNotEqualTo(unbound).isEqualTo(metadata.getIndexAnalyzerClass() + "#lucene-x:abc");
  }

  // ---- route 3: a clamp is not a floor

  @Test
  void aRemovalBelowZeroIsCountedAndArmsTheRescan() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(1, 3);
    assertThat(metadata.getClamps()).isZero();

    metadata.removeDocument(3L);
    assertThat(metadata.getClamps()).as("an exact removal clamps nothing").isZero();
    assertThat(metadata.consumeRescanRequired()).isFalse();

    metadata.removeDocument(3L);

    assertThat(metadata.getClamps()).isPositive();
    assertThat(metadata.consumeRescanRequired()).isTrue();
    assertThat(metadata.getCountersPair()).containsExactly(0L, 0L);
  }

  @Test
  void aSumDrivenBelowZeroWithTheCountIntactIsCounted() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(5, 2);

    // one document of 9 tokens leaves while the sum holds 2: the count is fine, the sum cannot be
    metadata.removeDocument(9L);

    assertThat(metadata.getClamps()).isPositive();
    assertThat(metadata.consumeRescanRequired()).isTrue();
  }

  @Test
  void aCommitDeltaBelowZeroIsCounted() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(5, 10);

    metadata.applyCorpusDelta(-1, -30);

    assertThat(metadata.getClamps()).isPositive();
    assertThat(metadata.consumeRescanRequired()).isTrue();
  }

  @Test
  void aScanPublishedOverCountersThatDroveItBelowZeroIsNotExact() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(5, 10);
    final long mutationsAtStart = metadata.getMutations();

    // the scan found an empty corpus, and the counters sit at the values it started from, so the delta takes them below zero
    // exactly where the scan result is zero: the clamp makes the published pair EQUAL to the scan result by accident
    final boolean exact = metadata.publishScannedCounters(0, 0, 10, 40, mutationsAtStart);

    assertThat(metadata.getClamps()).isPositive();
    assertThat(exact).isFalse();
    assertThat(metadata.consumeRescanRequired()).isTrue();
  }

  @Test
  void theCountersDescribeTheirClamps() {
    createAndFill();
    metadata().removeDocument(1L);
    for (int i = 0; i < DOCS + 1; i++)
      metadata().removeDocument(1L);

    assertThat(LSMTreeFullTextIndex.describeCorpusCounters(database.getSchema().getIndexByName(INDEX)).get("clamps")).isNotEqualTo(0L);
  }

  // ---- the pair: the sum is read where it can be

  @Test
  void anEmptyCorpusHoldsNoTokensWhateverTheSumSays() throws IOException {
    createAndFill();
    database.transaction(() -> database.command("sql", "DELETE FROM Doc"));
    assertThat(metadata().getCountersPair()).containsExactly(0L, 0L);

    // the persisted pair is torn the way a crash or a clamp tears it: the count is right (no documents), the sum is not
    editIndexDefinition(index -> index.put("ft_sumDocLength", 5L));
    assertThat(metadata().getCountersPair()).containsExactly(0L, 5L);

    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getCountersPair()).containsExactly(0L, 0L);
  }

  @Test
  void anAnalyzerBehaviourChangedUnderTheSameClassNameRescansEvenWhenTheCountIsUnchanged() {
    createAndFill(MovingAnalyzer.class.getName());
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, DOCS * 3L);

    // THE SAME CLASS NAME, ANOTHER TOKENIZATION: THE NEXT SESSION MEASURES A WHOLE VALUE AS ONE TOKEN
    database.close();
    MovingAnalyzer.wholeValueIsOneToken = true;
    reopenDatabase();
    assertThat(metadata().getSumDocLength()).as("the persisted pair is the old one").isEqualTo(DOCS * 3L);

    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, (long) DOCS);
  }

  @Test
  void anUnchangedAnalyzerScansNothingInTheNextSessions() {
    createAndFill(MovingAnalyzer.class.getName());

    for (int session = 0; session < 3; session++) {
      reopenDatabase();
      search();

      assertThat(metadata().getScans()).as("session %d", session).isZero();
      assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, DOCS * 3L);
    }
  }

  private void createAndFill() {
    createAndFill(null);
  }

  private void createAndFill(final String analyzerClass) {
    final String metadata = analyzerClass == null ? "{\"similarity\": \"BM25\"}" : "{\"similarity\": \"BM25\", \"analyzer\": \"" + analyzerClass + "\"}";
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA " + metadata);
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
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
}
