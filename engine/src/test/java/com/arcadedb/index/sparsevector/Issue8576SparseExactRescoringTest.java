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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LSMSparseVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #8576: the INT8 weights an {@code LSM_SPARSE_VECTOR} index stores drift with every compaction merge (each merge
 * re-quantizes the decoded weights onto the grid of a new block), so its scores - and its top-K - depended on how the
 * same data had been committed and merged. A search now rescores an oversampled candidate set exactly, from the
 * records' own full-precision weights, so the quantized scores only choose the candidates and never the answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8576SparseExactRescoringTest extends TestHelper {

  /**
   * Two records whose weights fall on the same INT8 level of their block: the quantized index scores them as a tie and
   * breaks it by RID, so with {@code k = 1} it answers the lower RID although the other one scores higher. Oversampling
   * brings the true winner into the candidate set and the exact rescoring puts it first, with its exact score.
   */
  @Test
  void exactRescoringPicksTheTrueWinnerTheQuantizedScoresTie() throws Exception {
    for (final int oversample : new int[] { 0, 1, 2 })
      createType("Tie" + oversample, "{ dimensions: 10, rescoreOversample: " + oversample + " }");

    for (final int oversample : new int[] { 0, 1, 2 }) {
      final String type = "Tie" + oversample;
      database.transaction(() -> {
        // A wide block range (0.1 .. 10) makes one INT8 step ~0.039, so 5.001 and 5.010 land on the same level.
        database.newDocument(type).set("name", "low", "tokens", new int[] { 1 }, "weights", new float[] { 5.001f }).save();
        database.newDocument(type).set("name", "high", "tokens", new int[] { 1 }, "weights", new float[] { 5.010f }).save();
        database.newDocument(type).set("name", "min", "tokens", new int[] { 1 }, "weights", new float[] { 0.1f }).save();
        database.newDocument(type).set("name", "max", "tokens", new int[] { 1 }, "weights", new float[] { 10.0f }).save();
      });
      // Seal the memtable (whose weights are still full precision) into an INT8 segment.
      assertThat(sparseIndex(type).compact()).isTrue();
    }

    final int[] query = { 1 };
    final float[] weights = { 1.0f };

    // Off: the quantized tie is broken by RID, and the score is the quantized one.
    final List<RidScore> off = sparseIndex("Tie0").topK(query, weights, 2, null);
    assertThat(off.get(0).score()).isCloseTo(10.0f, within(1e-5f));
    assertThat(name(off.get(1).rid())).isEqualTo("low");
    assertThat(off.get(1).score()).isNotEqualTo(5.001f);

    // 1: no wider candidate set, but the score of what it returns is exact.
    final List<RidScore> exactScoresOnly = sparseIndex("Tie1").topK(query, weights, 2, null);
    assertThat(name(exactScoresOnly.get(1).rid())).isEqualTo("low");
    assertThat(exactScoresOnly.get(1).score()).isEqualTo(5.001f);

    // 2: the true winner is in the candidate set and comes out first, with its exact score.
    final List<RidScore> rescored = sparseIndex("Tie2").topK(query, weights, 2, null);
    assertThat(name(rescored.get(0).rid())).isEqualTo("max");
    assertThat(name(rescored.get(1).rid())).isEqualTo("high");
    assertThat(rescored.get(1).score()).isEqualTo(5.010f);
  }

  /**
   * The core claim of the issue: the same vectors committed and merged along different histories must answer the same
   * top-K with the same scores. One type is settled once; the other is compacted after every batch, so each of its
   * INT8 postings goes through several merges (and several re-quantizations).
   */
  @Test
  void answersDoNotDependOnTheCommitAndMergeHistory() throws Exception {
    createType("Once", "{ dimensions: 64 }");
    createType("Often", "{ dimensions: 64 }");

    final Random random = new Random(8576);
    final int docs = 600;
    final int[][] tokens = new int[docs][];
    final float[][] values = new float[docs][];
    for (int i = 0; i < docs; i++) {
      final int nnz = 4 + random.nextInt(8);
      final int[] t = random.ints(0, 64).distinct().limit(nnz).toArray();
      final float[] w = new float[t.length];
      for (int j = 0; j < w.length; j++)
        w[j] = (float) Math.exp(random.nextGaussian());
      tokens[i] = t;
      values[i] = w;
    }

    database.transaction(() -> {
      for (int i = 0; i < docs; i++)
        database.newDocument("Once").set("name", "d" + i, "tokens", tokens[i], "weights", values[i]).save();
    });
    sparseIndex("Once").compact();

    for (int from = 0; from < docs; from += 50) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < start + 50; i++)
          database.newDocument("Often").set("name", "d" + i, "tokens", tokens[i], "weights", values[i]).save();
      });
      sparseIndex("Often").compact();
    }

    for (int q = 0; q < 50; q++) {
      final int[] queryTokens = random.ints(0, 64).distinct().limit(3 + random.nextInt(5)).toArray();
      final float[] queryWeights = new float[queryTokens.length];
      for (int j = 0; j < queryWeights.length; j++)
        queryWeights[j] = 0.1f + random.nextFloat();

      final Map<String, Float> once = named(sparseIndex("Once").topK(queryTokens, queryWeights, 10, null));
      final Map<String, Float> often = named(sparseIndex("Often").topK(queryTokens, queryWeights, 10, null));
      assertThat(often).as("query %d", q).containsExactlyEntriesOf(once);

      // And every score is the exact dot product over the stored floats.
      for (final Map.Entry<String, Float> hit : once.entrySet()) {
        final int doc = Integer.parseInt(hit.getKey().substring(1));
        assertThat(hit.getValue()).isCloseTo((float) dot(queryTokens, queryWeights, tokens[doc], values[doc]), within(1e-5f));
      }
    }
  }

  /**
   * The rescored score is the engine's formula term for term: IDF-scaled query weights and a dimension asked twice
   * counting twice. An FP32 index (whose index scores are exact) over the same records is the reference.
   */
  @Test
  void rescoredScoresMatchTheExactIndexFormulaIncludingIdf() {
    createType("I8", "{ dimensions: 32, modifier: 'IDF' }");
    createType("F32", "{ dimensions: 32, modifier: 'IDF', weightQuantization: 'FP32' }");

    final Random random = new Random(42);
    for (final String type : new String[] { "I8", "F32" }) {
      final Random data = new Random(7);
      database.transaction(() -> {
        for (int i = 0; i < 200; i++) {
          final int[] t = data.ints(0, 32).distinct().limit(2 + data.nextInt(6)).toArray();
          final float[] w = new float[t.length];
          for (int j = 0; j < w.length; j++)
            w[j] = 0.01f + data.nextFloat() * 3;
          database.newDocument(type).set("name", "d" + i, "tokens", t, "weights", w).save();
        }
      });
    }
    assertThat(((LSMSparseVectorIndexMetadata) sparseIndex("F32").getMetadataForNewFile()).effectiveRescoreOversample())
        .as("FP32 scores are already exact: no rescoring by default").isZero();

    for (final String type : new String[] { "I8", "F32" })
      try {
        sparseIndex(type).compact();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }

    for (int q = 0; q < 20; q++) {
      final int d = random.nextInt(32);
      // The same dimension asked twice, plus two others.
      final int[] queryTokens = { d, (d + 7) % 32, d, (d + 13) % 32 };
      final float[] queryWeights = { 0.5f, 1.0f, 0.25f, 2.0f };
      final Map<String, Float> expected = named(sparseIndex("F32").topK(queryTokens, queryWeights, 10, null));
      final Map<String, Float> actual = named(sparseIndex("I8").topK(queryTokens, queryWeights, 10, null));
      assertThat(actual.keySet()).containsExactlyElementsOf(expected.keySet());
      for (final Map.Entry<String, Float> e : expected.entrySet())
        assertThat(actual.get(e.getKey())).isCloseTo(e.getValue(), within(1e-5f));
    }
  }

  /** Rows the transaction itself queued are merged, full precision, with the rescored committed ones. */
  @Test
  void pendingRowsMergeWithRescoredCommittedRows() throws Exception {
    createType("Tx", "{ dimensions: 10 }");
    database.transaction(() -> {
      database.newDocument("Tx").set("name", "low", "tokens", new int[] { 1 }, "weights", new float[] { 5.001f }).save();
      database.newDocument("Tx").set("name", "min", "tokens", new int[] { 1 }, "weights", new float[] { 0.1f }).save();
      database.newDocument("Tx").set("name", "max", "tokens", new int[] { 1 }, "weights", new float[] { 10.0f }).save();
    });
    sparseIndex("Tx").compact();

    database.begin();
    try {
      database.newDocument("Tx").set("name", "pending", "tokens", new int[] { 1 }, "weights", new float[] { 5.005f }).save();
      final List<String> names = neighbors("Tx", new int[] { 1 }, new float[] { 1.0f }, 3);
      assertThat(names).containsExactly("max", "pending", "low");
    } finally {
      database.rollback();
    }
  }

  /** Through SQL, over several buckets: each bucket's search may run on a scoring-pool worker, which rescores too. */
  @Test
  void sqlFunctionOverSeveralBucketsReturnsExactOrder() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Multi BUCKETS 4");
    database.command("sql", "CREATE PROPERTY Multi.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY Multi.weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE PROPERTY Multi.name STRING");
    database.command("sql", "CREATE INDEX ON Multi (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 10 }");

    final Random random = new Random(3);
    database.transaction(() -> {
      for (int i = 0; i < 400; i++) {
        // Fillers spread over 0.01 .. 4.9 widen every block to one INT8 step of ~0.02, so the ten targets 0.0001
        // apart all share its top level and the quantized index can only break their tie by RID.
        database.newDocument("Multi").set("name", "f" + i, "tokens", new int[] { 1 },
            "weights", new float[] { 0.01f + random.nextFloat() * 4.89f }).save();
        if (i % 40 == 0)
          database.newDocument("Multi").set("name", "t" + i / 40, "tokens", new int[] { 1 },
              "weights", new float[] { 5.0f + (i / 40) * 0.0001f }).save();
      }
    });
    for (final var idx : ((TypeIndex) database.getSchema().getIndexByName("Multi[tokens,weights]")).getIndexesOnBuckets())
      idx.compact();

    assertThat(neighbors("Multi", new int[] { 1 }, new float[] { 1.0f }, 5)).containsExactly("t9", "t8", "t7", "t6", "t5");
  }

  /**
   * The grouped search decides both which groups win and which members fill them on the exact scores too: with the
   * quantized tie the winning member of group B ("high") and the losing one of group A ("low") would be ranked by RID.
   */
  @Test
  void groupedSearchRanksGroupsAndMembersOnExactScores() throws Exception {
    for (final int oversample : new int[] { 0, 2 }) {
      final String type = "Grp" + oversample;
      createType(type, "{ dimensions: 10, rescoreOversample: " + oversample + " }");
      database.command("sql", "CREATE PROPERTY " + type + ".category STRING");
      database.transaction(() -> {
        // "high" and "low" share one INT8 level; the quantized pass breaks that tie towards group B, so towards "low".
        database.newDocument(type).set("name", "high", "category", "A", "tokens", new int[] { 1 }, "weights",
            new float[] { 5.010f }).save();
        database.newDocument(type).set("name", "low", "category", "B", "tokens", new int[] { 1 }, "weights", new float[] { 5.001f })
            .save();
        database.newDocument(type).set("name", "min", "category", "C", "tokens", new int[] { 1 }, "weights", new float[] { 0.1f })
            .save();
        database.newDocument(type).set("name", "max", "category", "C", "tokens", new int[] { 1 }, "weights", new float[] { 10.0f })
            .save();
      });
      sparseIndex(type).compact();
    }

    final Function<RID, Object> byCategory = rid -> rid.asDocument().getString("category");
    final List<RidScore> off = sparseIndex("Grp0").topKGrouped(new int[] { 1 }, new float[] { 1.0f }, 2, 1, null, byCategory);
    assertThat(named(off).keySet()).containsExactly("max", "low");

    final List<RidScore> rescored = sparseIndex("Grp2").topKGrouped(new int[] { 1 }, new float[] { 1.0f }, 2, 1, null, byCategory);
    assertThat(named(rescored)).containsExactly(Map.entry("max", 10.0f), Map.entry("high", 5.010f));

    // The second phase of a merged grouped search: the floor is an exact score, and "low" (5.001) is above 5.0 exactly
    // although its quantized score is not.
    final List<RidScore> forGroups = sparseIndex("Grp2").topKForGroups(new int[] { 1 }, new float[] { 1.0f },
        Set.of("B", "A"), 1, 5.0f, null, byCategory, null);
    assertThat(named(forGroups)).containsExactly(Map.entry("high", 5.010f), Map.entry("low", 5.001f));
  }

  /** Through SQL with groupBy over several buckets: the cross-bucket planner works on exact scores end to end. */
  @Test
  void sqlGroupedSearchOverSeveralBucketsReturnsExactOrder() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE MultiG BUCKETS 4");
    database.command("sql", "CREATE PROPERTY MultiG.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY MultiG.weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE PROPERTY MultiG.name STRING");
    database.command("sql", "CREATE PROPERTY MultiG.category STRING");
    database.command("sql", "CREATE INDEX ON MultiG (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 10 }");

    final Random random = new Random(5);
    database.transaction(() -> {
      for (int i = 0; i < 400; i++) {
        database.newDocument("MultiG").set("name", "f" + i, "category", "f" + i, "tokens", new int[] { 1 },
            "weights", new float[] { 0.01f + random.nextFloat() * 4.89f }).save();
        if (i % 20 == 0)
          // Ten categories of two targets each, all within one INT8 step of each other.
          database.newDocument("MultiG").set("name", "t" + i / 20, "category", "c" + (i / 20) % 10, "tokens", new int[] { 1 },
              "weights", new float[] { 5.0f + (i / 20) * 0.0001f }).save();
      }
    });
    for (final var idx : ((TypeIndex) database.getSchema().getIndexByName("MultiG[tokens,weights]")).getIndexesOnBuckets())
      idx.compact();

    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?, ?))",
        "MultiG[tokens,weights]", new int[] { 1 }, new float[] { 1.0f }, 3, Map.of("groupBy", "category", "groupSize", 1))) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    // t19..t10 are the best of categories c9..c0; the three best groups are c9, c8, c7 through t19, t18, t17.
    assertThat(names).containsExactly("t19", "t18", "t17");
  }

  /** compact() settles the WHOLE index: the memtable is sealed too, not left behind unquantized (issue #8576, Q3). */
  @Test
  void compactSealsTheMemtable() throws Exception {
    createType("Settle", "{ dimensions: 10 }");
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newDocument("Settle").set("name", "d" + i, "tokens", new int[] { i % 10 }, "weights", new float[] { 1.0f }).save();
    });
    final LSMSparseVectorIndex index = sparseIndex("Settle");
    assertThat(index.getStats().get("memtablePostings")).isEqualTo(20L);

    assertThat(index.compact()).as("sealing the memtable is work done").isTrue();

    assertThat(index.getStats().get("memtablePostings")).isZero();
    assertThat(index.getStats().get("segmentCount")).isEqualTo(1L);
    assertThat(index.compact()).as("nothing left to settle").isFalse();
  }

  /** The setting is validated, persisted only when chosen, and survives a reopen. */
  @Test
  void rescoreOversampleIsValidatedAndPersisted() {
    createType("Conf", "{ dimensions: 10, rescoreOversample: 5 }");
    createType("Auto", "{ dimensions: 10 }");

    assertThat(sparseIndex("Conf").toJSON().getInt("rescoreOversample")).isEqualTo(5);
    assertThat(sparseIndex("Auto").toJSON().has("rescoreOversample")).as("the default is not written").isFalse();
    assertThat(((LSMSparseVectorIndexMetadata) sparseIndex("Auto").getMetadataForNewFile()).effectiveRescoreOversample())
        .isEqualTo(LSMSparseVectorIndexMetadata.DEFAULT_RESCORE_OVERSAMPLE);

    reopenDatabase();

    assertThat(sparseIndex("Conf").toJSON().getInt("rescoreOversample")).isEqualTo(5);
    assertThat(((LSMSparseVectorIndexMetadata) sparseIndex("Conf").getMetadataForNewFile()).effectiveRescoreOversample()).isEqualTo(5);

    assertThatThrownBy(() -> createType("Bad", "{ dimensions: 10, rescoreOversample: -2 }")).hasMessageContaining("rescoreOversample");
    assertThatThrownBy(() -> createType("Bad2", "{ dimensions: 10, rescoreOversample: 101 }")).hasMessageContaining("rescoreOversample");

    final LSMSparseVectorIndexMetadata metadata = new LSMSparseVectorIndexMetadata("T", new String[] { "a", "b" }, 0);
    assertThatThrownBy(() -> metadata.setRescoreOversample(1000)).isInstanceOf(IndexException.class);
    metadata.fromJSON(new JSONObject().put("rescoreOversample", 0));
    assertThat(metadata.effectiveRescoreOversample()).isZero();
  }

  @Test
  void builderSetsRescoreOversample() {
    database.command("sql", "CREATE DOCUMENT TYPE Built");
    database.command("sql", "CREATE PROPERTY Built.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY Built.weights ARRAY_OF_FLOATS");
    database.getSchema().buildTypeIndex("Built", new String[] { "tokens", "weights" }).withType(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR)
        .withSparseVectorType().withDimensions(10).withRescoreOversample(4).create();
    assertThat(sparseIndex("Built").toJSON().getInt("rescoreOversample")).isEqualTo(4);
  }

  private void createType(final String type, final String metadata) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY " + type + ".weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
    database.command("sql", "CREATE INDEX ON " + type + " (tokens, weights) LSM_SPARSE_VECTOR METADATA " + metadata);
  }

  private LSMSparseVectorIndex sparseIndex(final String type) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(type + "[tokens,weights]");
    return (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private String name(final RID rid) {
    return rid.asDocument().getString("name");
  }

  private Map<String, Float> named(final List<RidScore> hits) {
    final Map<String, Float> out = new LinkedHashMap<>();
    for (final RidScore hit : hits)
      out.put(name(hit.rid()), hit.score());
    return out;
  }

  private static double dot(final int[] queryTokens, final float[] queryWeights, final int[] tokens, final float[] values) {
    double score = 0;
    for (int i = 0; i < queryTokens.length; i++)
      for (int j = 0; j < tokens.length; j++)
        if (queryTokens[i] == tokens[j])
          score += (double) queryWeights[i] * values[j];
    return score;
  }

  private List<String> neighbors(final String type, final int[] tokens, final float[] weights, final int k) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        type + "[tokens,weights]", tokens, weights, k)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        names.add(r.getProperty("name"));
      }
    }
    return names;
  }
}
