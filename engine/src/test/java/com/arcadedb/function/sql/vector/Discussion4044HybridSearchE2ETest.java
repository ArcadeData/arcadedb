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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end proof that the original use case from
 * <a href="https://github.com/ArcadeData/arcadedb/discussions/4044">discussion #4044</a> is now
 * expressible in a single ArcadeDB SQL statement.
 * <p>
 * The discussion described a Russian text classification service migrating from Qdrant. Each user
 * query produces a 768-dim dense embedding plus a 105K-vocab sparse embedding (SPLADE-style); the
 * service performs a hybrid dense + sparse retrieval with RRF fusion and groups results by
 * {@code source_file} with {@code group_size = 1}, so each XML source contributes its single best
 * matching chunk.
 * <p>
 * This test reproduces that pattern at a small but representative scale (10 source files with 5
 * chunks each, semantically distinct chunk types per source) and asserts:
 * <ul>
 *   <li>The hybrid query is one SQL statement.</li>
 *   <li>The top result is the chunk that should win on both modalities.</li>
 *   <li>{@code groupBy='source_file', groupSize=1} returns each source at most once.</li>
 *   <li>Sparse retrieval separates the «начисление процентов» vs «начисление баллов» pair that
 *       collapses under dense alone.</li>
 * </ul>
 * <p>
 * Companion to issues #4065 (sparse index), #4066 ({@code vector.fuse}), #4067 (groupBy on
 * {@code vector.neighbors}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Discussion4044HybridSearchE2ETest extends TestHelper {

  private static final int    DENSE_DIM   = 64;     // a stand-in for the 768-dim model in #4044
  private static final int    SPARSE_DIM  = 1000;   // stand-in for SPLADE's 105K-token vocabulary
  private static final int    SOURCES     = 10;
  private static final int    CHUNKS_PER  = 5;
  private static final long   SEED        = 4044L;

  private static final String TYPE_NAME   = "Chunk";
  private static final String DENSE_IDX   = "Chunk[dense]";
  private static final String SPARSE_IDX  = "Chunk[tokens,weights]";

  /**
   * The two semantically-similar Russian phrases from the discussion that fail dense-only retrieval
   * but separate cleanly with sparse. The "rare" token id is the shared «начисление» stem; the
   * second token differentiates them.
   */
  private static final int TOKEN_NACHISLENIE = 7;   // shared stem
  private static final int TOKEN_PROTSENTOV  = 11;  // «процентов» (interest)
  private static final int TOKEN_BALLOV      = 13;  // «баллов» (loyalty points)

  /**
   * Build the schema with both indexes ArcadeDB needs for hybrid retrieval.
   */
  private void buildSchema() {
    database.transaction(() -> {
      final DocumentType chunk = database.getSchema().createDocumentType(TYPE_NAME);
      chunk.createProperty("source_file", Type.STRING);
      chunk.createProperty("variant", Type.STRING);
      chunk.createProperty("dense", Type.ARRAY_OF_FLOATS);
      chunk.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      chunk.createProperty("weights", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "dense" })
          .withLSMVectorType()
          .withDimensions(DENSE_DIM)
          .withSimilarity("COSINE")
          .create();

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(SPARSE_DIM)
          .create();
    });
  }

  /**
   * Generate 50 chunks across 10 source files. Source 0's "interest-rate" chunk is the one that
   * should win the hybrid query; source 1's "loyalty-points" chunk is the one that *would* win if
   * sparse retrieval were broken (since dense alone collapses the two) and is the negative control.
   */
  private void seedFixture() {
    database.transaction(() -> {
      final Random rnd = new Random(SEED);
      for (int s = 0; s < SOURCES; s++) {
        final String sourceFile = "doc_" + s + ".xml";
        for (int c = 0; c < CHUNKS_PER; c++) {
          final String variant;
          final int specificToken;
          if (s == 0 && c == 0) {
            variant = "interest_rates";
            specificToken = TOKEN_PROTSENTOV;
          } else if (s == 1 && c == 0) {
            variant = "loyalty_points";
            specificToken = TOKEN_BALLOV;
          } else {
            variant = "noise_" + s + "_" + c;
            specificToken = 100 + rnd.nextInt(SPARSE_DIM - 100);
          }

          final MutableDocument doc = database.newDocument(TYPE_NAME);
          doc.set("source_file", sourceFile);
          doc.set("variant", variant);
          doc.set("dense", buildDense(variant, rnd));
          doc.set("tokens", buildTokens(variant, specificToken));
          doc.set("weights", buildWeights(variant));
          doc.save();
        }
      }
    });
  }

  /**
   * Dense embeddings are deliberately near-collinear for the "interest_rates" and "loyalty_points"
   * chunks - the dense model can't tell «процентов» from «баллов» given the shared «начисление»
   * stem. This is the core observation from #4044.
   */
  private static float[] buildDense(final String variant, final Random rnd) {
    final float[] v = new float[DENSE_DIM];
    if (variant.startsWith("interest_rates") || variant.startsWith("loyalty_points")) {
      // Both chunks live near the same point in the dense space; tiny perturbation only.
      v[0] = 1.0f;
      v[1] = 0.05f + rnd.nextFloat() * 0.01f;
      v[2] = 0.03f + rnd.nextFloat() * 0.01f;
    } else {
      // "noise" chunks are spread across the dense space so they don't accidentally rank high.
      for (int i = 0; i < DENSE_DIM; i++)
        v[i] = rnd.nextFloat();
    }
    return v;
  }

  private static int[] buildTokens(final String variant, final int specificToken) {
    if (variant.equals("interest_rates") || variant.equals("loyalty_points"))
      return new int[] { TOKEN_NACHISLENIE, specificToken, 23 };
    return new int[] { specificToken, 47, 91 };
  }

  private static float[] buildWeights(final String variant) {
    if (variant.equals("interest_rates") || variant.equals("loyalty_points"))
      return new float[] { 0.6f, 0.9f, 0.2f };
    return new float[] { 0.3f, 0.2f, 0.1f };
  }

  /**
   * The user wants documents about "interest rates" - their query has the «начисление» stem (token
   * 7) plus the «процентов» token (11) heavily weighted. The hybrid query must surface
   * source_file=doc_0.xml ahead of doc_1.xml even though dense alone would tie them.
   */
  @Test
  void hybridQueryMatches4044UseCase() {
    buildSchema();
    seedFixture();

    final float[] denseQuery = new float[DENSE_DIM];
    denseQuery[0] = 1.0f;
    denseQuery[1] = 0.05f;
    denseQuery[2] = 0.03f;

    // Sparse query mirrors astarso's payload: stem + specific term, both heavily weighted.
    final int[]   sparseTokens  = new int[] { TOKEN_NACHISLENIE, TOKEN_PROTSENTOV };
    final float[] sparseWeights = new float[] { 0.6f, 0.9f };

    // The full Qdrant-equivalent of:
    //
    //   query_points_groups(
    //     prefetch=[
    //       Prefetch(query=dense_vec,  using="dense",  limit=50),
    //       Prefetch(query=sparse_vec, using="sparse", limit=50),
    //     ],
    //     query=FusionQuery(fusion=Fusion.RRF),
    //     group_by="source_file",
    //     group_size=1,
    //   )
    //
    // is now a single ArcadeDB SQL statement:
    final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.fuse`(
            `vector.neighbors`(?, ?, ?),
            `vector.sparseNeighbors`(?, ?, ?, ?),
            { fusion: 'RRF', groupBy: 'source_file', groupSize: 1 }
        ))""",
        DENSE_IDX, denseQuery, 50,
        SPARSE_IDX, sparseTokens, sparseWeights, 50);

    final Set<String>             seenSources = new HashSet<>();
    final Map<String, String>     firstVariantPerSource = new LinkedHashMap<>();
    final Map<String, Float>      firstScorePerSource   = new HashMap<>();
    String topSource = null;
    String topVariant = null;
    float  topScore = Float.NEGATIVE_INFINITY;
    int rank = 0;

    while (rs.hasNext()) {
      final Result row = rs.next();
      final String sourceFile = row.getProperty("source_file");
      final String variant    = row.getProperty("variant");
      final float  score      = ((Number) row.getProperty("score")).floatValue();

      // groupSize=1 must collapse same-source duplicates.
      assertThat(seenSources.add(sourceFile))
          .as("source_file %s appears more than once", sourceFile)
          .isTrue();

      if (rank == 0) {
        topSource = sourceFile;
        topVariant = variant;
        topScore = score;
      }
      firstVariantPerSource.put(sourceFile, variant);
      firstScorePerSource.put(sourceFile, score);
      rank++;
    }

    // 1. The query has to find at least one match.
    assertThat(rank).as("hybrid query must return results").isPositive();

    // 2. The top result is the interest-rates chunk in doc_0.xml: dense alone would tie 0 and 1,
    //    sparse breaks the tie because doc_0 contains «процентов» (token 11) and doc_1 contains
    //    «баллов» (token 13).
    assertThat(topSource).as("top source").isEqualTo("doc_0.xml");
    assertThat(topVariant).as("top variant").isEqualTo("interest_rates");
    assertThat(topScore).as("top score is positive").isPositive();

    // 3. Negative control: the loyalty_points chunk in doc_1.xml exists in the corpus and would
    //    rank #1 if sparse were broken (dense collapses both). Under hybrid retrieval it must rank
    //    *after* doc_0.xml.
    if (firstVariantPerSource.containsKey("doc_1.xml")) {
      final List<String> ordered = new java.util.ArrayList<>(firstVariantPerSource.keySet());
      assertThat(ordered.indexOf("doc_0.xml"))
          .as("doc_0 must rank before doc_1 (sparse breaks the tie)")
          .isLessThan(ordered.indexOf("doc_1.xml"));
    }

    // 4. groupSize=1 guarantee: distinct source_files only.
    assertThat(seenSources.size()).as("each source_file appears at most once").isEqualTo(rank);
  }
}
