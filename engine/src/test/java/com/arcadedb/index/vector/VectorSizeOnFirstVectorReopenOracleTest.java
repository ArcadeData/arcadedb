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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An index created with {@code sizeOnFirstVector} whose schema was saved before the decision, and whose pages hold the vectors
 * written after it: the database that crashed between the commit that decided the size and the next save of {@code schema.json}.
 * The documentation of {@code deriveDimensionsFromData} promises that such an index takes its size from the vectors its records hold
 * (issues #10915, #10993, #11141). The oracle is the exact cosine ranking of the records.
 * <p>
 * Regression for #13214: the reopened index used to skip the deferred load of its locations while it had no dimensions, so it
 * knew none of the vectors its pages hold: it answered every search as an empty index, accepted a vector of another length as its
 * first one, and handed out vector ids that were already on its pages.
 */
@Tag("vector")
class VectorSizeOnFirstVectorReopenOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int RECORDS    = 30;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void aSearchOnAReopenedUndecidedIndexSeesTheVectorsItsPagesHold() {
    final Map<RID, float[]> live = createAndFill();
    reopenWithTheDecisionForgotten();

    for (final IndexInternal bucketIndex : typeIndex().getIndexesOnBuckets())
      assertThat(((LSMVectorIndex) bucketIndex).getMetadata().dimensions).as("the schema waits for the size").isZero();

    assertMatchesBruteForce(live, 5);
    for (final IndexInternal bucketIndex : typeIndex().getIndexesOnBuckets())
      assertThat(((LSMVectorIndex) bucketIndex).getMetadata().dimensions).isEqualTo(DIMENSIONS);
  }

  @Test
  void aQueryOfAnotherLengthIsRefusedOnAReopenedUndecidedIndex() {
    createAndFill();
    reopenWithTheDecisionForgotten();

    final LSMVectorIndex first = (LSMVectorIndex) typeIndex().getIndexesOnBuckets()[0];
    assertThatThrownBy(() -> first.findNeighborsFromVector(vector(DIMENSIONS + 2, 1), 3))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not match index dimension " + DIMENSIONS);
  }

  @Test
  void aWriteOfAnotherLengthIsRefusedOnAReopenedUndecidedIndex() {
    final Map<RID, float[]> live = createAndFill();
    reopenWithTheDecisionForgotten();

    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument("Doc").set("embedding", vector(DIMENSIONS + 4, 500)).save()))
        .hasStackTraceContaining("does not match index dimension " + DIMENSIONS);

    database.transaction(() -> {
      final float[] vector = vector(DIMENSIONS, 600);
      live.put(database.newDocument("Doc").set("embedding", vector).save().getIdentity(), vector);
    });
    assertMatchesBruteForce(live, 5);
  }

  private Map<RID, float[]> createAndFill() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 3");
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withLSMVectorType();
    builder.withMetadata(new JSONObject().put("similarity", "COSINE").put("sizeOnFirstVector", true));
    builder.create();

    final Map<RID, float[]> live = new LinkedHashMap<>();
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        final float[] vector = vector(DIMENSIONS, i + 1);
        live.put(database.newDocument("Doc").set("embedding", vector).save().getIdentity(), vector);
      }
    });
    return live;
  }

  /** The schema as the node that crashed before saving the decision left it, the pages as it left them. */
  private void reopenWithTheDecisionForgotten() {
    final String path = database.getDatabasePath();
    database.close();
    try {
      final Path schemaFile = Path.of(path, "schema.json");
      final String json = Files.readString(schemaFile);
      final String undecided = json.replaceAll("\"dimensions\"\\s*:\\s*" + DIMENSIONS,
          "\"dimensions\":0,\"sizeOnFirstVector\":true");
      assertThat(undecided).isNotEqualTo(json);
      Files.writeString(schemaFile, undecided);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    database = factory.open();
  }

  private TypeIndex typeIndex() {
    return (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
  }

  private void assertMatchesBruteForce(final Map<RID, float[]> live, final int k) {
    final Random random = new Random(5);
    for (int probe = 0; probe < 4; probe++) {
      final float[] query = vector(DIMENSIONS, random.nextInt(1000) + 2000);
      final List<RID> expected = live.entrySet().stream()
          .sorted(Comparator.comparingDouble((Map.Entry<RID, float[]> e) -> -cosine(query, e.getValue())))
          .limit(k).map(Map.Entry::getKey).toList();

      final List<Pair<RID, Float>> merged = new ArrayList<>();
      for (final IndexInternal bucketIndex : typeIndex().getIndexesOnBuckets())
        merged.addAll(((LSMVectorIndex) bucketIndex).findNeighborsFromVector(query, k, 100));
      final List<RID> actual = merged.stream().sorted(Comparator.comparingDouble((Pair<RID, Float> p) -> p.getSecond()))
          .limit(k).map(Pair::getFirst).toList();
      assertThat(actual).as("probe %d", probe).containsExactlyElementsOf(expected);
    }
  }

  private static double cosine(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return dot / Math.sqrt(na * nb);
  }

  private static float[] vector(final int dimensions, final int seed) {
    final Random random = new Random(seed * 7919L + dimensions);
    final float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++)
      vector[i] = (float) random.nextGaussian();
    vector[0] += 0.5f;
    return vector;
  }
}
