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
 * SPDX-FileCopyrightText: Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13027: the {@code dimensions} of an LSM_VECTOR index was validated in the SQL path and not in the Cypher one, so
 * the same declaration was a client error in one and a server fault in the other: a non-positive {@code dimensions} was
 * refused with a 400 by {@code CREATE INDEX ... METADATA} and reached the index creation through
 * {@code CREATE VECTOR INDEX ... OPTIONS {indexConfig: {dimensions: 0}}}, which refused it as a failed build (HTTP 500).
 * The other way round, the SQL path refused the {@code sizeOnFirstVector} clause that the engine's own refusal names as
 * the alternative, while the Cypher path accepted it and the index it created worked.
 */
class Issue13027VectorIndexDimensionsPathsTest extends TestHelper {
  private static final int DIMENSIONS = 8;

  @Test
  void sqlAcceptsAnIndexThatTakesItsDimensionsFromItsFirstVector() {
    createType("Doc");

    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {sizeOnFirstVector: true}");

    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isTrue();
    assertThat(bucketMetadata("Doc[embedding]").sizeOnFirstVector).isTrue();

    // the index takes the dimensions from the first vector written, and answers the search with it
    final RID[] rid = new RID[1];
    database.transaction(
        () -> rid[0] = database.newDocument("Doc").set("embedding", vector(DIMENSIONS, 1)).save().getIdentity());

    assertThat(bucketMetadata("Doc[embedding]").dimensions).isEqualTo(DIMENSIONS);
    assertThat(neighbors("Doc[embedding]", vector(DIMENSIONS, 1))).containsExactly(rid[0]);
  }

  @Test
  void sqlStillRefusesANonPositiveDimensions() {
    createType("Doc");

    for (final String dimensions : new String[] { "0", "-1" })
      assertThatThrownBy(() -> database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + dimensions + "}"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("dimensions");

    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();
  }

  @Test
  void cypherRefusesANonPositiveDimensionsAsAClientError() {
    for (final String dimensions : new String[] { "0", "-1" }) {
      assertThatThrownBy(() -> database.command("opencypher",
          "CREATE VECTOR INDEX v FOR (n:Fresh) ON n.e OPTIONS {indexConfig: {dimensions: " + dimensions + "}}"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("dimensions");

      // the statement is refused before it creates the label or the property for the index
      assertThat(database.getSchema().existsType("Fresh")).isFalse();
      assertThat(database.getSchema().existsIndex("v")).isFalse();
    }
  }

  @Test
  void cypherAcceptsAnIndexThatTakesItsDimensionsFromItsFirstVector() {
    database.command("opencypher", "CREATE VECTOR INDEX v FOR (n:Sized) ON n.e");

    assertThat(bucketMetadata("v").sizeOnFirstVector).isTrue();

    final RID[] rid = new RID[1];
    database.transaction(
        () -> rid[0] = database.newVertex("Sized").set("e", vector(DIMENSIONS, 1)).save().getIdentity());

    assertThat(bucketMetadata("v").dimensions).isEqualTo(DIMENSIONS);
    assertThat(neighbors("v", vector(DIMENSIONS, 1))).containsExactly(rid[0]);
  }

  @Test
  void cypherAcceptsANonPositiveDimensionsWithSizeOnFirstVector() {
    database.command("opencypher",
        "CREATE VECTOR INDEX v FOR (n:Sized) ON n.e OPTIONS {indexConfig: {dimensions: 0, sizeOnFirstVector: true}}");

    assertThat(bucketMetadata("v").sizeOnFirstVector).isTrue();

    final RID[] rid = new RID[1];
    database.transaction(
        () -> rid[0] = database.newVertex("Sized").set("e", vector(DIMENSIONS, 1)).save().getIdentity());

    assertThat(bucketMetadata("v").dimensions).isEqualTo(DIMENSIONS);
    assertThat(neighbors("v", vector(DIMENSIONS, 1))).containsExactly(rid[0]);
  }

  private void createType(final String name) {
    database.command("sql", "CREATE DOCUMENT TYPE " + name + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + name + ".embedding ARRAY_OF_FLOATS");
  }

  private LSMVectorIndexMetadata bucketMetadata(final String indexName) {
    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(indexName))
        .getIndexesOnBuckets()[0];
    return index.getMetadata();
  }

  private List<RID> neighbors(final String indexName, final float[] query) {
    final List<RID> found = new ArrayList<>();
    try (final ResultSet rows = database.query("sql", "SELECT expand(vector.neighbors('" + indexName + "', ?, 100))",
        (Object) query)) {
      rows.stream().map(Result::getIdentity).forEach(rid -> found.add(rid.orElseThrow()));
    }
    return found;
  }

  /** A non-zero vector distinct per {@code seed}, so COSINE can score it. */
  private static float[] vector(final int dimensions, final int seed) {
    final float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++)
      vector[i] = ((i * 31 + seed * 17) % 97 + 1) / 97f;
    vector[seed % dimensions] += seed;
    return vector;
  }
}
