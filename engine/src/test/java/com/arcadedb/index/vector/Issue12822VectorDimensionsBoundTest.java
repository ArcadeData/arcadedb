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
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.database.TransactionIndexContext.ComparableKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12822: an LSM_VECTOR index took any {@code dimensions}, and a huge value made the index allocate gigabytes of
 * dimensions-sized float arrays. The value is now bounded by {@code arcadedb.vectorIndex.maxDimensions} when the index is
 * created, through every creation path, while an index persisted with a larger value keeps opening. A removal of a record
 * with no usable vector no longer allocates a dimensions-sized placeholder key either.
 */
class Issue12822VectorDimensionsBoundTest extends TestHelper {
  private static final int SMALL_DIMENSIONS = 8;

  @Test
  void sqlRefusesDimensionsAboveTheBound() {
    createType("Doc");
    final int bound = bound();

    for (final long dimensions : new long[] { bound + 1L, 99_999_999L })
      assertThatThrownBy(() -> database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + dimensions + "}"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("dimensions")
          .hasMessageContaining(GlobalConfiguration.VECTOR_INDEX_MAX_DIMENSIONS.getKey());

    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();
  }

  @Test
  void sqlAcceptsDimensionsAtTheBound() {
    createType("Doc");
    final int bound = bound();

    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + bound + "}");

    assertThat(bucketMetadata("Doc[embedding]").dimensions).isEqualTo(bound);
  }

  @Test
  void cypherRefusesDimensionsAboveTheBoundBeforeCreatingTheLabel() {
    final int bound = bound();

    assertThatThrownBy(() -> database.command("opencypher",
        "CREATE VECTOR INDEX v FOR (n:Fresh) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: " + (bound + 1) + "}}"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining(GlobalConfiguration.VECTOR_INDEX_MAX_DIMENSIONS.getKey());

    // the native spelling of the setting goes through the same check
    assertThatThrownBy(() -> database.command("opencypher",
        "CREATE VECTOR INDEX w FOR (n:Fresh) ON n.e OPTIONS {indexConfig: {dimensions: " + (bound + 1) + "}}"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining(GlobalConfiguration.VECTOR_INDEX_MAX_DIMENSIONS.getKey());

    assertThat(database.getSchema().existsType("Fresh")).isFalse();
    assertThat(database.getSchema().existsIndex("v")).isFalse();
  }

  @Test
  void cypherAcceptsDimensionsAtTheBound() {
    final int bound = bound();

    database.command("opencypher",
        "CREATE VECTOR INDEX v FOR (n:Fresh) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: " + bound + "}}");

    assertThat(bucketMetadata("v").dimensions).isEqualTo(bound);
  }

  @Test
  void javaBuilderRefusesDimensionsAboveTheBound() {
    createType("Doc");
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withLSMVectorType();
    builder.withDimensions(bound() + 1).withSimilarity("COSINE");

    assertThatThrownBy(builder::create)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(GlobalConfiguration.VECTOR_INDEX_MAX_DIMENSIONS.getKey());
    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();
  }

  @Test
  void theBoundIsReadFromTheDatabaseConfiguration() {
    createType("Doc");
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DIMENSIONS, SMALL_DIMENSIONS);

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + (SMALL_DIMENSIONS + 1) + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(GlobalConfiguration.VECTOR_INDEX_MAX_DIMENSIONS.getKey());

    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + SMALL_DIMENSIONS + "}");
    assertThat(bucketMetadata("Doc[embedding]").dimensions).isEqualTo(SMALL_DIMENSIONS);
  }

  @Test
  void anIndexPersistedAboveTheBoundStillOpens() {
    final int bound = bound();
    final int dimensions = bound + 1;
    createType("Doc");
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DIMENSIONS, dimensions);
    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + dimensions + "}");
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DIMENSIONS, bound);

    reopenDatabase();

    assertThat(bound()).as("the reopened database runs with the bound below the persisted dimensions").isLessThan(dimensions);
    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isTrue();
    assertThat(bucketMetadata("Doc[embedding]").dimensions).isEqualTo(dimensions);
    // an IF NOT EXISTS answered by the persisted index is not refused: nothing is created
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + dimensions + "}");

    final float[] vector = vector(dimensions, 1);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("embedding", vector).save().getIdentity());
    assertThat(neighbors("Doc[embedding]", vector)).containsExactly(rid[0]);
  }

  @Test
  void aRemovalWithoutAVectorQueuesOneConstantEmptyKeyPerRecord() {
    final int dimensions = 1536;
    createType("Doc");
    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + dimensions + "}");

    final List<RID> withoutVector = new ArrayList<>();
    final List<RID> withVector = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        withoutVector.add(database.newDocument("Doc").set("name", "none" + i).save().getIdentity());
      for (int i = 0; i < 2; i++)
        withVector.add(database.newDocument("Doc").set("embedding", vector(dimensions, i + 1)).save().getIdentity());
    });

    final String bucketIndexName = bucketIndex("Doc[embedding]").getName();
    database.begin();
    for (final RID rid : withoutVector)
      rid.asDocument().delete();

    final TransactionIndexContext changes = ((DatabaseInternal) database).getTransaction().getIndexChanges();
    final TreeMap<ComparableKey, Map<IndexKey, IndexKey>> lane = changes.getIndexKeys(bucketIndexName);
    final List<IndexKey> removals = new ArrayList<>();
    for (final Map<IndexKey, IndexKey> entries : lane.values())
      for (final IndexKey entry : entries.values())
        if (entry.operation == IndexKey.IndexKeyOperation.REMOVE)
          removals.add(entry);

    // removals of different records under the same placeholder must not collapse into one
    assertThat(removals).extracting(e -> e.rid).containsExactlyInAnyOrderElementsOf(withoutVector);
    for (final IndexKey removal : removals) {
      assertThat(removal.keyValues).hasSize(1);
      assertThat(removal.keyValues[0]).isInstanceOf(ComparableVector.class);
      assertThat(((ComparableVector) removal.keyValues[0]).vector).as("no dimensions-sized placeholder per removal").isEmpty();
    }
    database.commit();

    final Set<RID> found = new HashSet<>(neighbors("Doc[embedding]", vector(dimensions, 1)));
    assertThat(found).containsExactlyInAnyOrderElementsOf(withVector);
  }

  @Test
  void removalsAndPutsOfTheSameRecordInOneTransactionStayConsistent() {
    createType("Doc");
    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + SMALL_DIMENSIONS + "}");

    final RID[] rids = new RID[4];
    database.transaction(() -> {
      // 0: no vector, gains one in the next transaction
      rids[0] = database.newDocument("Doc").set("name", "gains").save().getIdentity();
      // 1: no vector, gains one and is then deleted in the same transaction
      rids[1] = database.newDocument("Doc").set("name", "gainsThenDeleted").save().getIdentity();
      // 2: has a vector, loses it
      rids[2] = database.newDocument("Doc").set("embedding", vector(SMALL_DIMENSIONS, 2)).save().getIdentity();
      // 3: has a vector and is deleted
      rids[3] = database.newDocument("Doc").set("embedding", vector(SMALL_DIMENSIONS, 3)).save().getIdentity();
    });

    database.transaction(() -> {
      final MutableDocument gains = rids[0].asDocument().modify();
      gains.set("embedding", vector(SMALL_DIMENSIONS, 4)).save();

      final MutableDocument gainsThenDeleted = rids[1].asDocument().modify();
      gainsThenDeleted.set("embedding", vector(SMALL_DIMENSIONS, 5)).save();
      gainsThenDeleted.delete();

      final MutableDocument loses = rids[2].asDocument().modify();
      loses.remove("embedding");
      loses.save();

      rids[3].asDocument().delete();
    });

    assertThat(neighbors("Doc[embedding]", vector(SMALL_DIMENSIONS, 4))).containsExactly(rids[0]);
  }

  private void createType(final String name) {
    database.command("sql", "CREATE DOCUMENT TYPE " + name + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + name + ".embedding ARRAY_OF_FLOATS");
  }

  private int bound() {
    return database.getConfiguration().getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_MAX_DIMENSIONS);
  }

  private LSMVectorIndex bucketIndex(final String indexName) {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets()[0];
  }

  private LSMVectorIndexMetadata bucketMetadata(final String indexName) {
    return bucketIndex(indexName).getMetadata();
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
