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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10915: {@code CREATE VECTOR INDEX} without {@code vector.dimensions} before any vector is written, the order of a
 * script that creates the schema and then loads the data. The index is created unsized and fixes its dimensions on the first
 * vector it receives.
 */
class Issue10915VectorIndexSizedOnFirstVectorTest extends TestHelper {

  @Test
  void anIndexCreatedBeforeAnyVectorTakesItsDimensionsFromTheFirstOne() {
    command("CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");

    final LSMVectorIndex vector = vectorIndex("emb");
    assertThat(vector.getDimensions()).isZero();
    assertThat(vector.getMetadata().sizeOnFirstVector).isTrue();

    // an index still waiting for its first vector answers like an empty one, whatever the length of the query
    assertThat(nearestIds("emb", 3, List.of(1.0, 0.0, 0.0))).isEmpty();
    assertThat(nearestIds("emb", 3, List.of(1.0, 0.0))).isEmpty();

    command("CREATE (:Doc {id: 'a', embedding: [1.0, 0.0, 0.0]})");
    command("CREATE (:Doc {id: 'b', embedding: [0.0, 1.0, 0.0]})");

    assertThat(vector.getDimensions()).isEqualTo(3);
    assertThat(vector.getMetadata().sizeOnFirstVector).isFalse();
    assertThat(nearestIds("emb", 1, List.of(0.0, 1.0, 0.0))).containsExactly("b");
    assertThat(nearestIds("emb", 2, List.of(1.0, 0.1, 0.0))).containsExactly("a", "b");
  }

  @Test
  void showIndexesListsTheDimensionsOnceDecidedAndTheStatementItListsRecreatesTheIndex() {
    command("CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");

    // before the first vector there is no dimension to list: the statement asks for the sizing instead
    final String unsized = createStatement("emb");
    assertThat(unsized).doesNotContain("vector.dimensions").contains("sizeOnFirstVector");

    command("CREATE (:Doc {id: 'a', embedding: [1.0, 0.0, 0.0, 0.0]})");

    final String sized = createStatement("emb");
    assertThat(sized).contains("`vector.dimensions`: 4").doesNotContain("sizeOnFirstVector");

    // both statements run as they are
    command("DROP INDEX emb");
    command(unsized.replaceFirst("(INDEX\\s+`?)emb\\b", "$1emb2"));
    assertThat(vectorIndex("emb2").getDimensions()).isEqualTo(4);
  }

  @Test
  void aVectorOfAnotherLengthThanTheDecidedOneIsRefusedAsBySizedIndex() {
    command("CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");
    command("CREATE (:Doc {id: 'a', embedding: [1.0, 0.0, 0.0]})");

    assertThatThrownBy(() -> command("CREATE (:Doc {id: 'b', embedding: [1.0, 0.0]})")).hasStackTraceContaining(
        "Vector dimension does not match index dimension 3");
    assertThat(vectorIndex("emb").getDimensions()).isEqualTo(3);
  }

  @Test
  void theDimensionsSurviveAReopenOfTheDatabase() {
    command("CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding OPTIONS {indexConfig: {`vector.similarity_function`: 'euclidean'}}");
    command("CREATE (:Doc {id: 'a', embedding: [1.0, 0.0, 0.0]})");
    command("CREATE (:Doc {id: 'b', embedding: [0.0, 1.0, 0.0]})");

    reopenDatabase();

    final LSMVectorIndex vector = vectorIndex("emb");
    assertThat(vector.getDimensions()).isEqualTo(3);
    assertThat(vector.getMetadata().sizeOnFirstVector).isFalse();
    assertThat(nearestIds("emb", 1, List.of(0.0, 1.0, 0.0))).containsExactly("b");

    // and the index keeps taking vectors of that length only
    command("CREATE (:Doc {id: 'c', embedding: [0.0, 0.0, 1.0]})");
    assertThat(nearestIds("emb", 1, List.of(0.0, 0.0, 1.0))).containsExactly("c");
  }

  @Test
  void anIndexThatNeverReceivedAVectorIsStillUnsizedAfterAReopen() {
    command("CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");

    reopenDatabase();

    assertThat(vectorIndex("emb").getDimensions()).isZero();
    assertThat(vectorIndex("emb").getMetadata().sizeOnFirstVector).isTrue();

    command("CREATE (:Doc {id: 'a', embedding: [1.0, 0.0]})");
    assertThat(vectorIndex("emb").getDimensions()).isEqualTo(2);
    assertThat(nearestIds("emb", 1, List.of(1.0, 0.0))).containsExactly("a");
  }

  @Test
  void aRolledBackFirstVectorStillSizesTheIndex() {
    command("CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");

    database.begin();
    command("CREATE (:Doc {id: 'a', embedding: [1.0, 0.0, 0.0]})");
    database.rollback();

    // the size was decided by the vector, and other transactions may already have relied on it
    assertThat(vectorIndex("emb").getDimensions()).isEqualTo(3);
    assertThat(nearestIds("emb", 1, List.of(1.0, 0.0, 0.0))).isEmpty();
  }

  @Test
  void ifNotExistsOverASizedIndexIsStillANoOp() {
    command("CREATE VECTOR INDEX emb IF NOT EXISTS FOR (n:Doc) ON n.embedding");
    command("CREATE (:Doc {id: 'a', embedding: [1.0, 0.0, 0.0]})");
    command("MATCH (d:Doc) DETACH DELETE d");

    // the script runs again once the index is sized and no vector is left to take the size from
    command("CREATE VECTOR INDEX emb IF NOT EXISTS FOR (n:Doc) ON n.embedding");

    assertThat(vectorIndex("emb").getDimensions()).isEqualTo(3);
  }

  @Test
  void aNodeThatDidNotSeeThePutTakesTheDimensionsFromTheVectorsItHolds() {
    command("CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");
    command("CREATE (:Doc {id: 'a', embedding: [1.0, 0.0, 0.0]})");
    command("CREATE (:Doc {id: 'b', embedding: [0.0, 1.0, 0.0]})");

    // what a follower applying the replicated pages, or a database recovered before the schema was saved, holds: entries for
    // vectors and no dimension
    final LSMVectorIndex vector = vectorIndex("emb");
    vector.getMetadata().dimensions = 0;
    vector.getMetadata().sizeOnFirstVector = true;

    assertThat(nearestIds("emb", 1, List.of(0.0, 1.0, 0.0))).containsExactly("b");
    assertThat(vector.getDimensions()).isEqualTo(3);
    assertThat(vector.getMetadata().sizeOnFirstVector).isFalse();
  }

  @Test
  void theBucketsOfATypeDecideTheDimensionsOnce() throws Exception {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(4).create();
    type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
    final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
        .buildTypeIndex("Doc", new String[] { "embedding" }).withType(Schema.INDEX_TYPE.LSM_VECTOR);
    builder.withMetadata(new JSONObject().put("sizeOnFirstVector", true));
    builder.create();

    final int threads = 8;
    final CountDownLatch go = new CountDownLatch(1);
    final AtomicInteger saved = new AtomicInteger();
    final List<Integer> lengths = Collections.synchronizedList(new ArrayList<>());
    final List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int length = 3 + (t % 2);
      final Thread worker = new Thread(() -> {
        try {
          go.await();
          database.transaction(() -> {
            final float[] v = new float[length];
            v[0] = 1f;
            final MutableDocument doc = database.newDocument("Doc").set("embedding", v);
            doc.save();
          });
          lengths.add(length);
          saved.incrementAndGet();
        } catch (final Exception e) {
          // the vectors of the other length are refused, as by any sized index
        }
      });
      workers.add(worker);
      worker.start();
    }
    go.countDown();
    for (final Thread worker : workers)
      worker.join();

    assertThat(saved.get()).isPositive();
    assertThat(lengths.stream().distinct().count()).isEqualTo(1L);
    final int decided = lengths.getFirst();
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getType("Doc").getPolymorphicIndexByProperties("embedding"))
        .getIndexesOnBuckets())
      assertThat(((LSMVectorIndex) bucketIndex).getDimensions()).isEqualTo(decided);
  }

  @Test
  void aQuantizedIndexSizedOnItsFirstVectorBuildsItsGraphWithThatSize() {
    command("CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding OPTIONS {indexConfig: {`vector.quantization.type`: 'scalar'}}");
    for (int i = 0; i < 20; i++)
      command("CREATE (:Doc {id: 'd" + i + "', embedding: [" + i + ".0, " + (20 - i) + ".0, 1.0, 0.5]})");

    reopenDatabase();

    assertThat(vectorIndex("emb").getDimensions()).isEqualTo(4);
    assertThat(nearestIds("emb", 1, List.of(19.0, 1.0, 1.0, 0.5))).containsExactly("d19");
  }

  private LSMVectorIndex vectorIndex(final String name) {
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(name)).getIndexesOnBuckets())
      if (bucketIndex instanceof LSMVectorIndex vector)
        return vector;
    throw new AssertionError("no vector index behind " + name);
  }

  private List<String> nearestIds(final String index, final int k, final List<Double> vector) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes($index, $k, $vector) YIELD node, score RETURN node.id AS id ORDER BY score DESC", "index",
        index, "k", k, "vector", vector)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private String createStatement(final String name) {
    try (final ResultSet rs = database.query("opencypher", "SHOW INDEXES YIELD name, createStatement WHERE name = $name", "name",
        name)) {
      final Result row = rs.next();
      return row.getProperty("createStatement");
    }
  }

  private void command(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
