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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cypher vector indexes: vector type constraints, coordinate types and index sizing.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherVectorIndexTest {
  /**
   * Issue #10862: {@code CREATE CONSTRAINT ... REQUIRE n.e IS :: VECTOR<FLOAT32>(768)} was refused. It now declares the array
   * type the coordinate type maps to and enforces the dimension on every write through the property's MIN and MAX (an item
   * count), which the constraint owns and gives back on {@code DROP CONSTRAINT}. The {@code IS ::} predicate answers the same
   * vector type, so a constraint is refused when the stored data already violates it.
   */
  @Nested
  class Issue10862VectorTypeConstraint extends TestHelper {

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {//
        "VECTOR<FLOAT32>(3)|ARRAY_OF_FLOATS",//
        "VECTOR(3, FLOAT32)|ARRAY_OF_FLOATS",//
        "VECTOR<FLOAT64>(3)|ARRAY_OF_DOUBLES",//
        "VECTOR<FLOAT>(3)|ARRAY_OF_DOUBLES",//
        "VECTOR<INT64>(3)|ARRAY_OF_LONGS",//
        "VECTOR<INTEGER>(3)|ARRAY_OF_LONGS",//
        "VECTOR<INT32>(3)|ARRAY_OF_INTEGERS",//
        "VECTOR<INT16>(3)|ARRAY_OF_SHORTS",//
        "VECTOR<INT8>(3)|BINARY" })
    void aVectorTypeDeclaresTheArrayOfItsCoordinateTypeBoundedByItsDimension(final String cypherType, final String arcadeType) {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.e IS :: " + cypherType);

      final Property p = property("L", "e");
      assertThat(p.getType()).isEqualTo(Type.valueOf(arcadeType));
      assertThat(p.getMin()).isEqualTo("3");
      assertThat(p.getMax()).isEqualTo("3");
    }

    @Test
    void theDimensionIsEnforcedOnEveryWrite() {
      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");

      command("CREATE (:L {e: [0.1, 0.2, 0.3]})");
      command("CREATE (:L {e: vector([1, 2, 3], 3, FLOAT32)})");
      assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2]})")).isInstanceOf(RuntimeException.class);
      assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2, 0.3, 0.4]})")).isInstanceOf(RuntimeException.class);
      assertThatThrownBy(() -> command("MATCH (n:L) SET n.e = [1.0]")).isInstanceOf(RuntimeException.class);

      assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isEqualTo(2L);
      // what is stored satisfies the constraint's own type
      assertThat(count("MATCH (n:L) WHERE n.e IS :: VECTOR<FLOAT32>(3) RETURN count(n) AS c")).isEqualTo(2L);
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {//
        "VECTOR<FLOAT32>(3)", "VECTOR<FLOAT>(3)", "VECTOR<INTEGER>(3)", "VECTOR<INT32>(3)", "VECTOR<INT16>(3)" })
    void aWrittenListIsStoredAsAVectorOfTheConstraintsOwnType(final String cypherType) {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.e IS :: " + cypherType);

      command("CREATE (:L {e: [1, 2, 3]})");
      command("CREATE (:L {e: vector([1, 2, 3], 3, FLOAT32)})");
      assertThat(count("MATCH (n:L) WHERE n.e IS :: " + cypherType + " RETURN count(n) AS c")).isEqualTo(2L);
      assertThatThrownBy(() -> command("CREATE (:L {e: [1, 2]})")).isInstanceOf(RuntimeException.class);
    }

    @Test
    void anInt8VectorIsTheByteArrayOfABinary() {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.e IS :: VECTOR<INT8>(3)");

      command("CREATE (:L {e: [1, 2, 3]})");
      assertThat(count("MATCH (n:L) WHERE n.e IS :: VECTOR<INT8>(3) RETURN count(n) AS c")).isEqualTo(1L);
      assertThatThrownBy(() -> command("CREATE (:L {e: [1, 2]})")).isInstanceOf(RuntimeException.class);
      // vector() builds the byte[] of its INT8 coordinate type (#10917), which a BINARY takes
      command("CREATE (:L {e: vector([1, 2, 3], 3, INT8)})");
      assertThat(count("MATCH (n:L) WHERE n.e IS :: VECTOR<INT8>(3) RETURN count(n) AS c")).isEqualTo(2L);
    }

    @Test
    void thePredicateAnswersTheVectorType() {
      try (final ResultSet rs = database.query("opencypher", "WITH vector([1, 2, 3], 3, FLOAT32) AS v RETURN "
          + "v IS :: VECTOR<FLOAT32>(3) AS exact, v IS :: VECTOR<FLOAT32>(4) AS otherDimension, v IS :: VECTOR(3, FLOAT32) AS spelled, "
          + "v IS :: VECTOR<FLOAT>(3) AS wider, v IS :: VECTOR<INT32>(3) AS integers, v IS :: VECTOR AS bare, "
          + "[1.0, 2.0, 3.0] IS :: VECTOR<FLOAT32>(3) AS list, null IS :: VECTOR<FLOAT32>(3) AS nullable, "
          + "null IS :: VECTOR<FLOAT32>(3) NOT NULL AS notNull")) {
        final Result r = rs.next();
        assertThat(r.<Boolean>getProperty("exact")).isTrue();
        assertThat(r.<Boolean>getProperty("otherDimension")).isFalse();
        assertThat(r.<Boolean>getProperty("spelled")).isTrue();
        assertThat(r.<Boolean>getProperty("wider")).isTrue();
        assertThat(r.<Boolean>getProperty("integers")).isFalse();
        assertThat(r.<Boolean>getProperty("bare")).isTrue();
        // a list is not a vector
        assertThat(r.<Boolean>getProperty("list")).isFalse();
        assertThat(r.<Boolean>getProperty("nullable")).isTrue();
        assertThat(r.<Boolean>getProperty("notNull")).isFalse();
      }
    }

    @Test
    void aConstraintTheStoredDataViolatesIsRefusedAndLeavesNothingBehind() {
      database.transaction(() -> {
        database.getSchema().createVertexType("L");
        database.newVertex("L").set("e", new float[] { 1F, 2F }).save();
      });

      assertThatThrownBy(() -> command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)")).hasMessageContaining(
          "holds float[2]");
      assertThat(database.getSchema().getType("L").getPropertyIfExists("e")).isNull();
      assertThat(names("SHOW CONSTRAINTS YIELD name")).isEmpty();
    }

    @Test
    void aRefusalOverADeclaredPropertyTakesBackTheBoundsItSet() {
      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.e ARRAY_OF_FLOATS");
      database.transaction(() -> database.newVertex("L").set("e", new float[] { 1F, 2F }).save());

      // the declaration was there before: the refusal keeps it, without the MIN and MAX the statement set
      assertThatThrownBy(() -> command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)")).hasMessageContaining(
          "holds float[2]");
      final Property p = property("L", "e");
      assertThat(p.getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
      assertThat(p.getMin()).isNull();
      assertThat(p.getMax()).isNull();
      assertThat(names("SHOW CONSTRAINTS YIELD name")).doesNotContain("t");
    }

    @Test
    void aStoredListIsNotAVectorSoTheConstraintIsRefused() {
      // a list written before any declaration stays a list: it answers false to IS :: VECTOR, as in Neo4j
      command("CREATE (:L {e: [1.0, 2.0, 3.0]})");

      assertThatThrownBy(() -> command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)")).hasMessageContaining(
          "(LIST<ANY>)");
      assertThat(database.getSchema().getType("L").getPropertyIfExists("e")).isNull();
    }

    @Test
    void aDropWhoseRestoreIsRefusedLeavesTheDimensionInPlace() {
      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.e LONG");
      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
      command("CREATE (:L {e: [0.1, 0.2, 0.3]})");

      // the LONG declaration the constraint replaced cannot hold the stored vector: the drop is refused and changes nothing
      assertThatThrownBy(() -> command("DROP CONSTRAINT t")).isInstanceOf(RuntimeException.class);
      final Property p = property("L", "e");
      assertThat(p.getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
      assertThat(p.getMin()).isEqualTo("3");
      assertThat(p.getMax()).isEqualTo("3");
      assertThat(names("SHOW CONSTRAINTS YIELD name")).containsExactly("t");
      assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2]})")).isInstanceOf(RuntimeException.class);
    }

    @Test
    void dropConstraintGivesTheDeclarationBack() {
      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
      command("DROP CONSTRAINT t");

      assertThat(database.getSchema().getType("L").getPropertyIfExists("e")).isNull();
      command("CREATE (:L {e: [1.0]})");
    }

    @Test
    void aConstraintOverAVectorIndexPropertyOwnsOnlyTheDimension() {
      // CREATE VECTOR INDEX declares the property ARRAY_OF_FLOATS itself (#10678): the constraint adds the dimension only
      command("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3}}");
      assertThat(property("L", "e").getType()).isEqualTo(Type.ARRAY_OF_FLOATS);

      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
      assertThat(property("L", "e").getMin()).isEqualTo("3");
      assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2]})")).isInstanceOf(RuntimeException.class);

      command("DROP CONSTRAINT t");
      final Property p = property("L", "e");
      assertThat(p.getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
      assertThat(p.getMin()).isNull();
      assertThat(p.getMax()).isNull();
    }

    @Test
    void anotherDimensionOfTheSamePropertyConflicts() {
      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");

      assertThatThrownBy(() -> command("CREATE CONSTRAINT t2 FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(4)")).rootCause().hasMessageContaining(
          "already requires another type");
      // the same requirement again is a no-op
      command("CREATE CONSTRAINT IF NOT EXISTS FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
      assertThat(names("SHOW CONSTRAINTS YIELD name")).containsExactly("t");
    }

    @Test
    void dimensionBoundsPassToAnEquivalentConstraint() {
      command("CREATE CONSTRAINT a FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
      command("CREATE CONSTRAINT b FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");

      command("DROP CONSTRAINT a");
      assertThat(property("L", "e").getMax()).isEqualTo("3");
      assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2]})")).isInstanceOf(RuntimeException.class);

      command("DROP CONSTRAINT b");
      assertThat(database.getSchema().getType("L").getPropertyIfExists("e")).isNull();
    }

    @Test
    void theReplacedDeclarationWaitsForTheLastEquivalentConstraint() {
      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.e LONG");
      command("CREATE CONSTRAINT a FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
      command("CREATE CONSTRAINT b FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");

      // b still requires the vector: dropping a neither restores LONG nor lifts the dimension
      command("DROP CONSTRAINT a");
      Property p = property("L", "e");
      assertThat(p.getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
      assertThat(p.getMax()).isEqualTo("3");
      assertThatThrownBy(() -> command("CREATE (:L {e: [0.1, 0.2]})")).isInstanceOf(RuntimeException.class);

      // the last one puts the declaration a replaced back
      command("DROP CONSTRAINT b");
      p = property("L", "e");
      assertThat(p.getType()).isEqualTo(Type.LONG);
      assertThat(p.getMin()).isNull();
      assertThat(p.getMax()).isNull();
    }

    @Test
    void aScalarTypeConstraintAlsoHandsTheReplacedDeclarationOn() {
      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.p LONG");
      command("CREATE CONSTRAINT a FOR (n:L) REQUIRE n.p IS :: STRING");
      command("CREATE CONSTRAINT b FOR (n:L) REQUIRE n.p IS :: STRING");

      command("DROP CONSTRAINT a");
      assertThat(property("L", "p").getType()).isEqualTo(Type.STRING);
      command("DROP CONSTRAINT b");
      assertThat(property("L", "p").getType()).isEqualTo(Type.LONG);
    }

    @Test
    void boundsAlreadyOnThePropertyThatSaySomethingElseAreRefused() {
      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.e ARRAY_OF_FLOATS (MAX 10)");

      assertThatThrownBy(() -> command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)")).hasMessageContaining(
          "MAX 10");
      assertThat(property("L", "e").getMax()).isEqualTo("10");
    }

    @Test
    void aRetypedPropertyGetsItsDeclarationBackOnDrop() {
      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.e LIST");

      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR<FLOAT32>(3)");
      assertThat(property("L", "e").getType()).isEqualTo(Type.ARRAY_OF_FLOATS);

      command("DROP CONSTRAINT t");
      final Property p = property("L", "e");
      assertThat(p.getType()).isEqualTo(Type.LIST);
      assertThat(p.getMin()).isNull();
      assertThat(p.getMax()).isNull();
    }

    @Test
    void showConstraintsListsTheVectorType() {
      command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.e IS :: VECTOR(3, FLOAT64)");

      try (final ResultSet rs = database.query("opencypher", "SHOW CONSTRAINTS YIELD name, propertyType")) {
        final Result r = rs.next();
        assertThat(r.<String>getProperty("propertyType")).isEqualTo("VECTOR<FLOAT>(3)");
      }
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {//
        "VECTOR<FLOAT32>|coordinate type and its dimension",//
        "VECTOR(3)|coordinate type and its dimension",//
        "VECTOR|coordinate type and its dimension",//
        "VECTOR<FLOAT32>(3) NOT NULL|cannot require NOT NULL",//
        "VECTOR<FLOAT32>(0)|positive integer",//
        "LIST<VECTOR<FLOAT32>(3)>|list of vectors" })
    void anIncompleteOrUnsupportedVectorTypeIsRefused(final String cypherType, final String message) {
      assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.e IS :: " + cypherType)).isInstanceOf(
          CommandParsingException.class).hasMessageContaining(message);
    }

    private void command(final String cypher) {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
      }
    }

    private long count(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private Property property(final String typeName, final String propertyName) {
      final Property p = database.getSchema().getType(typeName).getPropertyIfExists(propertyName);
      assertThat(p).isNotNull();
      return p;
    }

    private List<Object> names(final String cypher) {
      final List<Object> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
      }
      return names;
    }
  }

  /**
   * Issue #10915: {@code CREATE VECTOR INDEX} without {@code vector.dimensions} before any vector is written, the order of a
   * script that creates the schema and then loads the data. The index is created unsized and fixes its dimensions on the first
   * vector it receives.
   */
  @Nested
  class Issue10915VectorIndexSizedOnFirstVector extends TestHelper {

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

    @Test
    void aProductQuantizedIndexSizedOnItsFirstVectorBuildsItsCodebooksWithThatSize() {
      command("CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding OPTIONS {indexConfig: {quantization: 'PRODUCT', pqClusters: 16}}");
      for (int i = 0; i < 100; i++)
        command("CREATE (:Doc {id: 'd" + i + "', embedding: [" + i + ".0, " + (100 - i) + ".0, " + (i % 7) + ".0, 1.0, 0.5, "
            + (i % 3) + ".0, 2.0, 0.25]})");

      reopenDatabase();

      final LSMVectorIndex index = vectorIndex("emb");
      assertThat(index.getDimensions()).isEqualTo(8);
      // The codebooks themselves are what the index sized on its first vector must have built: trained and encoding every
      // vector. Which id a PRODUCT search answers first is not part of that: the codebooks come out of JVector's k-means++,
      // seeded from ThreadLocalRandom, so the approximate top-1 of near-ties changes from one training to the next (#11035)
      // Searched first: a close that ran while a background rebuild had published the graph but not yet written its codebook
      // leaves none on disk, and the first search of the reopened database is what builds it then
      assertThat(nearestIds("emb", 1, List.of(99.0, 1.0, 1.0, 1.0, 0.5, 0.0, 2.0, 0.25))).hasSize(1);
      assertThat(index.isPQSearchAvailable()).isTrue();
      assertThat(index.getPQVectorCount()).isEqualTo(100);
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

  /**
   * Issue #10917: {@code vector(value, dimension, coordinateType)} ignored its coordinate type and always built a
   * {@code float[]}. It now builds the primitive array of the coordinate type, the one a {@code VECTOR<T>(n)} property declares.
   */
  @Nested
  class Issue10917VectorCoordinateType extends TestHelper {

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {//
        "INT8|VECTOR<INT8>(3)|byte[]", "INT16|VECTOR<INT16>(3)|short[]", "INT32|VECTOR<INT32>(3)|int[]",
        "INT|VECTOR<INT32>(3)|int[]", "INT64|VECTOR<INTEGER>(3)|long[]", "INTEGER|VECTOR<INTEGER>(3)|long[]",
        "FLOAT32|VECTOR<FLOAT32>(3)|float[]", "FLOAT64|VECTOR<FLOAT>(3)|double[]", "FLOAT|VECTOR<FLOAT>(3)|double[]" })
    void theVectorIsTheArrayOfItsCoordinateType(final String coordinate, final String vectorType, final String array) {
      try (final ResultSet rs = database.query("opencypher",
          "WITH vector([1, 2, 3], 3, " + coordinate + ") AS v RETURN v IS :: " + vectorType + " AS typed, v AS v")) {
        final Result r = rs.next();
        assertThat(r.<Boolean>getProperty("typed")).isTrue();
        final Object v = r.getProperty("v");
        assertThat(v.getClass().getSimpleName()).isEqualTo(array);
      }
    }

    @Test
    void aNarrowerCoordinateTypeIsNotAWiderOne() {
      try (final ResultSet rs = database.query("opencypher",
          "WITH vector([1, 2, 3], 3, INT32) AS i, vector([1, 2, 3], 3, INT8) AS b RETURN "
              + "i IS :: VECTOR<INT8>(3) AS intAsByte, b IS :: VECTOR<INT32>(3) AS byteAsInt, b IS :: VECTOR<FLOAT32>(3) AS byteAsFloat")) {
        final Result r = rs.next();
        assertThat(r.<Boolean>getProperty("intAsByte")).isFalse();
        assertThat(r.<Boolean>getProperty("byteAsInt")).isTrue();
        assertThat(r.<Boolean>getProperty("byteAsFloat")).isFalse();
      }
    }

    @Test
    void aCoordinateOutsideTheTypeIsRefused() {
      assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([1, 300, 3], 3, INT8) AS v").next())
          .hasMessageContaining("INT8");
      assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([1, 1.5, 3], 3, INT32) AS v").next())
          .hasMessageContaining("INT32");
    }

    @Test
    void theBoundariesOfTheTypeAreKeptAndWhatIsPastThemIsRefused() {
      try (final ResultSet rs = database.query("opencypher",
          "RETURN vector([-2147483648, 2147483647], 2, INT32) AS i, vector([-9223372036854775807, 9223372036854775807], 2, INTEGER) AS l")) {
        final Result r = rs.next();
        assertThat((int[]) r.getProperty("i")).containsExactly(Integer.MIN_VALUE, Integer.MAX_VALUE);
        assertThat((long[]) r.getProperty("l")).containsExactly(-Long.MAX_VALUE, Long.MAX_VALUE);
      }
      assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([2147483648], 1, INT32) AS v").next())
          .hasMessageContaining("INT32");
      assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([9.223372036854775807E18], 1, INTEGER) AS v").next())
          .hasMessageContaining("INTEGER");
      assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([0.0 / 0.0], 1, INT16) AS v").next())
          .hasMessageContaining("INT16");
      assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([1.0 / 0.0], 1, INT16) AS v").next())
          .hasMessageContaining("INT16");
    }

    @Test
    void theDimensionIsStillChecked() {
      assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([1, 2], 3, INT16) AS v").next())
          .hasMessageContaining("dimension");
    }

    @Test
    void aTypedVectorFeedsTheDistanceFunctions() {
      try (final ResultSet rs = database.query("opencypher",
          "WITH vector([3, 4], 2, INT8) AS a, vector([0, 0], 2, INT16) AS b RETURN "
              + "vector_distance(a, b, EUCLIDEAN) AS d, vector_norm(a, EUCLIDEAN) AS n")) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("d").doubleValue()).isEqualTo(5.0);
        assertThat(r.<Number>getProperty("n").doubleValue()).isEqualTo(5.0);
      }
    }

    /**
     * A {@code byte[]} reaching the Cypher vector functions is an INT8 vector of plain coordinates, never a quantized one: it is widened
     * as it is, not dequantized. Pinned because the SQL vector functions read the same {@code byte[]} as quantized (and refuse it).
     */
    @Test
    void aByteArrayIsReadAsPlainCoordinatesNotDequantized() {
      try (final ResultSet rs = database.query("opencypher",
          "RETURN vector_norm($v, EUCLIDEAN) AS n, vector_distance($v, $w, EUCLIDEAN) AS d",
          Map.of("v", new byte[] { 3, 4 }, "w", new byte[] { 0, 0 }))) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("n").doubleValue()).isEqualTo(5.0);
        assertThat(r.<Number>getProperty("d").doubleValue()).isEqualTo(5.0);
      }
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "vector.magnitude($v)|5.0",
        "vector.l2Distance($v, $w)|5.0",
        "vector.l1Norm($v)|7.0",
        "vector.dotProduct($v, $v)|25.0",
        "vector.distance.euclidean($v, $w)|5.0",
        "vector.distance.manhattan($v, $w)|7.0",
        "vector.dimension.count($v)|2",
        "sql.vector.magnitude($v)|5.0" })
    void everyVectorFunctionReadsAByteArrayAsPlainCoordinates(final String call, final double expected) {
      try (final ResultSet rs = database.query("opencypher", "RETURN " + call + " AS r",
          Map.of("v", new byte[] { 3, 4 }, "w", new byte[] { 0, 0 }))) {
        assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(expected);
      }
    }

    @Test
    void aQuantizedByteArrayIsStillDequantizedByDequantizeInt8() {
      try (final ResultSet rs = database.query("opencypher", "RETURN vector.dequantizeInt8($q, -1.0, 1.0) AS r",
          Map.of("q", new byte[] { 127, -128 }))) {
        final float[] r = rs.next().getProperty("r");
        assertThat(r).hasSize(2);
        assertThat(r[0]).isCloseTo(1.0f, Offset.offset(0.01f));
      }
    }

    @Test
    void aTypedVectorIsStoredAsTheConstrainedProperty() {
      command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.e IS :: VECTOR<INT32>(3)");
      command("CREATE (:L {e: vector([1, 2, 3], 3, INT32)})");
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:L) RETURN n.e IS :: VECTOR<INT32>(3) AS ok")) {
        assertThat(rs.next().<Boolean>getProperty("ok")).isTrue();
      }
    }

    private void command(final String cypher) {
      database.transaction(() -> database.command("opencypher", cypher).close());
    }
  }
}
