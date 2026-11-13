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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * The db.index.vector.queryNodes procedure: pre-filtering, score range and stale candidates.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherVectorQueryNodesTest {
  /**
   * Issue #8963: {@code db.index.vector.queryNodes} scored COSINE as the raw cosine (down to -1) and DOT_PRODUCT as
   * {@code (3 + dot) / 2} (1 to 2 for unit vectors), while Neo4j's procedure it mirrors returns {@code (1 + cosine) / 2}
   * in [0, 1] for both, and EUCLIDEAN was already {@code 1 / (1 + d^2)}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8963VectorQueryNodesScoreRange extends TestHelper {

    @Test
    void cosineScoreIsHalfOfOnePlusCosine() {
      final Map<String, Double> scores = scores("COSINE");
      assertThat(scores.get("same")).isCloseTo(1.0, within(1e-5));
      assertThat(scores.get("orthogonal")).isCloseTo(0.5, within(1e-5));
      assertThat(scores.get("opposite")).isCloseTo(0.0, within(1e-5));
    }

    @Test
    void dotProductScoreIsHalfOfOnePlusDotForUnitVectors() {
      final Map<String, Double> scores = scores("DOT_PRODUCT");
      assertThat(scores.get("same")).isCloseTo(1.0, within(1e-5));
      assertThat(scores.get("orthogonal")).isCloseTo(0.5, within(1e-5));
      assertThat(scores.get("opposite")).isCloseTo(0.0, within(1e-5));
    }

    @Test
    void euclideanScoreIsUnchanged() {
      final Map<String, Double> scores = scores("EUCLIDEAN");
      assertThat(scores.get("same")).isCloseTo(1.0, within(1e-5));
      assertThat(scores.get("orthogonal")).isCloseTo(1.0 / 3.0, within(1e-5));
      assertThat(scores.get("opposite")).isCloseTo(0.2, within(1e-5));
    }

    private Map<String, Double> scores(final String similarity) {
      final String type = "N" + similarity;
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + type + " (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"" + similarity + "\" }");
      database.transaction(() -> {
        database.newVertex(type).set("name", "same", "vector", new float[] { 1f, 0f }).save();
        database.newVertex(type).set("name", "orthogonal", "vector", new float[] { 0f, 1f }).save();
        database.newVertex(type).set("name", "opposite", "vector", new float[] { -1f, 0f }).save();
      });

      final Map<String, Double> result = new LinkedHashMap<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL db.index.vector.queryNodes('" + type + "[vector]', 3, $v) YIELD node, score RETURN node.name AS name, score",
          Map.of("v", new float[] { 1f, 0f }))) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          result.put(r.getProperty("name"), ((Number) r.getProperty("score")).doubleValue());
        }
      }
      assertThat(result).hasSize(3);
      return result;
    }
  }

  /**
   * Issue #9858: {@code db.index.vector.queryNodes} loaded every candidate the vector index answered with and let a
   * {@code RecordNotFoundException} escape, so a record deleted by a commit that overlapped the search failed the whole
   * query ({@code Neo.ClientError.Statement.EntityNotFound} over Bolt). A stale candidate must be skipped, and the answer
   * topped up to {@code k} live rows, exactly as {@code vectorNeighbors} does (#9368).
   * <p>
   * The overlapping delete is reproduced deterministically by removing records from their bucket only, which leaves their
   * index entries behind - the state a concurrent commit leaves between the index search and the record load.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9858VectorQueryNodesStaleCandidate extends TestHelper {
    private static final int     COUNT  = 60;
    private static final float[] ORIGIN = { 0f, 0f };

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // the tests leave index entries pointing at records they removed from the bucket, on purpose
      return false;
    }

    @Test
    void theNearestCandidatesBeingStaleCostsNoRow() {
      createType(1);
      removeFromBucketOnly("pid < 4");

      assertThat(queryNodes(10)).containsExactly(4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
    }

    @Test
    void aWholeWindowOfStaleCandidatesIsToppedUp() {
      createType(1);
      removeFromBucketOnly("pid < 10");

      assertThat(queryNodes(10)).containsExactly(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
    }

    @Test
    void anIndexWithFewerLiveRecordsThanKAnswersWhatItHas() {
      createType(1);
      removeFromBucketOnly("pid >= 5");

      assertThat(queryNodes(10)).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void staleCandidatesSpreadOverSeveralBucketIndexesAreToppedUp() {
      createType(4);
      removeFromBucketOnly("pid < 6");

      assertThat(queryNodes(10)).containsExactly(6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    }

    @Test
    void theScoreOfEveryRowIsTheOneOfItsOwnRecord() {
      createType(1);
      removeFromBucketOnly("pid < 3");

      try (final ResultSet rs = database.query("opencypher",
          "CALL db.index.vector.queryNodes('Doc[vector]', 3, $v) YIELD node, score RETURN node.pid AS pid, score", Map.of("v", ORIGIN))) {
        final List<Integer> pids = new ArrayList<>();
        while (rs.hasNext()) {
          final var row = rs.next();
          final int pid = row.getProperty("pid");
          pids.add(pid);
          // EUCLIDEAN: the distance is the squared L2, mapped to 1 / (1 + d)
          assertThat(((Number) row.getProperty("score")).floatValue()).isEqualTo(1.0f / (1.0f + pid * pid));
        }
        assertThat(pids).containsExactly(3, 4, 5);
      }
    }

    private void createType(final int buckets) {
      database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY Doc.pid INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
      database.transaction(() -> {
        for (int i = 0; i < COUNT; i++)
          database.newVertex("Doc").set("pid", i, "vector", new float[] { i, 0f }).save();
      });
    }

    /** Deletes the matching records from their bucket only, leaving their vector index entries behind. */
    private void removeFromBucketOnly(final String where) {
      final List<RID> stale = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Doc WHERE " + where)) {
        rs.forEachRemaining(r -> stale.add(r.getProperty("rid")));
      }
      assertThat(stale).isNotEmpty();

      // Build every graph first: a lazy build validates each record and would drop the ones removed below.
      final IndexInternal[] bucketIndexes = ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets();
      for (final IndexInternal bucketIndex : bucketIndexes)
        ((LSMVectorIndex) bucketIndex).findNeighborsFromVector(ORIGIN, 10);

      database.transaction(() -> {
        for (final RID rid : stale)
          ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).deleteRecord(rid);
      });

      // Precondition: the indexes still answer with the removed records, otherwise these tests prove nothing.
      final List<RID> fromIndexes = new ArrayList<>();
      for (final IndexInternal bucketIndex : bucketIndexes)
        ((LSMVectorIndex) bucketIndex).findNeighborsFromVector(ORIGIN, COUNT).forEach(p -> fromIndexes.add(p.getFirst()));
      assertThat(fromIndexes).containsAll(stale);
    }

    private List<Integer> queryNodes(final int k) {
      final List<Integer> pids = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL db.index.vector.queryNodes('Doc[vector]', " + k + ", $v) YIELD node, score RETURN node.pid AS pid", Map.of("v", ORIGIN))) {
        rs.forEachRemaining(r -> pids.add(r.getProperty("pid")));
      }
      return pids;
    }
  }

  /**
   * Issue #10685: {@code db.index.vector.queryNodes(index, k, vector)} had no filter, so a tenant filter could only be applied
   * to its k results afterwards and a query whose nearest neighbours belong to another tenant answered fewer rows than asked,
   * or none. The optional fourth argument {@code {where: ..., filter: [...]}} restricts the candidates DURING the search, the
   * same pre-filter {@code vector.neighbors} takes.
   * <p>
   * The data reproduces the issue: 900 vectors of tenant b, all nearer the query than any of the 100 of tenant a.
   */
  @Nested
  class Issue10685VectorQueryNodesPreFilter extends TestHelper {
    private static final int                 K      = 10;
    private static final float[]             QUERY  = { 1f, 0f };
    private static final Map<String, Object> PARAMS = Map.of("q", QUERY, "k", K, "tenant", "a");

    @Override
    public void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Fact");
      database.command("sql", "CREATE PROPERTY Fact.tenant STRING");
      database.command("sql", "CREATE PROPERTY Fact.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Fact (embedding) LSM_VECTOR METADATA { dimensions: 2, similarity: 'EUCLIDEAN' }");
      database.transaction(() -> {
        // tenant b: on the x axis near the query, every one nearer than any of tenant a
        for (int i = 0; i < 900; i++)
          database.newVertex("Fact").set("tenant", "b", "embedding", new float[] { 1f, i * 0.0001f }).save();
        // tenant a: far up the y axis
        for (int i = 0; i < 100; i++)
          database.newVertex("Fact").set("tenant", "a", "embedding", new float[] { 1f, 5f + i * 0.01f }).save();
      });
    }

    @Test
    void postFilterAfterTheTopKStillAnswersNothing() {
      // the documented three-argument form is unchanged: the filter applies after the top k
      assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q) YIELD node, score "
          + "WITH node WHERE node.tenant = $tenant RETURN node.tenant AS tenant")).isEmpty();
    }

    @Test
    void whereMapPreFiltersTheSearch() {
      final List<String> tenants = tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {where: {tenant: $tenant}}) "
          + "YIELD node, score RETURN node.tenant AS tenant");
      assertThat(tenants).hasSize(K);
      assertThat(tenants).containsOnly("a");
    }

    @Test
    void whereConditionWithNamedParameterPreFiltersTheSearch() {
      final List<String> tenants = tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {where: 'tenant = :tenant'}) "
          + "YIELD node, score RETURN node.tenant AS tenant");
      assertThat(tenants).hasSize(K);
      assertThat(tenants).containsOnly("a");
    }

    @Test
    void filterOfCollectedNodesPreFiltersTheSearch() {
      final List<String> tenants = tenants("MATCH (f:Fact) WHERE f.tenant = $tenant WITH collect(f) AS allowed "
          + "CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {filter: allowed}) YIELD node, score "
          + "RETURN node.tenant AS tenant");
      assertThat(tenants).hasSize(K);
      assertThat(tenants).containsOnly("a");
    }

    @Test
    void filterAndWhereAreBothApplied() {
      // the filter names one record of tenant a and one of tenant b; the where keeps only the tenant a one
      final String ridA;
      final String ridB;
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Fact WHERE tenant = 'a' LIMIT 1")) {
        ridA = rs.next().<Object>getProperty("rid").toString();
      }
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Fact WHERE tenant = 'b' LIMIT 1")) {
        ridB = rs.next().<Object>getProperty("rid").toString();
      }
      final List<String> tenants = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {filter: $allowed, where: {tenant: $tenant}}) YIELD node, score "
              + "RETURN node.tenant AS tenant, node",
          Map.of("q", QUERY, "k", K, "tenant", "a", "allowed", List.of(ridA, ridB)))) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          tenants.add(row.getProperty("tenant"));
          assertThat(row.<Document>getProperty("node").getIdentity().toString()).isEqualTo(ridA);
        }
      }
      assertThat(tenants).containsExactly("a");
    }

    @Test
    void preFilterMatchingFewerThanKAnswersEveryMatch() {
      // k above the 100 records of tenant a: the search and its stale top-up answer every one of them and nothing else
      final List<String> tenants = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {where: {tenant: $tenant}}) YIELD node, score "
              + "RETURN node.tenant AS tenant", Map.of("q", QUERY, "k", 200, "tenant", "a"))) {
        while (rs.hasNext())
          tenants.add(rs.next().getProperty("tenant"));
      }
      assertThat(tenants).hasSize(100);
      assertThat(tenants).containsOnly("a");
    }

    @Test
    void preFilterAppliesToEverySubIndexOfAParentTypeSpec() {
      // a sub-type brings its own buckets, so 'Fact[embedding]' resolves several indexes: tenant b nearest in the sub-type
      // too, one tenant a record in it that the filter names and one the filter leaves out
      database.command("sql", "CREATE VERTEX TYPE SubFact EXTENDS Fact");
      final String[] subA = new String[2];
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newVertex("SubFact").set("tenant", "b", "embedding", new float[] { 1f, i * 0.00001f }).save();
        subA[0] = database.newVertex("SubFact").set("tenant", "a", "embedding", new float[] { 1f, 4f }).save().getIdentity().toString();
        subA[1] = database.newVertex("SubFact").set("tenant", "a", "embedding", new float[] { 1f, 4.1f }).save().getIdentity()
            .toString();
      });
      final List<String> allowed = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Fact WHERE @type = 'Fact'")) {
        while (rs.hasNext())
          allowed.add(rs.next().<Object>getProperty("rid").toString());
      }
      allowed.add(subA[0]);

      final List<String> rids = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {filter: $allowed, where: {tenant: $tenant}}) YIELD node, score "
              + "RETURN node", Map.of("q", QUERY, "k", K, "tenant", "a", "allowed", allowed))) {
        while (rs.hasNext()) {
          final Document node = rs.next().getProperty("node");
          assertThat(node.getString("tenant")).isEqualTo("a");
          rids.add(node.getIdentity().toString());
        }
      }
      assertThat(rids).hasSize(K);
      // the sub-type's allowed tenant a record is the nearest of them all, the one the filter left out is not answered
      assertThat(rids.getFirst()).isEqualTo(subA[0]);
      assertThat(rids).doesNotContain(subA[1]);
    }

    @Test
    void filterMatchingNothingAnswersNothing() {
      assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {filter: []}) YIELD node, score "
          + "RETURN node.tenant AS tenant")).isEmpty();
    }

    @Test
    void whereMatchingNothingAnswersNothing() {
      assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {where: {tenant: 'nobody'}}) YIELD node, score "
          + "RETURN node.tenant AS tenant")).isEmpty();
      assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {where: \"tenant = 'nobody'\"}) YIELD node, score "
          + "RETURN node.tenant AS tenant")).isEmpty();
    }

    @Test
    void emptyOrNullOptionsBehaveAsTheThreeArgumentForm() {
      assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {}) YIELD node, score "
          + "RETURN node.tenant AS tenant")).hasSize(K).containsOnly("b");
      assertThat(tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, null) YIELD node, score "
          + "RETURN node.tenant AS tenant")).hasSize(K).containsOnly("b");
    }

    @Test
    void unknownOptionIsRejectedByName() {
      assertThatThrownBy(() -> tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, {tennant: 'a'}) YIELD node, score "
          + "RETURN node.tenant AS tenant"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("Unknown option 'tennant'");
    }

    @Test
    void optionsThatAreNotAMapAreRejected() {
      assertThatThrownBy(() -> tenants("CALL db.index.vector.queryNodes('Fact[embedding]', $k, $q, 'tenant = :tenant') YIELD node, score "
          + "RETURN node.tenant AS tenant"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("options must be a map");
    }

    private List<String> tenants(final String cypher) {
      final List<String> tenants = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", cypher, PARAMS)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          tenants.add(row.getProperty("tenant"));
        }
      }
      return tenants;
    }
  }
}
