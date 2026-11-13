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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.EquivalentIndexException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Type index integrity: planning, equivalence and serialization regressions.
 */
class TypeIndexIntegrityTest {
  /**
   * Regression for #11303: {@link TypeIndex#isReadyForQueries()}, the predicate every planner asks, did not ask {@link TypeIndex#isComplete()},
   * so an index missing the bucket index of one of the buckets it covers was planned on and a lookup through it answered short, while
   * {@link TypeIndex#isValid()} and the load-time SEVERE line both said it was not used. And {@code DROP INDEX} of a bucket index by its
   * own name was the statement that left a type index in that state.
   */
  @Nested
  class IncompleteTypeIndex extends TestHelper {
    private static final String INDEX = "S[k]";

    @Test
    void anIncompleteIndexIsNotPlannedAndTheLookupAnswersWhole() {
      final Bucket subBucket = prepare();
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
      assertThat(index.isReadyForQueries()).isTrue();
      assertThat(explain("SELECT FROM S WHERE k = 'a1'")).contains("FETCH FROM INDEX");

      // the Java API still drops a bucket index by name: it is how the repair tests build the state an earlier version left behind
      database.getSchema().dropIndex(bucketIndexOf(index, subBucket).getName());

      assertThat(index.isValid()).isFalse();
      assertThat(index.isComplete()).isFalse();
      assertThat(index.getBucketsWithoutIndex()).containsExactly(subBucket);
      assertThat(index.isReadyForQueries()).as("an index missing a bucket's sub-index is no planning candidate").isFalse();
      assertThat(TypeIndex.filterReadyForQueries(database.getSchema().getType("S").getAllIndexes(true))).doesNotContain(index);

      // SQL: the plan and the answer, which together fail on the code before the fix
      assertThat(explain("SELECT FROM S WHERE k = 'a1'")).doesNotContain("FETCH FROM INDEX");
      assertThat(count("sql", "SELECT count(*) AS c FROM S WHERE k = 'a1'")).isEqualTo(1L);
      assertThat(count("sql", "SELECT count(*) AS c FROM S WHERE k = 's1'")).isEqualTo(1L);
      // the range and IN shapes go through the same candidate list
      assertThat(count("sql", "SELECT count(*) AS c FROM S WHERE k >= 'a' AND k < 'b'")).isEqualTo(1L);
      assertThat(count("sql", "SELECT count(*) AS c FROM S WHERE k IN ['a1', 's1']")).isEqualTo(2L);
      // openCypher
      assertThat(count("opencypher", "MATCH (n:S) WHERE n.k = 'a1' RETURN count(n) AS c")).isEqualTo(1L);
      assertThat(count("opencypher", "MATCH (n:S {k: 'a1'}) RETURN count(n) AS c")).isEqualTo(1L);
      // the native select API
      assertThat(database.select().fromType("S").where().property("k").eq().value("a1").documents().stream().count()).isEqualTo(1L);

      // SHOW INDEXES does not call it ONLINE, nor POPULATING: nothing is filling it
      assertThat(indexState()).isEqualTo("FAILED");

      // the lookup that has no scan to fall back on refuses, and says why, instead of answering short or "no index"
      final Throwable refused = catchThrowable(() -> database.lookupByKey("S", "k", "a1"));
      assertThat(refused).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(INDEX).hasMessageContaining("REBUILD INDEX");

      // REBUILD INDEX repairs it, and the planner uses it again
      database.command("sql", "REBUILD INDEX `" + INDEX + "`");
      final TypeIndex repaired = (TypeIndex) database.getSchema().getIndexByName(INDEX);
      assertThat(repaired.isReadyForQueries()).isTrue();
      assertThat(explain("SELECT FROM S WHERE k = 'a1'")).contains("FETCH FROM INDEX");
      assertThat(count("sql", "SELECT count(*) AS c FROM S WHERE k = 'a1'")).isEqualTo(1L);
      assertThat(database.lookupByKey("S", "k", "a1").hasNext()).isTrue();
      assertThat(indexState()).isEqualTo("ONLINE");
    }

    private String indexState() {
      try (final ResultSet rs = database.query("opencypher", "SHOW INDEXES")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          if (INDEX.equals(row.getProperty("name")))
            return row.getProperty("state");
        }
      }
      throw new IllegalStateException("SHOW INDEXES does not list " + INDEX);
    }

    @Test
    void dropIndexOfABucketIndexByNameIsRefusedInSqlAndCypher() {
      final Bucket subBucket = prepare();
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
      final String bucketIndexName = bucketIndexOf(index, subBucket).getName();

      for (final String[] statement : new String[][] { { "sql", "DROP INDEX `" + bucketIndexName + "`" },
          { "sql", "DROP INDEX `" + bucketIndexName + "` IF EXISTS" }, { "opencypher", "DROP INDEX `" + bucketIndexName + "`" } }) {
        final Throwable refused = catchThrowable(() -> database.command(statement[0], statement[1]));
        assertThat(refused).as(statement[1]).isNotNull();
        assertThat(CauseChain.find(refused, SchemaException.class)).as("got %s", refused).isNotNull();
        assertThat(CauseChain.find(refused, SchemaException.class).getMessage()).contains(bucketIndexName).contains(INDEX);

        // nothing changed: the index is whole, planned on, and still refuses a duplicate in the sub type's bucket
        assertThat(database.getSchema().existsIndex(bucketIndexName)).isTrue();
        assertThat(index.isComplete()).isTrue();
        assertThat(index.isReadyForQueries()).isTrue();
      }

      assertThat(catchThrowable(() -> database.transaction(() -> database.command("sql", "INSERT INTO A SET k = 'a1'")))).isInstanceOf(
          DuplicatedKeyException.class);

      // the type index itself still drops, with every bucket index it holds
      database.command("sql", "DROP INDEX `" + INDEX + "`");
      assertThat(database.getSchema().existsIndex(INDEX)).isFalse();
      assertThat(database.getSchema().existsIndex(bucketIndexName)).isFalse();
    }

    private Bucket prepare() {
      database.command("sql", "CREATE VERTEX TYPE S BUCKETS 1");
      database.command("sql", "CREATE PROPERTY S.k STRING");
      database.command("sql", "CREATE INDEX ON S (k) UNIQUE");
      database.command("sql", "CREATE VERTEX TYPE A EXTENDS S BUCKETS 1");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO S SET k = 's1'");
        database.command("sql", "INSERT INTO A SET k = 'a1'");
      });
      assertThat(database.getSchema().getIndexByName(INDEX).getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
      return database.getSchema().getType("A").getBuckets(false).getFirst();
    }

    private static IndexInternal bucketIndexOf(final TypeIndex index, final Bucket bucket) {
      for (final IndexInternal onBucket : index.getIndexesOnBuckets())
        if (onBucket.getAssociatedBucketId() == bucket.getFileId())
          return onBucket;
      throw new IllegalStateException("No bucket index for bucket " + bucket.getName());
    }

    private String explain(final String query) {
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
        return rs.next().getProperty("executionPlanAsString");
      }
    }

    private long count(final String language, final String query) {
      try (final ResultSet rs = database.query(language, query)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }
  }

  /**
   * Regression for #11392: an index the type already has under another name, or asked for twice without {@code IF NOT EXISTS}, is refused
   * as an {@link EquivalentIndexException}: the definition exists already, which every wire protocol answers as a conflict
   * ({@link ErrorCategory#ALREADY_EXISTS}, Bolt {@code Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists}, Postgres
   * {@code 42P07}) rather than as a malformed argument. Still an {@link IllegalArgumentException}, as the builders raised before,
   * and an index of a DIFFERENT definition on the same properties keeps the plain one: that is no equivalent rule.
   */
  @Nested
  class EquivalentIndex extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Eq").createProperty("a", Type.STRING);
        database.getSchema().getType("Eq").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
      });
    }

    @Test
    void theSameIndexUnderAnotherNameIsEquivalent() {
      final Throwable refused = catchThrowable(() -> database.command("sql", "CREATE INDEX eqOther ON Eq (a) NOTUNIQUE"));

      assertThat(refused).isInstanceOf(IllegalArgumentException.class);
      assertThat(causeChainHolds(refused)).as("the refusal is an equivalent index").isTrue();
      assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.ALREADY_EXISTS);
    }

    @Test
    void theSameIndexAskedTwiceWithoutIfNotExistsIsEquivalent() {
      final Throwable refused = catchThrowable(
          () -> database.getSchema().getType("Eq").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a"));

      assertThat(refused).isInstanceOf(EquivalentIndexException.class).hasMessageContaining("It already provides what was requested");
      assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.ALREADY_EXISTS);
    }

    @Test
    void aCypherIndexUnderAnotherNameIsEquivalent() {
      database.command("opencypher", "CREATE INDEX cyA FOR (n:CyEq) ON (n.a)");

      final Throwable refused = catchThrowable(() -> database.command("opencypher", "CREATE INDEX cyB FOR (n:CyEq) ON (n.a)"));

      assertThat(causeChainHolds(refused)).isTrue();
      assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.ALREADY_EXISTS);
    }

    /** The control: another definition on the same properties is no equivalent rule, and stays the argument error it was. */
    @Test
    void anotherDefinitionOnTheSamePropertiesIsNotEquivalent() {
      final Throwable refused = catchThrowable(
          () -> database.getSchema().getType("Eq").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "a"));

      assertThat(refused).isInstanceOf(IllegalArgumentException.class).isNotInstanceOf(EquivalentIndexException.class);
      assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.VALIDATION);
    }

    private static boolean causeChainHolds(final Throwable error) {
      for (Throwable t = error; t != null; t = t.getCause())
        if (t instanceof EquivalentIndexException)
          return true;
      return false;
    }
  }

  /**
   * Regression for #11569: a bucket index whose bucket association was lost ({@code associatedBucketId == -1}: corrupt, orphaned or
   * imported metadata) made {@code toJSON()} call {@code getBucketById(-1)}, so serializing its type - and with it every schema
   * save, whatever the change being saved - raised "Bucket with id '-1' was not found". The LSM tree had already been fixed;
   * the hash, vector, sparse-vector, full-text and geospatial indexes had not.
   * <p>
   * A census over {@link Schema.INDEX_TYPE}, the authority that defines the index kinds: every kind is created, orphaned and
   * serialized, and a kind this test does not know how to create fails it, so the next index kind cannot ship without the
   * guarantee.
   */
  @Nested
  class OrphanBucketIndexSerialization extends TestHelper {

    @Test
    void everyIndexKindSerializesAnOrphanedBucketIndex() {
      final Set<Schema.INDEX_TYPE> reached = EnumSet.noneOf(Schema.INDEX_TYPE.class);

      for (final Schema.INDEX_TYPE kind : Schema.INDEX_TYPE.values()) {
        final String typeName = "Orphan" + kind.name().replace("_", "");
        createIndexOfKind(kind, typeName);

        final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
        final List<TypeIndex> typeIndexes = new ArrayList<>(type.getAllIndexes(false));
        assertThat(typeIndexes).as("one index of kind %s on %s", kind, typeName).hasSize(1);
        final TypeIndex typeIndex = typeIndexes.getFirst();
        assertThat(typeIndex.getType()).isEqualTo(kind);

        for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets()) {
          final String expectedBucket = database.getSchema().getBucketById(bucketIndex.getAssociatedBucketId()).getName();
          final int associatedBucketId = bucketIndex.getMetadata().associatedBucketId;
          bucketIndex.getMetadata().associatedBucketId = -1;
          try {
            final JSONObject json = bucketIndex.toJSON();
            // recovered from the <bucketName>_<uniqueId> name of the index file, so the loader can relink the orphan
            assertThat(json.getString("bucket")).as("bucket of the orphaned %s index %s", kind, bucketIndex.getName())
                .isEqualTo(expectedBucket);
            assertThat(type.toJSON().getJSONObject("indexes").length()).as("the type of the orphaned %s index serializes", kind)
                .isEqualTo(typeIndex.getIndexesOnBuckets().length);
            assertThat(database.getSchema().getEmbedded().toJSON().getJSONObject("types").has(typeName))
                .as("the schema holding the orphaned %s index serializes", kind).isTrue();
          } finally {
            // put back, or the CHECK DATABASE run after every test reports the type index as lacking the index of that bucket
            bucketIndex.getMetadata().associatedBucketId = associatedBucketId;
          }
        }
        reached.add(kind);
      }

      assertThat(reached).as("the census reaches every Schema.INDEX_TYPE").containsExactlyInAnyOrder(Schema.INDEX_TYPE.values());
    }

    @Test
    void anOrphanWhoseNameNamesNoBucketSerializesWithAnEmptyBucket() {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      final Schema schema = database.getSchema();
      assertThat(IndexInternal.bucketNameForJSON(schema, -1, "NoSuchBucket_123")).isEmpty();
      assertThat(IndexInternal.bucketNameForJSON(schema, -1, "noUnderscore")).isEmpty();
      assertThat(IndexInternal.bucketNameForJSON(schema, -1, null)).isEmpty();
      // a stale id that names no bucket falls back to the name
      final String docBucket = schema.getType("Doc").getBuckets(false).getFirst().getName();
      assertThat(IndexInternal.bucketNameForJSON(schema, Integer.MAX_VALUE, docBucket + "_42")).isEqualTo(docBucket);
      assertThat(IndexInternal.bucketNameForJSON(schema, schema.getBucketByName(docBucket).getFileId(), "whatever"))
          .isEqualTo(docBucket);
    }

    private void createIndexOfKind(final Schema.INDEX_TYPE kind, final String typeName) {
      database.command("sql", "CREATE DOCUMENT TYPE " + typeName + " BUCKETS 2");
      final String create = switch (kind) {
        case LSM_TREE -> {
          database.command("sql", "CREATE PROPERTY " + typeName + ".p STRING");
          yield "CREATE INDEX ON " + typeName + " (p) NOTUNIQUE";
        }
        case HASH -> {
          database.command("sql", "CREATE PROPERTY " + typeName + ".p STRING");
          yield "CREATE INDEX ON " + typeName + " (p) NOTUNIQUE_HASH";
        }
        case FULL_TEXT -> {
          database.command("sql", "CREATE PROPERTY " + typeName + ".p STRING");
          yield "CREATE INDEX ON " + typeName + " (p) FULL_TEXT";
        }
        case GEOSPATIAL -> {
          database.command("sql", "CREATE PROPERTY " + typeName + ".p STRING");
          yield "CREATE INDEX ON " + typeName + " (p) GEOSPATIAL";
        }
        case COLUMNAR -> {
          database.command("sql", "CREATE PROPERTY " + typeName + ".p STRING");
          yield "CREATE INDEX ON " + typeName + " (p) COLUMNAR";
        }
        case LSM_VECTOR -> {
          database.command("sql", "CREATE PROPERTY " + typeName + ".p ARRAY_OF_FLOATS");
          yield "CREATE INDEX ON " + typeName + " (p) LSM_VECTOR METADATA { dimensions: 4, similarity: 'COSINE' }";
        }
        case LSM_SPARSE_VECTOR -> {
          database.command("sql", "CREATE PROPERTY " + typeName + ".tokens ARRAY_OF_INTEGERS");
          database.command("sql", "CREATE PROPERTY " + typeName + ".weights ARRAY_OF_FLOATS");
          yield "CREATE INDEX ON " + typeName + " (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }";
        }
        // no default: a new Schema.INDEX_TYPE constant fails to compile here, which is the point of the census
      };
      database.command("sql", create);
    }
  }
}
