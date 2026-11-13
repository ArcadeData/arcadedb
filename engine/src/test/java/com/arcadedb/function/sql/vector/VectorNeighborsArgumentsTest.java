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
import com.arcadedb.database.RID;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the vector SQL functions, one nested class per fixed issue.
 */
class VectorNeighborsArgumentsTest {
  /**
   * Issue #10611: two {@code vector.neighbors} arguments that make no sense were accepted silently. {@code rerank: 2.5} was cut
   * to 2 by {@code intValue()}, where the index METADATA refuses the same value for {@code rerankOversample}; and a negative
   * {@code k} answered no rows, which reads as "no neighbours" when it is a caller mistake. Both are refused now, and so is the
   * same negative {@code k} on the sibling functions that read it the same way.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NeighborsArgumentsTest extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"COSINE\" }");
      database.transaction(() -> {
        database.newDocument("Doc").set("name", "a", "vector", new float[] { 0.9f, 0.1f }).save();
        database.newDocument("Doc").set("name", "b", "vector", new float[] { 0.1f, 0.9f }).save();
        database.newDocument("Doc").set("name", "c", "vector", new float[] { 0.8f, 0.2f }).save();
      });
    }

    private List<String> names(final String sql, final Object... args) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT name FROM (SELECT expand(" + sql + "))", args)) {
        rs.forEachRemaining(r -> names.add(r.getProperty("name")));
      }
      return names;
    }

    @Test
    void aFractionalRerankIsRefusedNotTruncated() {
      final float[] query = { 1f, 0f };
      assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 3, { rerank: 2.5 })", (Object) query))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("rerank").hasMessageContaining("must be an integer");
      assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 3, ?)", query, Map.of("rerank", 2.5d)))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("rerank").hasMessageContaining("must be an integer");
      assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 3, ?)", query, Map.of("rerank", "2.5")))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("rerank").hasMessageContaining("must be an integer");
    }

    @Test
    void theOtherIntegerOptionsAreRefusedWhenFractionalToo() {
      final float[] query = { 1f, 0f };
      assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 3, ?)", query, Map.of("efSearch", 10.5d)))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("efSearch").hasMessageContaining("must be an integer");
      assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 3, ?)", query, Map.of("groupBy", "name", "groupSize", 1.5d)))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("groupSize").hasMessageContaining("must be an integer");
    }

    @Test
    void aWholeNumberWrittenAsADecimalStillWorks() {
      final float[] query = { 1f, 0f };
      assertThat(names("vectorNeighbors('Doc[vector]', ?, 3, ?)", query, Map.of("rerank", 2.0d, "efSearch", 20.0d))).hasSize(3);
      assertThat(names("vectorNeighbors('Doc[vector]', ?, 3, { rerank: 2 })", (Object) query)).hasSize(3);
      assertThat(names("vectorNeighbors('Doc[vector]', ?, 3.0)", (Object) query)).hasSize(3);
    }

    @Test
    void aNegativeKIsRefusedAndNamesTheParameter() {
      final float[] query = { 1f, 0f };
      assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, -1)", (Object) query))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative").hasMessageContaining("-1");
      assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', ?, ?)", query, -5))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative").hasMessageContaining("-5");
      assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, '-2')", (Object) query))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");
    }

    @Test
    void aFractionalKIsRefused() {
      final float[] query = { 1f, 0f };
      assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 2.5)", (Object) query))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must be an integer").hasMessageContaining("2.5");
    }

    @Test
    void aZeroKIsAValidEmptyRequest() {
      final float[] query = { 1f, 0f };
      assertThat(names("vectorNeighbors('Doc[vector]', ?, 0)", (Object) query)).isEmpty();
      assertThat(names("vectorNeighbors('Doc[vector]', ?, 1)", (Object) query)).hasSize(1);
    }

    @Test
    void theSiblingFunctionsRefuseANegativeKToo() {
      assertThatThrownBy(() -> names("`vector.sparseNeighbors`('Doc[vector]', [0, 1], [1.0, 1.0], -1)"))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");
      assertThatThrownBy(() -> names("`vector.discover`('Doc[vector]', [[#1:0, #1:1]], -1)"))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");
      assertThatThrownBy(() -> names("`vector.recommend`('Doc[vector]', [#1:0], [], -1)"))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");
      assertThatThrownBy(() -> names("`vector.rerank`((SELECT FROM Doc), [1.0, 0.0], 'vector', -1)"))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");
    }
  }

  /**
   * Issue #11052: the vector functions disagreed on a NULL vector and on an empty one. {@code vector.l2Norm} (the alias of
   * {@code vector.magnitude}) failed the whole query on a NULL where {@code vector.l1Norm} and {@code vector.lInfNorm}
   * answered NULL, and those two refused an empty vector whose norm {@code vector.l2Norm} answered as 0. The two-vector
   * functions (similarities, distances, element-wise arithmetic) failed the query on a NULL too, so one row of a type being
   * backfilled took down every other row of the result.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorDegenerateArgumentsTest extends TestHelper {
    private static final String[] VALUES = { "[1,2]", "2" };

    /**
     * The searches and the reranker: their arguments name an index, a query vector, a count of neighbours or the dimensions of
     * the vector to build, which the caller supplies once for the whole query and no row can lack, so a NULL one is the
     * caller's mistake and is refused. Refused with a message, never a crash.
     */
    private static final Set<String> SEARCH_ARGUMENTS = Set.of("vector.neighbors", "vector.sparseneighbors", "vector.discover",
        "vector.recommend", "vector.rerank", "vector.sparsecreate");

    /**
     * Over every registered {@code vector.*} function: a NULL argument never makes a call fail that would not fail with a value
     * in its place. A call refused for its arity, or for another argument, keeps failing the same way, which is why the
     * comparison is with the same call holding a value rather than with "must answer NULL". No call crashes on a NULL with
     * anything but a database exception.
     */
    @Test
    void aNullArgumentNeverIntroducesAFailure() {
      final List<String> violations = new ArrayList<>();
      for (final String name : new TreeSet<>(DefaultSQLFunctionFactory.getInstance().getBuiltInFunctionNames())) {
        if (!name.startsWith("vector."))
          continue;
        for (int arity = 1; arity <= 4; arity++)
          for (final String[] base : combinations(arity))
            for (int position = 0; position < arity; position++) {
              final String[] withNull = base.clone();
              withNull[position] = "null";
              final String failure = failureOf(name, withNull);
              if (failure == null)
                continue;

              if (!failure.startsWith(ArcadeDBException.class.getPackageName())) {
                violations.add(call(name, withNull) + " -> " + failure);
                continue;
              }
              if (SEARCH_ARGUMENTS.contains(name))
                continue;

              boolean sameFailureWithAValue = false;
              for (final String value : VALUES) {
                final String[] withValue = base.clone();
                withValue[position] = value;
                if (failure.equals(failureOf(name, withValue))) {
                  sameFailureWithAValue = true;
                  break;
                }
              }
              if (!sameFailureWithAValue)
                violations.add(call(name, withNull) + " -> " + failure);
            }
      }
      assertThat(violations).as("calls a NULL argument made fail").isEmpty();
    }

    @Test
    void theThreeNormsAgreeOnNullAndEmpty() {
      for (final String norm : new String[] { "vector.l1Norm", "vector.l2Norm", "vector.lInfNorm", "vector.magnitude" }) {
        assertThat(scalar("SELECT `" + norm + "`([3,4]) as v")).as(norm).isNotNull();
        assertThat(scalar("SELECT `" + norm + "`(null) as v")).as(norm + "(null)").isNull();
        assertThat(((Number) scalar("SELECT `" + norm + "`([]) as v")).floatValue()).as(norm + "([])").isEqualTo(0.0f);
      }
      assertThat(((Number) scalar("SELECT `vector.l1Norm`([3,4]) as v")).floatValue()).isEqualTo(7.0f);
      assertThat(((Number) scalar("SELECT `vector.l2Norm`([3,4]) as v")).floatValue()).isEqualTo(5.0f);
      assertThat(((Number) scalar("SELECT `vector.lInfNorm`([3,4]) as v")).floatValue()).isEqualTo(4.0f);
    }

    /**
     * An empty vector answers what the function answers on a zero vector wherever that value is defined. Cosine similarity,
     * variance, standard deviation, sparsity and the quantizations are 0/0 on it and keep refusing it.
     */
    @Test
    void anEmptyVectorAnswersWhatAZeroVectorAnswers() {
      assertThat((float[]) scalar("SELECT `vector.normalize`([]) as v")).isEmpty();
      assertThat((float[]) scalar("SELECT `vector.normalize`([0,0]) as v")).containsExactly(0.0f, 0.0f);
      assertThat((float[]) scalar("SELECT `vector.clip`([], 0, 1) as v")).isEmpty();
      assertThat(scalar("SELECT `vector.hasNaN`([]) as v")).isEqualTo(false);
      assertThat(scalar("SELECT `vector.hasInf`([]) as v")).isEqualTo(false);
      assertThat(scalar("SELECT `vector.hasNull`([]) as v")).isEqualTo(false);
      assertThat(scalar("SELECT `vector.isNormalized`([]) as v")).isEqualTo(false);
      assertThat(scalar("SELECT `vector.isNormalized`([0,0]) as v")).isEqualTo(false);
      assertThat(scalar("SELECT `vector.toString`([]) as v")).isEqualTo("[]");
      assertThat(scalar("SELECT `vector.dimension`([]) as v")).isEqualTo(0);
    }

    /** The consequence the issue measured: one row without an embedding no longer takes the other rows down with it. */
    @Test
    void aRowWithoutAnEmbeddingAnswersNullAndKeepsTheOtherRows() {
      database.command("sql", "CREATE DOCUMENT TYPE Emb");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Emb SET id = 1, v = [3,4]");
        database.command("sql", "INSERT INTO Emb SET id = 2");
      });

      for (final String projection : new String[] { "`vector.l2Norm`(v)", "`vector.magnitude`(v)", "`vector.normalize`(v)",
          "`vector.cosineSimilarity`(v, [1,0])", "`vector.dotProduct`(v, [1,0])", "`vector.l2Distance`(v, [1,0])",
          "`vector.l1Distance`(v, [1,0])", "`vector.multiply`(v, [1,1])", "`vector.add`(v, 1)", "`vector.subtract`(v, [1,1])",
          "`vector.scale`(v, 2)" }) {
        try (final ResultSet rs = database.query("sql", "SELECT id, " + projection + " as x FROM Emb ORDER BY id")) {
          final Result first = rs.next();
          assertThat(first.<Integer>getProperty("id")).as(projection).isEqualTo(1);
          assertThat(first.<Object>getProperty("x")).as(projection).isNotNull();
          final Result second = rs.next();
          assertThat(second.<Integer>getProperty("id")).as(projection).isEqualTo(2);
          assertThat(second.<Object>getProperty("x")).as(projection).isNull();
          assertThat(rs.hasNext()).isFalse();
        }
      }
    }

    private Object scalar(final String query) {
      try (final ResultSet rs = database.query("sql", query)) {
        return rs.next().getProperty("v");
      }
    }

    /** The failure message of the call, or null when it answers. */
    private String failureOf(final String name, final String[] args) {
      try (final ResultSet rs = database.query("sql", call(name, args))) {
        rs.next();
        return null;
      } catch (final Exception e) {
        return e.getClass().getName() + ": " + e.getMessage();
      }
    }

    private static String call(final String name, final String[] args) {
      return "SELECT `" + name + "`(" + String.join(", ", args) + ") as v";
    }

    private static List<String[]> combinations(final int arity) {
      final List<String[]> out = new ArrayList<>();
      final int total = (int) Math.pow(VALUES.length, arity);
      for (int i = 0; i < total; i++) {
        final String[] args = new String[arity];
        int n = i;
        for (int p = 0; p < arity; p++) {
          args[p] = VALUES[n % VALUES.length];
          n /= VALUES.length;
        }
        out.add(args);
      }
      return out;
    }
  }

  /**
   * Regression tests for issue #6391: three {@code vector.*} sparse/ANN functions threw raw
   * exceptions (or imposed an unnecessary restriction) on valid input, inconsistent with their own
   * siblings.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorSparseAnnGuardsTest extends TestHelper {

    @Override
    public void beginTest() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc IF NOT EXISTS");
        database.command("sql", "CREATE PROPERTY Doc.name IF NOT EXISTS STRING");
        database.command("sql", "CREATE PROPERTY Doc.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (name) UNIQUE");
        database.command("sql", """
            CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR
            METADATA {
              dimensions: 3,
              similarity: 'COSINE',
              idPropertyName: 'name'
            }""");
      });

      database.transaction(() -> {
        database.newVertex("Doc").set("name", "docA").set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
        database.newVertex("Doc").set("name", "docB").set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).save();
      });
    }

    // 1. sparseDot must not require equal inferred dimensions: it is implicitly 0 over non-overlapping indices.
    @Test
    void sparseDotOnDisjointIndicesReturnsZeroInsteadOfThrowing() {
      try (final ResultSet rs = database.query("sql",
          "SELECT `vector.sparseDot`(`vector.sparseCreate`([0,2],[0.5,0.3]), `vector.sparseCreate`([1,5],[0.2,0.8])) as score")) {
        assertThat(rs.hasNext()).isTrue();
        final Result result = rs.next();
        assertThat(result.<Float>getProperty("score")).isEqualTo(0.0f);
      }
    }

    @Test
    void sparseDotOnDifferentDimensionsWithOverlapStillComputesCorrectly() {
      try (final ResultSet rs = database.query("sql",
          "SELECT `vector.sparseDot`(`vector.sparseCreate`([0,2],[0.5,0.3]), `vector.sparseCreate`([0,1,5],[0.6,0.2,0.8])) as score")) {
        final Result result = rs.next();
        // only index 0 overlaps: 0.5 * 0.6 = 0.30
        assertThat(result.<Float>getProperty("score")).isEqualTo(0.30f, org.assertj.core.data.Offset.offset(0.0001f));
      }
    }

    // 2. sparseCreate must accept long[] indices (how integer JSON arrays arrive from HTTP), not just int[]/Object[].
    @Test
    void sparseCreateAcceptsLongArrayIndices() {
      final long[] indices = { 0L, 2L, 5L };
      final float[] values = { 0.5f, 0.3f, 0.8f };

      try (final ResultSet rs = database.query("sql", "SELECT `vector.sparseCreate`(?, ?) as sparse_emb",
          (Object) indices, (Object) values)) {
        assertThat(rs.hasNext()).isTrue();
        final Result result = rs.next();
        assertThat((Object) result.getProperty("sparse_emb")).isNotNull();
      }
    }

    // 3. vector.neighbors(k <= 0) must not reach a negative-capacity ArrayList allocation deep in LSMVectorIndex. k = 0 is an
    //    empty request; a negative k is refused with a typed error (issue #10611) rather than answered with no rows, which
    //    would read as "no neighbours".
    @Test
    void neighborsWithNegativeKIsRefusedAndZeroReturnsEmpty() {
      final SQLFunctionVectorNeighbors function = new SQLFunctionVectorNeighbors();
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      assertThatThrownBy(() -> function.execute(null, null, null,
          new Object[] { "Doc[embedding]", new float[] { 1.0f, 0.0f, 0.0f }, -1 }, context))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");

      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> zero = (List<Map<String, Object>>) function.execute(null, null, null,
          new Object[] { "Doc[embedding]", new float[] { 1.0f, 0.0f, 0.0f }, 0 }, context);
      assertThat(zero).isNotNull().isEmpty();
    }
  }

  /**
   * Issue #8959 for {@code vector.sparseNeighbors}: a {@code filter} that resolves to no RIDs matches nothing, it does not
   * lift the restriction.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EmptySparseFilterTest extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final var type = database.getSchema().buildDocumentType().withName("Doc").create();
        type.createProperty("tenant", Type.STRING);
        type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
        type.createProperty("weights", Type.ARRAY_OF_FLOATS);
        database.getSchema().buildTypeIndex("Doc", new String[] { "tokens", "weights" }).withSparseVectorType().withDimensions(4)
            .create();
        for (final String tenant : new String[] { "a", "b" }) {
          final MutableDocument d = database.newDocument("Doc");
          d.set("tenant", tenant);
          d.set("tokens", new int[] { 1 });
          d.set("weights", new float[] { 1.0f });
          d.save();
        }
      });
    }

    private int count(final String filter, final String tenant) {
      final List<RID> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(`vector.sparseNeighbors`('Doc[tokens,weights]', [1], [1.0], 5, { filter: " + filter + " }))",
          Map.of("tenant", tenant, "ids", ids))) {
        int n = 0;
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
        return n;
      }
    }

    @Test
    void subqueryMatchingNothingReturnsNothing() {
      assertThat(count("(SELECT @rid FROM Doc WHERE tenant = :tenant)", "c")).isZero();
    }

    @Test
    void emptyRidListReturnsNothing() {
      assertThat(count(":ids", "a")).isZero();
    }

    @Test
    void nestedEmptyListAndListOfNullsReturnNothing() {
      final List<Object> nested = new ArrayList<>();
      nested.add(new ArrayList<RID>());
      final List<Object> nulls = new ArrayList<>();
      nulls.add(null);
      nulls.add(null);
      for (final List<Object> ids : List.of(nested, nulls))
        try (final ResultSet rs = database.query("sql",
            "SELECT expand(`vector.sparseNeighbors`('Doc[tokens,weights]', [1], [1.0], 5, { filter: :ids }))", Map.of("ids", ids))) {
          assertThat(rs.hasNext()).isFalse();
        }
    }

    @Test
    void filterMatchingARecordStillRestrictsTheSearch() {
      assertThat(count("(SELECT @rid FROM Doc WHERE tenant = :tenant)", "a")).isEqualTo(1);
    }

    @Test
    void badIndexSpecStaysLoudWithAnEmptyFilter() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT expand(`vector.sparseNeighbors`('Nope[tokens,weights]', [1], [1.0], 5, { filter: :ids }))",
            Map.of("ids", new ArrayList<RID>()))) {
          rs.hasNext();
        }
      }).isInstanceOf(SchemaException.class);
    }
  }

  /**
   * Issue #8959: a {@code filter} option of {@code vector.neighbors} that resolves to no RIDs was read as "no filter", so a
   * tenant subquery with no rows returned the records of every other tenant.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EmptyVectorFilterTest extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.tenant STRING");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"COSINE\" }");
      database.transaction(() -> {
        database.newDocument("Doc").set("name", "a1", "tenant", "a", "vector", new float[] { 0.9f, 0.1f }).save();
        database.newDocument("Doc").set("name", "a2", "tenant", "a", "vector", new float[] { 0.1f, 0.9f }).save();
        database.newDocument("Doc").set("name", "b1", "tenant", "b", "vector", new float[] { 0.8f, 0.2f }).save();
        database.newDocument("Doc").set("name", "b2", "tenant", "b", "vector", new float[] { 0.2f, 0.8f }).save();
      });
    }

    private List<String> search(final String filter, final String tenant, final List<RID> ids) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT name FROM (SELECT expand(vectorNeighbors('Doc[vector]', :v, 3, { filter: " + filter + " })))",
          Map.of("v", new float[] { 1.0f, 0.0f }, "tenant", tenant, "ids", ids))) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          names.add(r.getProperty("name"));
        }
      }
      return names;
    }

    private List<RID> idsOf(final String tenant) {
      final List<RID> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE tenant = ?", tenant)) {
        rs.forEachRemaining(r -> ids.add(r.getIdentity().get()));
      }
      return ids;
    }

    @Test
    void subqueryMatchingNothingReturnsNothing() {
      assertThat(search("(SELECT @rid FROM Doc WHERE tenant = :tenant)", "c", idsOf("c"))).isEmpty();
    }

    @Test
    void emptyRidListReturnsNothing() {
      assertThat(search(":ids", "c", idsOf("c"))).isEmpty();
    }

    @Test
    void badIndexSpecStaysLoudWithAnEmptyFilter() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT expand(vectorNeighbors('Nope[vector]', [1.0, 0.0], 3, { filter: :ids }))", Map.of("ids", new ArrayList<RID>()))) {
          rs.hasNext();
        }
      }).isInstanceOf(SchemaException.class);
    }

    @Test
    void nestedEmptyListAndListOfNullsReturnNothing() {
      final List<Object> nested = new ArrayList<>();
      nested.add(new ArrayList<RID>());
      final List<Object> nulls = new ArrayList<>();
      nulls.add(null);
      for (final List<Object> ids : List.of(nested, nulls))
        try (final ResultSet rs = database.query("sql",
            "SELECT expand(vectorNeighbors('Doc[vector]', [1.0, 0.0], 3, { filter: :ids }))", Map.of("ids", ids))) {
          assertThat(rs.hasNext()).isFalse();
        }
    }

    @Test
    void nullFilterIsTheAbsentFilterAndSearchesEverything() {
      // PINNED CONTRACT: ONLY A FILTER THAT RESOLVED TO A COLLECTION CAN MATCH NOTHING. A NULL VALUE IS "NO FILTER", LIKE AN ABSENT KEY
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(vectorNeighbors('Doc[vector]', [1.0, 0.0], 3, { filter: :none }))", hashMapWithNull())) {
        int n = 0;
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
        assertThat(n).isEqualTo(3);
      }
    }

    @Test
    void filterMatchingRecordsStillRestrictsTheSearch() {
      assertThat(search("(SELECT @rid FROM Doc WHERE tenant = :tenant)", "a", idsOf("a"))).containsExactlyInAnyOrder("a1", "a2");
      assertThat(search(":ids", "b", idsOf("b"))).containsExactlyInAnyOrder("b1", "b2");
    }

    @Test
    void absentFilterStillSearchesEverything() {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT name FROM (SELECT expand(vectorNeighbors('Doc[vector]', [1.0, 0.0], 3)))")) {
        rs.forEachRemaining(r -> names.add(r.getProperty("name")));
      }
      assertThat(names).hasSize(3);
    }

    private static Map<String, Object> hashMapWithNull() {
      final Map<String, Object> params = new HashMap<>();
      params.put("none", null);
      return params;
    }
  }
}
