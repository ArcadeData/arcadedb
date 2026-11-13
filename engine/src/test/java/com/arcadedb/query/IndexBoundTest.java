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
package com.arcadedb.query;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexKeyEquality;
import com.arcadedb.index.IntegralKeyBound;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.select.SelectOperator;
import com.arcadedb.query.select.SelectWhereAfterBlock;
import com.arcadedb.query.select.SelectWhereOperatorBlock;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Consolidated regression tests, one nested class per original issue.
 */
class IndexBoundTest {
  /**
   * Regression for #8865: planners never answer from a partially built index.
   * <p>
   * Issue #8865: a TypeIndex is registered on its type before the build has scanned every bucket, so the planners could choose it and
   * answer from a partially populated index. {@code TypeIndex.isReadyForQueries()} (#9331) is the "population complete" state, and
   * every reader of a type's indexes has to honour it: here SQL and OpenCypher, by equality, range, {@code IN} and an ordering that
   * only an index can give, probed from inside the build while the index holds part of the records.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PartiallyBuiltIndex extends TestHelper {
    private static final int ROWS = 400;

    @Override
    public void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Person BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Person.id LONG");
      database.transaction(() -> {
        for (long i = 0; i < ROWS; i++)
          database.newVertex("Person").set("id", i).save();
      });
    }

    @Test
    void noQuerySurfaceAnswersFromAnIndexWhileItIsBeingBuilt() {
      assertProbedWhileBuilding(false);
    }

    @Test
    void noQuerySurfaceAnswersFromAUniqueIndexWhileItIsBeingBuilt() {
      assertProbedWhileBuilding(true);
    }

    private void assertProbedWhileBuilding(final boolean unique) {
      final AtomicBoolean probed = new AtomicBoolean();
      final AtomicLong wrong = new AtomicLong();
      final StringBuilder detail = new StringBuilder();

      database.getSchema().getType("Person").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, unique, new String[] { "id" }, 262_144,
          (document, totalIndexed) -> {
            // the index is registered bucket by bucket and holds part of the records here
            if (totalIndexed < 10 || !probed.compareAndSet(false, true))
              return;
            check(wrong, detail, "sql =", 1, count(database.query("sql", "SELECT FROM Person WHERE id = 7")));
            check(wrong, detail, "sql >=", ROWS - 50, count(database.query("sql", "SELECT FROM Person WHERE id >= 50")));
            check(wrong, detail, "sql between", 21, count(database.query("sql", "SELECT FROM Person WHERE id BETWEEN 100 AND 120")));
            check(wrong, detail, "sql in", 3, count(database.query("sql", "SELECT FROM Person WHERE id IN [1, 200, 399]")));
            check(wrong, detail, "sql order by", ROWS, count(database.query("sql", "SELECT FROM Person ORDER BY id")));
            check(wrong, detail, "sql count", ROWS, database.query("sql", "SELECT count(*) AS c FROM Person WHERE id >= 0").next()
                .<Number>getProperty("c").longValue());
            check(wrong, detail, "cypher =", 1, count(database.query("opencypher", "MATCH (n:Person {id: 7}) RETURN n.id")));
            check(wrong, detail, "cypher where", 1, count(database.query("opencypher", "MATCH (n:Person) WHERE n.id = 399 RETURN n.id")));
            check(wrong, detail, "cypher range", ROWS - 50,
                count(database.query("opencypher", "MATCH (n:Person) WHERE n.id >= 50 RETURN n.id")));
            check(wrong, detail, "cypher param", 1, count(database.query("opencypher", "MATCH (n:Person {id: $id}) RETURN n.id", Map.of("id", 123L))));
          });

      assertThat(probed.get()).as("the build callback must have run").isTrue();
      assertThat(wrong.get()).as("answers that came from the half built index: " + detail).isZero();
      assertThat(count(database.query("sql", "SELECT FROM Person WHERE id = 7"))).isEqualTo(1);
      assertThat(count(database.query("opencypher", "MATCH (n:Person {id: 7}) RETURN n.id"))).isEqualTo(1);
    }

    private static void check(final AtomicLong wrong, final StringBuilder detail, final String surface, final long expected, final long actual) {
      if (expected != actual) {
        wrong.incrementAndGet();
        detail.append(surface).append(" expected ").append(expected).append(" got ").append(actual).append("; ");
      }
    }

    private static long count(final ResultSet rs) {
      try (rs) {
        long rows = 0;
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
        return rows;
      }
    }
  }

  /**
   * Regression for #8919: a bound a FLOAT property cannot hold is read the same by index, scan and Cypher.
   * <p>
   * Issue #8919: a bound a FLOAT property cannot hold is read the same way by the index (which converts it to its FLOAT key), by a
   * scan and by Cypher, and a value is never equal to a bound and below it at once.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FloatRangeBound extends TestHelper {

    @Override
    public void beginTest() {
      database.transaction(() -> {
        for (final String type : new String[] { "I", "N" }) {
          database.command("sql", "CREATE VERTEX TYPE " + type);
          database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
          database.command("sql", "CREATE PROPERTY " + type + ".v FLOAT");
          database.command("sql", "CREATE PROPERTY " + type + ".d DOUBLE");
        }
        database.command("sql", "CREATE INDEX ON I (v) NOTUNIQUE");
        database.command("sql", "CREATE INDEX ON I (d) NOTUNIQUE");
        for (final String type : new String[] { "I", "N" }) {
          database.command("sql", "INSERT INTO " + type + " SET id = 1, v = 1.0, d = 1.0");
          database.command("sql", "INSERT INTO " + type + " SET id = 2, v = 16777216, d = 9007199254740992.0");
          database.command("sql", "INSERT INTO " + type + " SET id = 3, v = 100000, d = 5.0");
        }
      });
    }

    @Test
    void sqlOrderingAgreesWithEqualityOnAnUnrepresentableFloatBound() {
      for (final String type : new String[] { "I", "N" }) {
        assertThat(sql("SELECT id FROM " + type + " WHERE v = 16777217.0")).as(type + " =").containsExactly(2);
        assertThat(sql("SELECT id FROM " + type + " WHERE v >= 16777217.0")).as(type + " >=").containsExactly(2);
        assertThat(sql("SELECT id FROM " + type + " WHERE v > 16777217.0")).as(type + " >").isEmpty();
        assertThat(sql("SELECT id FROM " + type + " WHERE v <= 16777217.0")).as(type + " <=").containsExactly(1, 2, 3);
        assertThat(sql("SELECT id FROM " + type + " WHERE v < 16777217.0")).as(type + " <").containsExactly(1, 3);
        assertThat(sql("SELECT id FROM " + type + " WHERE v <> 16777217.0")).as(type + " <>").containsExactly(1, 3);
        assertThat(sql("SELECT id FROM " + type + " WHERE v >= 16777217.0 OR v < 16777217.0")).as(type + " >= or <").containsExactly(1, 2, 3);
        assertThat(sql("SELECT id FROM " + type + " WHERE v BETWEEN 16777217.0 AND 16777218.0")).as(type + " between").containsExactly(2);
      }
    }

    @Test
    void cypherOrderingAgreesWithEqualityOnAnUnrepresentableFloatBound() {
      for (final String type : new String[] { "I", "N" }) {
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.v = 16777217.0 RETURN n.id AS id")).as(type + " =").containsExactly(2);
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.v >= 16777217.0 RETURN n.id AS id")).as(type + " >=").containsExactly(2);
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.v > 16777217.0 RETURN n.id AS id")).as(type + " >").isEmpty();
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.v <= 16777217.0 RETURN n.id AS id")).as(type + " <=").containsExactly(1, 2, 3);
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.v < 16777217.0 RETURN n.id AS id")).as(type + " <").containsExactly(1, 3);
      }
    }

    @Test
    void aValueIsNeverEqualToABoundAndBelowIt() {
      for (final String type : new String[] { "I", "N" }) {
        final List<Integer> equal = sql("SELECT id FROM " + type + " WHERE v = 16777217.0");
        final List<Integer> less = sql("SELECT id FROM " + type + " WHERE v < 16777217.0");
        assertThat(less).as(type).doesNotContainAnyElementsOf(equal);
      }
    }

    @Test
    void exactLongBoundAboveTwoToThe53OnADoubleAgreesBetweenIndexAndScan() {
      for (final String type : new String[] { "I", "N" }) {
        assertThat(sql("SELECT id FROM " + type + " WHERE d = 9007199254740993")).as(type + " =").isEmpty();
        assertThat(sql("SELECT id FROM " + type + " WHERE d < 9007199254740993")).as(type + " <").containsExactly(1, 2, 3);
        assertThat(sql("SELECT id FROM " + type + " WHERE d >= 9007199254740993")).as(type + " >=").isEmpty();
        assertThat(sql("SELECT id FROM " + type + " WHERE d > 9007199254740991")).as(type + " > 2^53-1").containsExactly(2);
      }
    }

    @Test
    void lossyIntegerBoundsInBetweenAndInAgreeBetweenIndexAndScan() {
      for (final String type : new String[] { "I", "N" }) {
        assertThat(sql("SELECT id FROM " + type + " WHERE d IN [9007199254740993]")).as(type + " IN").isEmpty();
        assertThat(sql("SELECT id FROM " + type + " WHERE d IN [9007199254740993, 5]")).as(type + " IN 2").containsExactly(3);
        assertThat(sql("SELECT id FROM " + type + " WHERE v IN [16777217]")).as(type + " FLOAT IN").isEmpty();
      }
      // BETWEEN: the indexed type answers as the unindexed one does
      assertThat(sql("SELECT id FROM I WHERE d BETWEEN 9007199254740993 AND 9007199254740995"))
          .isEqualTo(sql("SELECT id FROM N WHERE d BETWEEN 9007199254740993 AND 9007199254740995"));
      assertThat(sql("SELECT id FROM I WHERE v BETWEEN 16777217 AND 16777219"))
          .isEqualTo(sql("SELECT id FROM N WHERE v BETWEEN 16777217 AND 16777219"));
    }

    @Test
    void cypherIntegerBoundAboveTwoToThe53OnADoubleAgreesBetweenIndexAndScan() {
      for (final String type : new String[] { "I", "N" }) {
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.d = 9007199254740993 RETURN n.id AS id")).as(type + " =").isEmpty();
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.d < 9007199254740993 RETURN n.id AS id")).as(type + " <").containsExactly(1, 2, 3);
      }
    }

    @Test
    void aTinyBoundNarrowingToTheStoredFloatIsNeitherAboveNorBelowIt() {
      database.transaction(() -> database.command("sql", "INSERT INTO N SET id = 4, v = 0.0, d = 0.0"));
      assertThat(sql("SELECT id FROM N WHERE v = 1e-50")).containsExactly(4);
      assertThat(sql("SELECT id FROM N WHERE v < 1e-50 AND id = 4")).isEmpty();
      assertThat(sql("SELECT id FROM N WHERE v >= 1e-50 AND id = 4")).containsExactly(4);
    }

    private List<Integer> sql(final String statement) {
      return ids(database.query("sql", statement + " ORDER BY id"));
    }

    private List<Integer> cypher(final String statement) {
      return ids(database.query("opencypher", statement + " ORDER BY id", Map.of()));
    }

    private static List<Integer> ids(final ResultSet rs) {
      final List<Integer> ids = new ArrayList<>();
      try (rs) {
        while (rs.hasNext())
          ids.add(rs.next().<Number>getProperty("id").intValue());
      }
      return ids;
    }
  }

  /**
   * Regression for #8920: a stored -0.0 equals 0.0 on every path.
   * <p>
   * Issue #8920: a stored -0.0 is equal to 0.0 (IEEE 754, openCypher), whichever path answers: SQL or Cypher, `=` or `IN`, an
   * index seek or a scan. The `IN` / `NOT IN` pair must partition the type.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SignedZero extends TestHelper {

    @Override
    public void beginTest() {
      database.transaction(() -> {
        for (final String type : new String[] { "I", "N", "H" }) {
          database.command("sql", "CREATE VERTEX TYPE " + type);
          database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
          database.command("sql", "CREATE PROPERTY " + type + ".v DOUBLE");
        }
        database.command("sql", "CREATE INDEX ON I (v) NOTUNIQUE");
        database.command("sql", "CREATE INDEX ON H (v) NOTUNIQUE_HASH");
        for (final String type : new String[] { "I", "N", "H" }) {
          database.command("sql", "INSERT INTO " + type + " SET id = 1, v = 0.0");
          database.command("sql", "INSERT INTO " + type + " SET id = 2, v = 1.5");
          database.command("sql", "INSERT INTO " + type + " SET id = 3, v = ?", -0.0d);
          database.command("sql", "INSERT INTO " + type + " SET id = 4, v = 0.1");
        }
      });
    }

    @Test
    void sqlEqualityAgreesBetweenIndexAndScan() {
      for (final String type : new String[] { "I", "N", "H" }) {
        assertThat(sql("SELECT id FROM " + type + " WHERE v = 0.0")).as(type + " v = 0.0").containsExactly(1, 3);
        assertThat(sql("SELECT id FROM " + type + " WHERE v = -0.0")).as(type + " v = -0.0").containsExactly(1, 3);
        assertThat(sql("SELECT id FROM " + type + " WHERE v <> 0.0")).as(type + " v <> 0.0").containsExactly(2, 4);
      }
    }

    @Test
    void sqlInAndNotInPartitionTheType() {
      for (final String type : new String[] { "I", "N", "H" }) {
        assertThat(sql("SELECT id FROM " + type + " WHERE v IN [0.0]")).as(type + " IN").containsExactly(1, 3);
        assertThat(sql("SELECT id FROM " + type + " WHERE v IN [-0.0]")).as(type + " IN -0.0").containsExactly(1, 3);
        assertThat(sql("SELECT id FROM " + type + " WHERE v NOT IN [0.0]")).as(type + " NOT IN").containsExactly(2, 4);
        assertThat(sql("SELECT id FROM " + type + " WHERE v IN [0.0, 'x']")).as(type + " IN mixed").containsExactly(1, 3);
        assertThat(sqlParam("SELECT id FROM " + type + " WHERE v IN ?", List.of(0.0d))).as(type + " IN ?").containsExactly(1, 3);
      }
    }

    @Test
    void sqlOrderingTreatsTheZerosAsOneValue() {
      for (final String type : new String[] { "I", "N" }) {
        assertThat(sql("SELECT id FROM " + type + " WHERE v < 0.0")).as(type + " v < 0.0").isEmpty();
        assertThat(sql("SELECT id FROM " + type + " WHERE v <= 0.0")).as(type + " v <= 0.0").containsExactly(1, 3);
        assertThat(sql("SELECT id FROM " + type + " WHERE v >= 0.0")).as(type + " v >= 0.0").containsExactly(1, 2, 3, 4);
        assertThat(sql("SELECT id FROM " + type + " WHERE v > -0.0")).as(type + " v > -0.0").containsExactly(2, 4);
      }
    }

    @Test
    void cypherEqualityAgreesBetweenIndexAndScan() {
      for (final String type : new String[] { "I", "N", "H" }) {
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.v = 0.0 RETURN n.id AS id")).as(type + " = 0.0").containsExactly(1, 3);
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.v = -0.0 RETURN n.id AS id")).as(type + " = -0.0").containsExactly(1, 3);
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.v IN [0.0] RETURN n.id AS id")).as(type + " IN").containsExactly(1, 3);
        assertThat(cypher("MATCH (n:" + type + ") WHERE NOT n.v IN [0.0] RETURN n.id AS id")).as(type + " NOT IN").containsExactly(2, 4);
        assertThat(cypher("MATCH (n:" + type + " {v: 0.0}) RETURN n.id AS id")).as(type + " {v: 0.0}").containsExactly(1, 3);
      }
    }

    @Test
    void uniqueIndexSeesTheCollision() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE U");
        database.command("sql", "CREATE PROPERTY U.v DOUBLE");
        database.command("sql", "CREATE INDEX ON U (v) UNIQUE");
        database.command("sql", "INSERT INTO U SET v = 0.0");
      });
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO U SET v = ?", -0.0d)))
          .isInstanceOf(DuplicatedKeyException.class);
    }

    @Test
    void indexKeyEqualityTreatsTheZerosAsOneKey() {
      assertThat(IndexKeyEquality.sameTuple(new Object[] { 0.0d, "a" }, new Object[] { -0.0d, "a" })).isTrue();
      assertThat(IndexKeyEquality.sameTuple(new Object[] { 0.0f }, new Object[] { -0.0f })).isTrue();
      assertThat(IndexKeyEquality.sameTuple(new Object[] { 0.0d }, new Object[] { 1.0d })).isFalse();
      assertThat(IndexKeyEquality.hashTuple(new Object[] { 0.0d, "a" })).isEqualTo(IndexKeyEquality.hashTuple(new Object[] { -0.0d, "a" }));
      assertThat(IndexKeyEquality.hashTuple(new Object[] { 0.0f })).isEqualTo(IndexKeyEquality.hashTuple(new Object[] { -0.0f }));
      assertThat(IndexKeyEquality.hashTuple(new Object[] { new byte[] { 1, 2 } }))
          .isEqualTo(IndexKeyEquality.hashTuple(new Object[] { new byte[] { 1, 2 } }));
      assertThat(IndexKeyEquality.hashTuple(new Object[] { new byte[] { 1, 2 } })).isEqualTo(Arrays.deepHashCode(new Object[] { new byte[] { 1, 2 } }));
    }

    @Test
    void zerosStayEqualAfterTheIndexIsCompacted() throws Exception {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE C");
        database.command("sql", "CREATE PROPERTY C.id INTEGER");
        database.command("sql", "CREATE PROPERTY C.v DOUBLE");
        database.command("sql", "CREATE INDEX ON C (v) NOTUNIQUE");
        database.command("sql", "INSERT INTO C SET id = 1, v = 0.0");
        database.command("sql", "INSERT INTO C SET id = 2, v = ?", -0.0d);
      });
      for (final TypeIndex index : database.getSchema().getType("C").getAllIndexes(false))
        index.compact();
      assertThat(sql("SELECT id FROM C WHERE v = 0.0")).containsExactly(1, 2);
      assertThat(sql("SELECT id FROM C WHERE v = -0.0")).containsExactly(1, 2);
    }

    @Test
    void legacyZeroSpellingOfAKeyIsDerivedForTheBloomFilterProbe() {
      final Object[] negated = BinaryComparator.withNegativeZeros(new Object[] { 0.0d, "a", 0.0f, 1.5d });
      assertThat(negated).hasSize(4);
      assertThat(Double.doubleToRawLongBits((Double) negated[0])).isEqualTo(Double.doubleToRawLongBits(-0.0d));
      assertThat(Float.floatToRawIntBits((Float) negated[2])).isEqualTo(Float.floatToRawIntBits(-0.0f));
      assertThat(negated[1]).isEqualTo("a");
      assertThat(negated[3]).isEqualTo(1.5d);
      assertThat(BinaryComparator.withNegativeZeros(new Object[] { 1.5d, "a" })).isNull();
    }

    @Test
    void floatPropertyAndCompositeKeysTreatTheZerosAsOne() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE F");
        database.command("sql", "CREATE PROPERTY F.id INTEGER");
        database.command("sql", "CREATE PROPERTY F.g STRING");
        database.command("sql", "CREATE PROPERTY F.v FLOAT");
        database.command("sql", "CREATE INDEX ON F (g, v) NOTUNIQUE");
        database.command("sql", "CREATE VERTEX TYPE FH");
        database.command("sql", "CREATE PROPERTY FH.id INTEGER");
        database.command("sql", "CREATE PROPERTY FH.v FLOAT");
        database.command("sql", "CREATE INDEX ON FH (v) NOTUNIQUE_HASH");
        database.command("sql", "INSERT INTO F SET id = 1, g = 'a', v = 0.0");
        database.command("sql", "INSERT INTO F SET id = 2, g = 'a', v = ?", -0.0f);
        database.command("sql", "INSERT INTO FH SET id = 1, v = 0.0");
        database.command("sql", "INSERT INTO FH SET id = 2, v = ?", -0.0f);
      });
      assertThat(sql("SELECT id FROM F WHERE g = 'a' AND v = 0.0")).containsExactly(1, 2);
      assertThat(sql("SELECT id FROM F WHERE g = 'a' AND v = ?", -0.0f)).containsExactly(1, 2);
      assertThat(sql("SELECT id FROM FH WHERE v = 0.0")).containsExactly(1, 2);
      assertThat(sql("SELECT id FROM FH WHERE v IN [-0.0]")).containsExactly(1, 2);
      assertThat(sql("SELECT id FROM FH WHERE v NOT IN [-0.0]")).isEmpty();
    }

    private List<Integer> sql(final String statement, final Object... params) {
      return ids(database.query("sql", statement + " ORDER BY id", params));
    }

    private List<Integer> sqlParam(final String statement, final Object param) {
      return ids(database.query("sql", statement + " ORDER BY id", param));
    }

    private List<Integer> cypher(final String statement) {
      return ids(database.query("opencypher", statement + " ORDER BY id", Map.of()));
    }

    private static List<Integer> ids(final ResultSet rs) {
      final List<Integer> ids = new ArrayList<>();
      try (rs) {
        while (rs.hasNext())
          ids.add(rs.next().<Number>getProperty("id").intValue());
      }
      return ids;
    }
  }

  /**
   * Regression for #9021: an index on an integral property answers inexact bounds like the scan.
   * <p>
   * Issue #9021: an index on an integral property (BYTE, SHORT, INTEGER, LONG) converted a bound to its key type, truncating a
   * fraction and clamping a value past the range, so it answered for another bound than the scan: {@code i = 12.5} found 12,
   * {@code i < 12.5} lost it, and {@code l = 1e19} found {@code Long.MAX_VALUE}. Every query runs on the indexed type {@code I}
   * and on the identical unindexed type {@code S}, and the two must agree.
   */
  @Nested
  class IntegralIndexInexactBound extends TestHelper {

    private static final String[] OPERATORS = { "=", "<", "<=", ">", ">=" };

    @Override
    public void beginTest() {
      database.transaction(() -> {
        for (final String type : new String[] { "I", "S" }) {
          database.command("sql", "CREATE VERTEX TYPE " + type);
          database.command("sql", "CREATE PROPERTY " + type + ".i INTEGER");
          database.command("sql", "CREATE PROPERTY " + type + ".l LONG");
          database.command("sql", "CREATE PROPERTY " + type + ".s SHORT");
          database.command("sql", "CREATE PROPERTY " + type + ".b BYTE");
          database.command("sql", "CREATE PROPERTY " + type + ".c INTEGER");
          database.command("sql", "CREATE PROPERTY " + type + ".d INTEGER");
          database.command("sql", "CREATE PROPERTY " + type + ".u INTEGER");
          database.command("sql", "CREATE PROPERTY " + type + ".h INTEGER");
        }
        database.command("sql", "CREATE INDEX ON I (i) NOTUNIQUE");
        database.command("sql", "CREATE INDEX ON I (l) NOTUNIQUE");
        database.command("sql", "CREATE INDEX ON I (s) NOTUNIQUE");
        database.command("sql", "CREATE INDEX ON I (b) NOTUNIQUE");
        database.command("sql", "CREATE INDEX ON I (c, d) NOTUNIQUE");
        // an equality on a UNIQUE index, and any bound on a HASH index, take other lookup paths than a NOTUNIQUE LSM range
        database.command("sql", "CREATE INDEX ON I (u) UNIQUE");
        database.command("sql", "CREATE INDEX ON I (h) UNIQUE_HASH");
        for (final String type : new String[] { "I", "S" })
          for (int k = 11; k <= 13; k++)
            database.newVertex(type)
                .set("i", k, "l", Long.MAX_VALUE - 13 + k, "s", (short) k, "b", (byte) k, "c", k % 2, "d", k, "u", k, "h", k).save();
      });
    }

    @Test
    void sqlLinesOfTheIssue() {
      for (final String type : new String[] { "I", "S" }) {
        assertThat(sql("SELECT i FROM " + type + " WHERE i = 12.5")).as(type).isEmpty();
        assertThat(sql("SELECT i FROM " + type + " WHERE i = ?", 12.5d)).as(type).isEmpty();
        assertThat(sql("SELECT i FROM " + type + " WHERE i >= 12.5")).as(type).containsExactly(13);
        assertThat(sql("SELECT i FROM " + type + " WHERE i < 12.5")).as(type).containsExactly(11, 12);
        assertThat(sql("SELECT i FROM " + type + " WHERE i < ?", 12.5d)).as(type).containsExactly(11, 12);
        assertThat(sql("SELECT i FROM " + type + " WHERE l = ?", 1e19d)).as(type).isEmpty();
        assertThat(sql("SELECT i FROM " + type + " WHERE l = ?", new BigInteger("18446744073709551615"))).as(type).isEmpty();
        assertThat(sql("SELECT i FROM " + type + " WHERE l > ?", 1e19d)).as(type).isEmpty();
        assertThat(sql("SELECT i FROM " + type + " WHERE l < ?", 1e19d)).as(type).containsExactly(11, 12, 13);
        // the scan of BETWEEN converted its bounds to the value's class, so 11 was between 11.5 and 12.5
        assertThat(sql("SELECT i FROM " + type + " WHERE i BETWEEN 11.5 AND 12.5")).as(type).containsExactly(12);
        assertThat(sql("SELECT i FROM " + type + " WHERE i BETWEEN ? AND 13", -3_000_000_000L)).as(type).containsExactly(11, 12, 13);
      }
    }

    @Test
    void fluentSelectIndexAgreesWithTheScan() {
      for (final String type : new String[] { "I", "S" }) {
        assertThat(fluent(type, SelectOperator.lt, 12.5d)).as(type + " <").containsExactly(11, 12);
        assertThat(fluent(type, SelectOperator.le, 12.5d)).as(type + " <=").containsExactly(11, 12);
        assertThat(fluent(type, SelectOperator.gt, 11.5d)).as(type + " >").containsExactly(12, 13);
        assertThat(fluent(type, SelectOperator.ge, 11.5d)).as(type + " >=").containsExactly(12, 13);
        assertThat(fluent(type, SelectOperator.eq, 12.5d)).as(type + " =").isEmpty();
        assertThat(fluent(type, SelectOperator.lt, 3_000_000_000L)).as(type + " < 3e9").containsExactly(11, 12, 13);
        assertThat(fluent(type, SelectOperator.gt, 3_000_000_000L)).as(type + " > 3e9").isEmpty();
        assertThat(fluent(type, SelectOperator.between, 11.5d, 12.5d)).as(type + " between").containsExactly(12);
        assertThat(fluent(type, SelectOperator.between, 12.2d, 12.7d)).as(type + " empty between").isEmpty();
      }
    }

    @Test
    void fluentSelectIndexAgreesWithTheScanOnEveryIntegralKeyType() {
      final Object[] bounds = { 12.5d, -12.5d, 3_000_000_000L, -3_000_000_000L, 1e19d, -1e19d, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.NaN };
      final SelectOperator[] operators = { SelectOperator.eq, SelectOperator.lt, SelectOperator.le, SelectOperator.gt, SelectOperator.ge };
      for (final String property : new String[] { "i", "l", "s", "b", "u", "h" })
        for (final SelectOperator operator : operators)
          for (final Object bound : bounds) {
            if (property.equals("h") && operator != SelectOperator.eq)
              // a HASH index answers no range: the fluent API scans, there is no index side to compare
              continue;
            assertThat(fluent("I", property, operator, bound)).as(property + " " + operator + " " + describe(bound))
                .isEqualTo(fluent("S", property, operator, bound));
          }
      for (final String property : new String[] { "i", "l", "s", "b", "u" })
        for (final Object[] range : new Object[][] { { 11.5d, 12.5d }, { 12.2d, 12.7d }, { -1e19d, 1e19d }, { 1e19d, -1e19d },
            { Double.NEGATIVE_INFINITY, Double.NaN } })
          assertThat(fluent("I", property, SelectOperator.between, range)).as(property + " between " + range[0] + " and " + range[1])
              .isEqualTo(fluent("S", property, SelectOperator.between, range));
    }

    private List<Object> fluent(final String type, final SelectOperator operator, final Object... value) {
      return fluent(type, "i", operator, value);
    }

    private List<Object> fluent(final String type, final String property, final SelectOperator operator, final Object... value) {
      final SelectWhereOperatorBlock where = database.select().fromType(type).where().property(property);
      final SelectWhereAfterBlock after = switch (operator) {
        case lt -> where.lt().value(value[0]);
        case le -> where.le().value(value[0]);
        case gt -> where.gt().value(value[0]);
        case ge -> where.ge().value(value[0]);
        case eq -> where.eq().value(value[0]);
        default -> where.between().values(value[0], value[1]);
      };
      final List<Object> rows = new ArrayList<>();
      after.vertices().forEachRemaining(v -> rows.add(v.get("i")));
      rows.sort(null);
      return rows;
    }

    @Test
    void cypherLinesOfTheIssue() {
      for (final String type : new String[] { "I", "S" }) {
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.i = 12.5 RETURN n.i AS i ORDER BY i", Map.of())).as(type).isEmpty();
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.i >= 12.5 RETURN n.i AS i ORDER BY i", Map.of())).as(type).containsExactly(13);
        assertThat(cypher("MATCH (n:" + type + ") WHERE n.i < 12.5 RETURN n.i AS i ORDER BY i", Map.of())).as(type).containsExactly(11, 12);
      }
    }

    @Test
    void theIndexIsUsedForTheBoundsUnderTest() {
      // the agreement below proves nothing if the indexed type is answered by a scan as well
      for (final String where : new String[] { "i = 12.5", "i < 12.5", "i >= ?", "l = ?", "s > 12.5", "b <= 12.5", "c = 1 AND d < 12.5",
          "u = 12.5", "h = 12.5" })
        try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT i FROM I WHERE " + where,
            where.contains("?") ? new Object[] { 12.5d } : new Object[0])) {
          assertThat(rs.next().<String>getProperty("executionPlanAsString")).as(where).contains("FETCH FROM INDEX");
        }
    }

    @Test
    void sqlIndexAgreesWithTheScanForEveryOperatorAndInexactBound() {
      final Object[] bounds = { 12.5d, 12.5f, -12.5d, 10.5d, 13.5d, 12.0d, new BigDecimal("12.5"), 3_000_000_000L, -3_000_000_000L, 1e19d,
          -1e19d, new BigInteger("18446744073709551615"), Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN, 40_000, 300 };
      for (final String field : new String[] { "i", "l", "s", "b", "u", "h" })
        for (final String op : OPERATORS)
          for (final Object bound : bounds) {
            final String where = field + " " + op + " ?";
            assertThat(sql("SELECT i FROM I WHERE " + where, bound)).as(where + " with " + describe(bound))
                .isEqualTo(sql("SELECT i FROM S WHERE " + where, bound));
          }
    }

    @Test
    void sqlLiteralBoundsAgreeWithTheScan() {
      for (final String op : OPERATORS)
        for (final String literal : new String[] { "12.5", "-12.5", "13.5", "10.5", "12.0", "3000000000", "-3000000000" })
          for (final String field : new String[] { "i", "s", "b", "u", "h" }) {
            final String where = field + " " + op + " " + literal;
            assertThat(sql("SELECT i FROM I WHERE " + where)).as(where).isEqualTo(sql("SELECT i FROM S WHERE " + where));
          }
    }

    @Test
    void sqlTwoSidedRangesBetweenAndInAgreeWithTheScan() {
      final String[] wheres = { "i > 11.5 AND i < 12.5", "i >= 11.5 AND i <= 13.5", "i > ? AND i <= 13", "i BETWEEN 11.5 AND 12.5",
          "i BETWEEN ? AND 13", "i IN [12.5, 13]", "i IN [12.5]", "i IN ?", "l BETWEEN ? AND 9223372036854775807",
          "c = 1 AND d < 12.5", "c = 1 AND d >= 12.5", "c = 0.5 AND d > 0", "i > 12.2 AND i < 12.7", "i BETWEEN 12.2 AND 12.7",
          "i BETWEEN 13 AND 12", "c = 1 AND d = 12.5", "c = 1 AND d > 11.5 AND d < 13.5", "c = 1 AND d BETWEEN 10.5 AND 12.5",
          "u IN [12.5, 13]", "h IN [12.5, 13]", "u BETWEEN 11.5 AND 12.5" };
      for (final String where : wheres) {
        final Object parameter = where.contains("IN ?") ? List.of(12.5d, 11) : where.startsWith("l ") ? 1e19d : 11.5d;
        // one value for the one placeholder, none for a statement without (issue #10721)
        final Object[] args = where.contains("?") ? new Object[] { parameter } : new Object[0];
        assertThat(sql("SELECT i FROM I WHERE " + where, args)).as(where).isEqualTo(sql("SELECT i FROM S WHERE " + where, args));
      }
    }

    @Test
    void sqlBetweenWithNaNAndInfiniteBoundsAgreesWithTheScan() {
      final double[] bounds = { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 12.5d };
      for (final String field : new String[] { "i", "l", "s", "b", "u" })
        for (final double from : bounds)
          for (final double to : bounds) {
            final String where = field + " BETWEEN ? AND ?";
            assertThat(sql("SELECT i FROM I WHERE " + where, from, to)).as(where + " with " + from + ", " + to)
                .isEqualTo(sql("SELECT i FROM S WHERE " + where, from, to));
          }
    }

    @Test
    void sqlDescendingOrderOverAnInexactRange() {
      for (final String type : new String[] { "I", "S" }) {
        assertThat(sql("SELECT i FROM " + type + " WHERE i < 12.5 ORDER BY i DESC")).as(type).containsExactly(12, 11);
        assertThat(sql("SELECT i FROM " + type + " WHERE i > 11.5 ORDER BY i DESC")).as(type).containsExactly(13, 12);
        // two mapped bounds: the descending seek swaps them, with their inclusion flags
        assertThat(sql("SELECT i FROM " + type + " WHERE i > 10.5 AND i < 12.5 ORDER BY i DESC")).as(type).containsExactly(12, 11);
        assertThat(sql("SELECT i FROM " + type + " WHERE i BETWEEN 11.5 AND 13.5 ORDER BY i DESC")).as(type).containsExactly(13, 12);
        assertThat(sql("SELECT i FROM " + type + " WHERE c = 1 AND d > 10.5 AND d < 13.5 ORDER BY i DESC")).as(type)
            .containsExactly(13, 11);
      }
    }

    @Test
    void cypherIndexAgreesWithTheScanForEveryOperatorAndInexactBound() {
      final Object[] bounds = { 12.5d, -12.5d, 10.5d, 13.5d, 12.0d, 3_000_000_000L, -3_000_000_000L, 1e19d, -1e19d, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY };
      for (final String field : new String[] { "i", "l", "s", "b" })
        for (final String op : OPERATORS)
          for (final Object bound : bounds) {
            final String where = "n." + field + " " + op + " $p";
            assertThat(cypher("MATCH (n:I) WHERE " + where + " RETURN n.i AS i ORDER BY i", Map.of("p", bound)))
                .as(where + " with " + describe(bound))
                .isEqualTo(cypher("MATCH (n:S) WHERE " + where + " RETURN n.i AS i ORDER BY i", Map.of("p", bound)));
          }
      for (final String where : new String[] { "n.i > 12.2 AND n.i < 12.7", "n.i > 11.5 AND n.i < 12.5", "n.i >= 10.5 AND n.i <= 12.5",
          "n.i < 12.5", "n.i <= 12.5", "n.i > 12.5" })
        assertThat(cypher("MATCH (n:I) WHERE " + where + " RETURN n.i AS i ORDER BY i DESC", Map.of())).as(where)
            .isEqualTo(cypher("MATCH (n:S) WHERE " + where + " RETURN n.i AS i ORDER BY i DESC", Map.of()));
    }

    @Test
    void boundMapping() {
      final byte INT = BinaryTypes.TYPE_INT;
      assertThat(IntegralKeyBound.isInexact(INT, 12)).isFalse();
      assertThat(IntegralKeyBound.isInexact(INT, 12.0d)).isFalse();
      assertThat(IntegralKeyBound.isInexact(INT, new BigDecimal("12.000"))).isFalse();
      assertThat(IntegralKeyBound.isInexact(INT, 12.5d)).isTrue();
      assertThat(IntegralKeyBound.isInexact(INT, 3_000_000_000L)).isTrue();
      assertThat(IntegralKeyBound.isInexact(INT, Double.NaN)).isTrue();
      assertThat(IntegralKeyBound.isInexact(BinaryTypes.TYPE_DOUBLE, 12.5d)).isFalse();
      assertThat(IntegralKeyBound.isInexact(INT, "12.5")).isFalse();
      // the primitive fast path for a double or float below 2^53, and the BigDecimal path above it
      assertThat(IntegralKeyBound.isInexact(INT, 12.0f)).isFalse();
      assertThat(IntegralKeyBound.isInexact(INT, 12.5f)).isTrue();
      assertThat(IntegralKeyBound.isInexact(INT, -0.0d)).isFalse();
      assertThat(IntegralKeyBound.isInexact(INT, (double) Integer.MAX_VALUE)).isFalse();
      assertThat(IntegralKeyBound.isInexact(INT, Integer.MAX_VALUE + 1.0d)).isTrue();
      assertThat(IntegralKeyBound.isInexact(INT, Integer.MIN_VALUE - 1.0d)).isTrue();
      assertThat(IntegralKeyBound.isInexact(BinaryTypes.TYPE_LONG, 9007199254740991d)).isFalse();
      assertThat(IntegralKeyBound.isInexact(BinaryTypes.TYPE_LONG, 9007199254740992d)).isFalse();
      assertThat(IntegralKeyBound.isInexact(BinaryTypes.TYPE_LONG, 0x1p63)).isTrue();
      assertThat(IntegralKeyBound.isInexact(BinaryTypes.TYPE_BYTE, 128.0d)).isTrue();

      assertThat(IntegralKeyBound.ceiling(INT, 12.5d)).isEqualTo(13);
      assertThat(IntegralKeyBound.floor(INT, 12.5d)).isEqualTo(12);
      assertThat(IntegralKeyBound.ceiling(INT, -12.5d)).isEqualTo(-12);
      assertThat(IntegralKeyBound.floor(INT, -12.5d)).isEqualTo(-13);
      assertThat(IntegralKeyBound.ceiling(INT, 3_000_000_000L)).isNull();
      assertThat(IntegralKeyBound.floor(INT, 3_000_000_000L)).isEqualTo(Integer.MAX_VALUE);
      assertThat(IntegralKeyBound.ceiling(INT, -3_000_000_000L)).isEqualTo(Integer.MIN_VALUE);
      assertThat(IntegralKeyBound.floor(INT, -3_000_000_000L)).isNull();
      assertThat(IntegralKeyBound.ceiling(BinaryTypes.TYPE_LONG, 1e19d)).isNull();
      assertThat(IntegralKeyBound.floor(BinaryTypes.TYPE_LONG, 1e19d)).isEqualTo(Long.MAX_VALUE);
      assertThat(IntegralKeyBound.floor(BinaryTypes.TYPE_SHORT, 12.5d)).isEqualTo((short) 12);
      assertThat(IntegralKeyBound.ceiling(BinaryTypes.TYPE_BYTE, 12.5d)).isEqualTo((byte) 13);
      // NaN orders above every number, as the scan compares it
      assertThat(IntegralKeyBound.ceiling(INT, Double.NaN)).isNull();
      assertThat(IntegralKeyBound.floor(INT, Double.NaN)).isEqualTo(Integer.MAX_VALUE);
    }

    private static String describe(final Object bound) {
      return bound.getClass().getSimpleName() + " " + bound;
    }

    private List<Object> sql(final String query, final Object... args) {
      final List<Object> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query + (query.contains("ORDER BY") ? "" : " ORDER BY i"), args)) {
        rs.forEachRemaining(r -> rows.add(r.getProperty("i")));
      }
      return rows;
    }

    private List<Object> cypher(final String query, final Map<String, Object> params) {
      final List<Object> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        rs.forEachRemaining(r -> rows.add(r.getProperty("i")));
      }
      return rows;
    }
  }
}
