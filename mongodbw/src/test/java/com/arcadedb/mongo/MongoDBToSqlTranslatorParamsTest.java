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
package com.arcadedb.mongo;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A MongoDB filter is translated into a SQL statement, so every value taken off the wire used to be spelled into that
 * statement's text. These tests pin the replacement contract: a value is bound as a named parameter and never appears in the
 * text, which is what makes the injection unreachable by construction rather than by remembering to escape at each call site.
 * <p>
 * The builders are {@code protected static}, so a test in the same package can read the generated SQL directly instead of
 * inferring it from a round trip.
 */
class MongoDBToSqlTranslatorParamsTest {

  @Test
  void anEqualityValueIsBoundAndNeverSpelledIntoTheStatement() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("name", "v1"));

    assertThat(sql.toString()).isEqualTo("`name` = :p0");
    assertThat(params).containsOnlyKeys("p0");
    assertThat(params.get("p0")).isEqualTo("v1");
  }

  @Test
  void aQuoteBearingValueStaysOutOfTheStatementText() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    // the classic payload from #5575: inlined, it would close the literal and add an always-true disjunction
    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("name", "v1' OR 'x' = 'x"));

    assertThat(sql.toString()).doesNotContain("OR").doesNotContain("'");
    assertThat(sql.toString()).isEqualTo("`name` = :p0");
    assertThat(params.get("p0")).isEqualTo("v1' OR 'x' = 'x");
  }

  @Test
  void aBackslashBearingValueStaysOutOfTheStatementText() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("path", "C:\\data\\'"));

    assertThat(sql.toString()).isEqualTo("`path` = :p0");
    assertThat(params.get("p0")).isEqualTo("C:\\data\\'");
  }

  @Test
  void everyValueGetsItsOwnPlaceholder() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final Document query = new Document();
    query.put("first", "a");
    query.put("second", "b");
    MongoDBToSqlTranslator.buildExpression(sql, params, query);

    assertThat(sql.toString()).contains(":p0").contains(":p1");
    assertThat(params).hasSize(2).containsValues("a", "b");
  }

  @Test
  void nonStringValuesKeepTheirJavaTypeInsteadOfBeingStringified() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final Date when = new Date();
    final Document query = new Document();
    query.put("count", 42);
    query.put("ratio", 1.0E10d);
    query.put("active", Boolean.TRUE);
    query.put("when", when);
    MongoDBToSqlTranslator.buildExpression(sql, params, query);

    // inlining these went through String.valueOf, which turns a Date into text no SQL parser accepts and a double into
    // scientific notation
    assertThat(params.get("p0")).isEqualTo(42);
    assertThat(params.get("p1")).isEqualTo(1.0E10d);
    assertThat(params.get("p2")).isEqualTo(Boolean.TRUE);
    assertThat(params.get("p3")).isEqualTo(when);
    assertThat(sql.toString()).doesNotContain("E10");
  }

  @Test
  void aNullValueIsBoundRatherThanSpelled() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final Document query = new Document();
    query.put("missing", null);
    MongoDBToSqlTranslator.buildExpression(sql, params, query);

    assertThat(sql.toString()).isEqualTo("`missing` = :p0");
    assertThat(params).containsKey("p0");
    assertThat(params.get("p0")).isNull();
  }

  @Test
  void comparisonOperatorsBindTheirOperand() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("age", new Document("$gte", 18)));

    assertThat(sql.toString()).isEqualTo("(`age` >= :p0)");
    assertThat(params.get("p0")).isEqualTo(18);
  }

  @Test
  void inBindsTheWholeCollectionToASingleParameter() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final List<String> names = List.of("v1", "v2' OR 'x' = 'x");
    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("name", new Document("$in", names)));

    assertThat(sql.toString()).isEqualTo("(`name` IN (:p0))");
    assertThat(params.get("p0")).isEqualTo(names);
  }

  @Test
  void notInBindsTheWholeCollectionToASingleParameter() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final List<String> names = List.of("v1", "v2");
    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("name", new Document("$nin", names)));

    assertThat(sql.toString()).isEqualTo("(`name` NOT IN (:p0))");
    assertThat(params.get("p0")).isEqualTo(names);
  }

  @Test
  void orBranchesEachBindTheirOwnValue() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params,
        new Document("$or", List.of(new Document("name", "v1"), new Document("name", "v2"))));

    assertThat(sql.toString()).isEqualTo("(`name` = :p0 OR `name` = :p1)");
    assertThat(params).hasSize(2);
    assertThat(params.get("p0")).isEqualTo("v1");
    assertThat(params.get("p1")).isEqualTo("v2");
  }

  @Test
  void andBranchesEachBindTheirOwnValue() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params,
        new Document("$and", List.of(new Document("name", "v1"), new Document("age", 18))));

    assertThat(sql.toString()).isEqualTo("(`name` = :p0 AND `age` = :p1)");
    assertThat(params.get("p0")).isEqualTo("v1");
    assertThat(params.get("p1")).isEqualTo(18);
  }

  @Test
  void aDottedPathKeepsPerSegmentQuotingWhileItsValueIsBound() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("address.city", "Rome"));

    // quoting the whole path would turn MongoDB navigation into a single property named "address.city"
    assertThat(sql.toString()).isEqualTo("`address`.`city` = :p0");
    assertThat(params.get("p0")).isEqualTo("Rome");
  }

  @Test
  void aCraftedFieldNameIsStillQuotedAsAnIdentifier() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    // SQL cannot bind a property name, so this half stays escaped - the #5575 contract must survive the change
    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("1 = 1 OR name", "nomatch"));

    assertThat(sql.toString()).isEqualTo("`1 = 1 OR name` = :p0");
  }

  @Test
  void sizeBindsItsOperand() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("tags", new Document("$size", 3)));

    assertThat(sql.toString()).isEqualTo("(`tags`.size() = :p0)");
    assertThat(params.get("p0")).isEqualTo(3);
  }

  /**
   * Regression test for issue #6748 (1): {@code $exists: false} used to emit {@code IS DEFINED} regardless of the
   * boolean operand, so it matched the exact opposite set of documents.
   */
  @Test
  void existsFalseEmitsIsNotDefined() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("middleName", new Document("$exists", false)));

    assertThat(sql.toString()).isEqualTo("(`middleName` IS NOT DEFINED )");
  }

  @Test
  void existsTrueEmitsIsDefined() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("middleName", new Document("$exists", true)));

    assertThat(sql.toString()).isEqualTo("(`middleName` IS DEFINED )");
  }

  /**
   * Regression test for issue #6748 (3): {@code $not} used to recurse into its operand without re-emitting the
   * field it applies to, producing an operator with no left-hand operand (e.g. {@code field NOT > :p0}), which the
   * SQL parser rejects outright.
   */
  @Test
  void notWrapsTheFieldAndOperatorInAValidParenthesizedClause() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("age", new Document("$not", new Document("$gt", 18))));

    assertThat(sql.toString()).isEqualTo("(NOT (`age` > :p0))");
    assertThat(params.get("p0")).isEqualTo(18);
  }

  /**
   * A top-level {@code $not} wrapping a whole {@code {field: {...}}} fragment (as built by the driver's
   * {@code Filters.not(...)}) is a different shape from the field-scoped case above and must keep working: its
   * operand already has its own field, so recursing into it directly (without any extra field prepended) is
   * correct as-is.
   */
  @Test
  void topLevelNotWrapsAWholeFieldExpression() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("$not", new Document("name", new Document("$eq", "Jay2"))));

    assertThat(sql.toString()).isEqualTo(" NOT (`name` = :p0)");
    assertThat(params.get("p0")).isEqualTo("Jay2");
  }

  /**
   * Regression test for issue #6745: an {@link ObjectId} bound as-is compares as its {@code toString()}
   * ({@code "ObjectId[<hex>]"}), which never equals the bare lowercase hex string ArcadeDB stores. Binding must
   * convert it to the same hex form used on the write path.
   */
  @Test
  void anObjectIdValueIsBoundAsItsBareHexString() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("_id", id));

    assertThat(sql.toString()).isEqualTo("`_id` = :p0");
    assertThat(params.get("p0")).isEqualTo("507f1f77bcf86cd799439011");
  }

  /**
   * Follow-up to #6745, flagged by review on PR #6767: {@code buildCollection} binds the whole {@code $in}/{@code
   * $nin} collection as a single parameter without normalizing its elements, so an {@code ObjectId} inside it kept
   * comparing as {@code toString()} instead of the stored hex string - {@code $in}/{@code $nin} on {@code _id}
   * never matched even after the scalar case was fixed.
   */
  @Test
  void inNormalizesEachObjectIdElementToItsHexString() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final ObjectId id1 = new ObjectId("507f1f77bcf86cd799439011");
    final ObjectId id2 = new ObjectId("507f1f77bcf86cd799439012");
    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("_id", new Document("$in", List.of(id1, id2))));

    assertThat(sql.toString()).isEqualTo("(`_id` IN (:p0))");
    assertThat(params.get("p0")).isEqualTo(List.of("507f1f77bcf86cd799439011", "507f1f77bcf86cd799439012"));
  }

  @Test
  void ninNormalizesEachObjectIdElementToItsHexString() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final ObjectId id = new ObjectId("507f1f77bcf86cd799439011");
    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("_id", new Document("$nin", List.of(id))));

    assertThat(sql.toString()).isEqualTo("(`_id` NOT IN (:p0))");
    assertThat(params.get("p0")).isEqualTo(List.of("507f1f77bcf86cd799439011"));
  }

  /**
   * Flagged by review on PR #6767: the field-scoped {@code $not} fix only re-emitted the field once, so a
   * multi-operator operand (a valid Mongo shape, e.g. a range) produced a dangling second comparison with no
   * left-hand side - {@code NOT (`price` > :p0 <  :p1)} rather than an AND-joined pair of full comparisons.
   */
  @Test
  void notWithAMultiOperatorOperandJoinsEachComparisonWithItsOwnField() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final Document range = new Document();
    range.put("$gt", 1);
    range.put("$lt", 5);
    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("price", new Document("$not", range)));

    assertThat(sql.toString()).isEqualTo("(NOT (`price` > :p0 AND `price` < :p1))");
    assertThat(params.get("p0")).isEqualTo(1);
    assertThat(params.get("p1")).isEqualTo(5);
  }

  /**
   * Flagged by review on PR #6767: a nested {@code $not} operand (e.g. {@code {field: {$not: {$not: {$gt: 5}}}}})
   * would fall through to the top-level {@code $not} branch with no field in scope, silently producing invalid SQL
   * ({@code NOT (`field` NOT  > :p0)}). Double negation is not a real Mongo query shape, so this is guarded
   * explicitly rather than left to produce a malformed statement.
   */
  @Test
  void nestedNotIsRejectedRatherThanProducingInvalidSql() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    assertThatThrownBy(() -> MongoDBToSqlTranslator.buildExpression(sql, params,
        new Document("field", new Document("$not", new Document("$not", new Document("$gt", 5))))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Flagged by review on PR #6767: an empty {@code $not} operand (e.g. {@code {field: {$not: {}}}}) would produce
   * {@code NOT ()}, empty parentheses that the SQL parser rejects. Guarded explicitly rather than left to produce
   * a malformed statement.
   */
  @Test
  void emptyNotOperandIsRejectedRatherThanProducingInvalidSql() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    assertThatThrownBy(() -> MongoDBToSqlTranslator.buildExpression(sql, params, new Document("field", new Document("$not", new Document()))))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
