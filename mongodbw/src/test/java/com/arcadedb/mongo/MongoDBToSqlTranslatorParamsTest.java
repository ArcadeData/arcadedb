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
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
}
