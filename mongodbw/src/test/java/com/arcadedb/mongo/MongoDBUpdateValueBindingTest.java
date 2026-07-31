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

/**
 * An update takes its values off the wire and used to spell them into the statement as a JSON literal, so their safety rested
 * on {@code JSONObject} escaping being correct. These tests pin the replacement contract: the payload of {@code MERGE} and of
 * {@code CONTENT} is a bound parameter and never appears in the statement text, which is what makes injection unreachable by
 * construction rather than by remembering to escape at each call site.
 * <p>
 * Field names are a deliberate exception and stay quoted: SQL cannot bind a property name.
 * <p>
 * {@code appendUpdateOperations} is package-private static, so a test in the same package can read the generated SQL directly
 * instead of inferring it from a round trip.
 */
class MongoDBUpdateValueBindingTest {

  @Test
  void aSetOperandIsBoundRatherThanSpelledAsJson() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("$set", new Document("note", "v1")));

    assertThat(sql.toString()).isEqualTo(" MERGE :p0");
    assertThat(params).containsOnlyKeys("p0");
    assertThat(params.get("p0")).isEqualTo(Map.of("note", "v1"));
  }

  @Test
  void aReplacementDocumentIsBoundRatherThanSpelledAsJson() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    // no $-prefixed key means a full replacement, which used to go out as CONTENT <json>
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("name", "target").append("note", "v1"));

    assertThat(sql.toString()).isEqualTo(" CONTENT :p0");
    assertThat(params).containsOnlyKeys("p0");
    assertThat(params.get("p0")).isEqualTo(Map.of("name", "target", "note", "v1"));
  }

  @Test
  void aQuoteBearingSetValueNeverAppearsInTheStatementText() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final String awkward = "v1\", \"injected\": \"yes";
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("$set", new Document("note", awkward)));

    assertThat(sql.toString()).isEqualTo(" MERGE :p0");
    assertThat(sql.toString()).doesNotContain("injected").doesNotContain("\"");
    assertThat(((Map<?, ?>) params.get("p0")).get("note")).isEqualTo(awkward);
  }

  @Test
  void aQuoteBearingReplacementValueNeverAppearsInTheStatementText() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final String awkward = "replaced' with \"quotes\" and a C:\\path";
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("note", awkward));

    assertThat(sql.toString()).isEqualTo(" CONTENT :p0");
    assertThat(sql.toString()).doesNotContain("'").doesNotContain("\\");
    assertThat(((Map<?, ?>) params.get("p0")).get("note")).isEqualTo(awkward);
  }

  @Test
  void aNestedDocumentIsBoundAsANestedMap() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final Document nested = new Document("city", "Rome").append("zip", 145);
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("$set", new Document("address", nested)));

    assertThat(sql.toString()).isEqualTo(" MERGE :p0");
    final Object address = ((Map<?, ?>) params.get("p0")).get("address");
    assertThat(address).isInstanceOf(Map.class);
    assertThat(((Map<?, ?>) address).get("city")).isEqualTo("Rome");
    assertThat(((Map<?, ?>) address).get("zip")).isEqualTo(145);
  }

  @Test
  void aListValueIsBoundAsAList() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final List<Object> tags = List.of("a", new Document("nested", "b"));
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("$set", new Document("tags", tags)));

    final Object bound = ((Map<?, ?>) params.get("p0")).get("tags");
    assertThat(bound).isInstanceOf(List.class);
    assertThat((List<?>) bound).hasSize(2);
    assertThat(((List<?>) bound).getFirst()).isEqualTo("a");
    // the recursion has to reach documents nested inside a list, not just documents nested inside a document
    assertThat(((List<?>) bound).get(1)).isInstanceOf(Map.class);
    assertThat(((Map<?, ?>) ((List<?>) bound).get(1)).get("nested")).isEqualTo("b");
  }

  @Test
  void anObjectIdIsBoundAsItsHexString() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final ObjectId id = new ObjectId();
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("$set", new Document("ref", id)));

    // an ObjectId has no SQL type: the wrapper stores it as hex text, and that conversion must survive the binding
    final Object bound = ((Map<?, ?>) params.get("p0")).get("ref");
    assertThat(bound).isInstanceOf(String.class);
    assertThat((String) bound).hasSize(id.toByteArray().length * 2).matches("[0-9a-f]+");
  }

  @Test
  void aDateIsBoundAsADateInsteadOfBeingSerialised() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final Date when = new Date();
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("$set", new Document("when", when)));

    // going through JSONObject reshaped a Date before it reached the record; binding hands over the original object
    assertThat(((Map<?, ?>) params.get("p0")).get("when")).isEqualTo(when);
  }

  @Test
  void updateValuesAndFilterValuesShareOneParameterNumbering() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    // executeUpdate appends the operations first and the WHERE clause second, against the same map: the two halves must
    // not collide on a placeholder name
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("$set", new Document("touched", "yes")));
    sql.append(" WHERE ");
    MongoDBToSqlTranslator.buildExpression(sql, params, new Document("name", "target"));

    assertThat(sql.toString()).isEqualTo(" MERGE :p0 WHERE `name` = :p1");
    assertThat(params).hasSize(2);
    assertThat(params.get("p0")).isEqualTo(Map.of("touched", "yes"));
    assertThat(params.get("p1")).isEqualTo("target");
  }

  @Test
  void unsetStillSpellsFieldNamesAsQuotedIdentifiers() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final Document unset = new Document("a", "").append("b.c", "");
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("$unset", unset));

    // SQL cannot bind a property name, so this half stays escaped - the #5575 contract must survive the change
    assertThat(sql.toString()).isEqualTo(" REMOVE `a`, `b`.`c`");
    assertThat(params).isEmpty();
  }

  @Test
  void incStillBindsItsOperandAndQuotesItsFieldName() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("$inc", new Document("count", 3)));

    assertThat(sql.toString()).isEqualTo(" SET `count` += :p0");
    assertThat(params.get("p0")).isEqualTo(3);
  }

  @Test
  void aCombinedSetAndIncUpdateChainsTwoOperationsOverOneParameterMap() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    final Document u = new Document("$set", new Document("note", "v1")).append("$inc", new Document("count", 3));
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, u);

    // the grammar takes updateOperation+, so the two clauses chain; the payload of the first must not swallow the
    // SET keyword that opens the second
    assertThat(sql.toString()).isEqualTo(" MERGE :p0 SET `count` += :p1");
    assertThat(params.get("p0")).isEqualTo(Map.of("note", "v1"));
    assertThat(params.get("p1")).isEqualTo(3);
  }

  @Test
  void aCraftedFieldNameInASetOperandCannotBreakOutOfTheStatement() {
    final StringBuilder sql = new StringBuilder();
    final Map<String, Object> params = new HashMap<>();

    // inside a bound map even the key is data, so it needs no quoting and cannot reach the parser at all
    final String crafted = "x\": 1, \"admin";
    MongoDBDatabaseWrapper.appendUpdateOperations(sql, params, new Document("$set", new Document(crafted, "v1")));

    assertThat(sql.toString()).isEqualTo(" MERGE :p0");
    assertThat(sql.toString()).doesNotContain("admin");
    final Map<?, ?> bound = (Map<?, ?>) params.get("p0");
    assertThat(bound).hasSize(1);
    assertThat(bound.keySet().iterator().next()).isEqualTo(crafted);
  }
}
