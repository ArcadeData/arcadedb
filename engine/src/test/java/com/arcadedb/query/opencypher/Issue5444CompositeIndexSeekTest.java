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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5444: the Cypher optimizer offered a composite index for a predicate on its leading
 * property, but the seek operator asked the index for a one-element key. The index is keyed by the
 * whole tuple, so the lookup found nothing and the query returned an empty result set, silently.
 * <p>
 * Every case is checked against the SQL engine over the same data, since SQL has always used
 * composite indexes correctly and is the reference for what the rows must be.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5444CompositeIndexSeekTest {
  private static final String DATABASE_PATH = "./target/databases/issue-5444-composite-index-seek";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory(DATABASE_PATH).create();

    final var product = database.getSchema().createVertexType("Product");
    product.createProperty("category", Type.STRING);
    product.createProperty("price", Type.INTEGER);
    product.createProperty("sku", Type.STRING);

    final var area = database.getSchema().createVertexType("Area");
    area.createProperty("id", Type.STRING);
    area.createProperty("zone", Type.STRING);

    database.transaction(() -> {
      newProduct("a", 10, "p1");
      newProduct("a", 20, "p2");
      newProduct("b", 10, "p3");
      for (int i = 0; i < 128; i++)
        newProduct("decoy", 1000 + i, "decoy-" + i);

      database.newVertex("Area").set("id", "country").set("zone", "EU").save();
      database.newVertex("Area").set("id", "region").set("zone", "EU").save();
    });

    database.transaction(() -> {
      // the only index on either type is composite
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Product", "category", "price");
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Area", "id", "zone");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void equalityOnTheLeadingPropertyOfAUniqueCompositeIndex() {
    assertThat(cypher("MATCH (a:Area) WHERE a.id = 'country' RETURN a.id AS value"))
        .containsExactly("country");
    assertSqlParity("MATCH (a:Area) WHERE a.id = 'country' RETURN a.id AS value",
        "SELECT id AS value FROM Area WHERE id = 'country'");
  }

  @Test
  void equalityOnTheFullKeyOfAUniqueCompositeIndex() {
    assertThat(cypher("MATCH (a:Area) WHERE a.id = 'country' AND a.zone = 'EU' RETURN a.id AS value"))
        .containsExactly("country");
    assertSqlParity("MATCH (a:Area) WHERE a.id = 'country' AND a.zone = 'EU' RETURN a.id AS value",
        "SELECT id AS value FROM Area WHERE id = 'country' AND zone = 'EU'");
  }

  @Test
  void equalityOnTheLeadingPropertyOfANonUniqueCompositeIndex() {
    assertThat(cypher("MATCH (p:Product) WHERE p.category = 'a' RETURN p.sku AS value ORDER BY value"))
        .containsExactly("p1", "p2");
    assertSqlParity("MATCH (p:Product) WHERE p.category = 'a' RETURN p.sku AS value ORDER BY value",
        "SELECT sku AS value FROM Product WHERE category = 'a' ORDER BY sku");
  }

  @Test
  void equalityOnTheFullKeyOfANonUniqueCompositeIndex() {
    assertThat(cypher("MATCH (p:Product) WHERE p.category = 'a' AND p.price = 20 RETURN p.sku AS value"))
        .containsExactly("p2");
    assertSqlParity("MATCH (p:Product) WHERE p.category = 'a' AND p.price = 20 RETURN p.sku AS value",
        "SELECT sku AS value FROM Product WHERE category = 'a' AND price = 20");
  }

  @Test
  void equalityOnATrailingPropertyIsNotSeekableButStaysCorrect() {
    assertThat(cypher("MATCH (p:Product) WHERE p.price = 10 RETURN p.sku AS value ORDER BY value"))
        .containsExactly("p1", "p3");
    assertSqlParity("MATCH (p:Product) WHERE p.price = 10 RETURN p.sku AS value ORDER BY value",
        "SELECT sku AS value FROM Product WHERE price = 10 ORDER BY sku");
  }

  @Test
  void parameterizedEqualityOnTheLeadingProperty() {
    assertThat(cypher("MATCH (p:Product) WHERE p.category = $category RETURN p.sku AS value ORDER BY value",
        Map.of("category", "a"))).containsExactly("p1", "p2");
  }

  @Test
  void inListOnTheLeadingPropertyOfACompositeIndex() {
    assertThat(cypher("MATCH (p:Product) WHERE p.category IN ['a', 'b'] RETURN p.sku AS value ORDER BY value"))
        .containsExactly("p1", "p2", "p3");
    assertSqlParity("MATCH (p:Product) WHERE p.category IN ['a', 'b'] RETURN p.sku AS value ORDER BY value",
        "SELECT sku AS value FROM Product WHERE category IN ['a', 'b'] ORDER BY sku");
  }

  @Test
  void rangeOnTheLeadingPropertyOfACompositeIndex() {
    assertThat(cypher("MATCH (p:Product) WHERE p.category > 'a' AND p.category < 'c' RETURN p.sku AS value ORDER BY value"))
        .containsExactly("p3");
    assertSqlParity("MATCH (p:Product) WHERE p.category > 'a' AND p.category < 'c' RETURN p.sku AS value ORDER BY value",
        "SELECT sku AS value FROM Product WHERE category > 'a' AND category < 'c' ORDER BY sku");
  }

  @Test
  void equalityWithAValueOfAnotherTypeThanTheKeyReturnsNoRowsWithoutFailing() {
    // a cross-category comparison is never true in Cypher: the prefix seek must not push a value the
    // index key cannot represent and blow up (issue #5225 for the range-scan equivalent)
    assertThat(cypher("MATCH (p:Product) WHERE p.category = 42 RETURN p.sku AS value")).isEmpty();
    assertSqlParity("MATCH (p:Product) WHERE p.category = 42 RETURN p.sku AS value",
        "SELECT sku AS value FROM Product WHERE category = 42");
  }

  @Test
  void aSeekableCompositePredicateStillUsesTheIndex() {
    assertThat(planOf("MATCH (a:Area) WHERE a.id = 'country' RETURN a.id AS value"))
        .contains("NodeIndexSeek")
        .contains("Area[id,zone]");
    assertThat(planOf("MATCH (p:Product) WHERE p.category = 'a' AND p.price = 20 RETURN p.sku AS value"))
        .contains("NodeIndexSeek")
        .contains("Product[category,price]");
  }

  private void assertSqlParity(final String cypherQuery, final String sqlQuery) {
    assertThat(cypher(cypherQuery))
        .as("the Cypher engine must agree with SQL: %s", cypherQuery)
        .isEqualTo(values(database.query("sql", sqlQuery)));
  }

  private List<String> cypher(final String query) {
    return values(database.query("opencypher", query));
  }

  private List<String> cypher(final String query, final Map<String, Object> parameters) {
    return values(database.query("opencypher", query, parameters));
  }

  private String planOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "PROFILE " + query)) {
      while (resultSet.hasNext())
        resultSet.next();
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static List<String> values(final ResultSet resultSet) {
    final List<String> values = new ArrayList<>();
    try (resultSet) {
      while (resultSet.hasNext())
        values.add(String.valueOf(resultSet.next().<Object>getProperty("value")));
    }
    return values;
  }

  private void newProduct(final String category, final int price, final String sku) {
    database.newVertex("Product").set("category", category).set("price", price).set("sku", sku).save();
  }
}
