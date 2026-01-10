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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for issue #3075: RegEx in parser's filter conditions
 * Tests that property paths with backticks work correctly with indexes
 * and that invalid paths (consecutive dots) are handled properly
 */
public class Issue3075PropertyPathValidationTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      // Create schema with properties that require backticks
      database.command("sql", "CREATE VERTEX TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.`product-id` INTEGER");
      database.command("sql", "CREATE PROPERTY Product.`category-name` STRING");

      // Create embedded document type with property that needs backticks
      database.command("sql", "CREATE DOCUMENT TYPE Tag");
      database.command("sql", "CREATE PROPERTY Tag.`tag-id` INTEGER");
      database.command("sql", "CREATE PROPERTY Tag.`tag-name` STRING");

      // Create list property
      database.command("sql", "CREATE PROPERTY Product.tags LIST OF Tag");

      // Create index on property with backticks
      database.command("sql", "CREATE INDEX ON Product (`product-id`) UNIQUE");

      // Create BY ITEM index on nested property with backticks
      database.command("sql", "CREATE INDEX ON Product (`tags.tag-id` BY ITEM) NOTUNIQUE");
    });
  }

  @Test
  void testPropertyWithBackticksCanUseIndex() {
    // Insert test data
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO Product SET `product-id` = 1, `category-name` = 'Electronics', " +
          "tags = [{'@type':'Tag', 'tag-id': 100, 'tag-name': 'new'}, {'@type':'Tag', 'tag-id': 101, 'tag-name': 'featured'}]");
      database.command("sql",
          "INSERT INTO Product SET `product-id` = 2, `category-name` = 'Books', " +
          "tags = [{'@type':'Tag', 'tag-id': 100, 'tag-name': 'new'}, {'@type':'Tag', 'tag-id': 102, 'tag-name': 'bestseller'}]");
      database.command("sql",
          "INSERT INTO Product SET `product-id` = 3, `category-name` = 'Clothing', " +
          "tags = [{'@type':'Tag', 'tag-id': 103, 'tag-name': 'sale'}]");
    });

    database.transaction(() -> {
      // Test simple property with backticks
      ResultSet result = database.query("sql", "SELECT FROM Product WHERE `product-id` = 1");
      assertThat(result.stream().count()).isEqualTo(1);

      // Verify index is being used for backtick property
      String explain = database.query("sql", "EXPLAIN SELECT FROM Product WHERE `product-id` = 1")
          .next().getProperty("executionPlan").toString();
      assertThat(explain).contains("FETCH FROM INDEX Product[product-id]");
    });

    database.transaction(() -> {
      // Test CONTAINSANY on nested property with backticks (this is where the bug occurs)
      ResultSet result = database.query("sql", "SELECT FROM Product WHERE `tags.tag-id` CONTAINSANY [100, 101]");
      assertThat(result.stream().count()).isEqualTo(2); // Products 1 and 2

      // Verify index is being used for backtick nested property
      String explain = database.query("sql", "EXPLAIN SELECT FROM Product WHERE `tags.tag-id` CONTAINSANY [100]")
          .next().getProperty("executionPlan").toString();
      System.out.println("Explain plan: " + explain);
      // This should use the index but currently fails due to regex issue
      assertThat(explain).contains("FETCH FROM INDEX Product[tags.tag-idbyitem]");
    });

    database.transaction(() -> {
      // Test CONTAINS on nested property with backticks
      ResultSet result = database.query("sql", "SELECT FROM Product WHERE `tags.tag-id` CONTAINS 100");
      assertThat(result.stream().count()).isEqualTo(2); // Products 1 and 2

      // Verify index is being used
      String explain = database.query("sql", "EXPLAIN SELECT FROM Product WHERE `tags.tag-id` CONTAINS 100")
          .next().getProperty("executionPlan").toString();
      System.out.println("Explain plan for CONTAINS: " + explain);
      assertThat(explain).contains("FETCH FROM INDEX Product[tags.tag-idbyitem]");
    });
  }

  @Test
  void testNestedPropertyWithBackticksInMiddle() {
    database.transaction(() -> {
      // Create a type with a normal property name that has a nested property with backticks
      database.command("sql", "CREATE VERTEX TYPE Article");
      database.command("sql", "CREATE DOCUMENT TYPE Meta");
      database.command("sql", "CREATE PROPERTY Meta.`content-type` STRING");
      database.command("sql", "CREATE PROPERTY Article.metadata LIST OF Meta");
      database.command("sql", "CREATE INDEX ON Article (`metadata.content-type` BY ITEM) NOTUNIQUE");

      // Insert data
      database.command("sql",
          "INSERT INTO Article SET metadata = [{'@type':'Meta', 'content-type': 'text/html'}]");
      database.command("sql",
          "INSERT INTO Article SET metadata = [{'@type':'Meta', 'content-type': 'text/plain'}]");
    });

    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM Article WHERE `metadata.content-type` CONTAINS 'text/html'");
      assertThat(result.stream().count()).isEqualTo(1);

      // Verify index usage
      String explain = database.query("sql", "EXPLAIN SELECT FROM Article WHERE `metadata.content-type` CONTAINS 'text/html'")
          .next().getProperty("executionPlan").toString();
      System.out.println("Explain plan for nested backtick: " + explain);
      assertThat(explain).contains("FETCH FROM INDEX Article[metadata.content-typebyitem]");
    });
  }

  @Test
  void testBackticksAtStartOfNestedPath() {
    // THIS IS THE ACTUAL BUG CASE: using `property-with-dash`.nestedProperty in WHERE clause
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Page");
      database.command("sql", "CREATE DOCUMENT TYPE PageItem");
      database.command("sql", "CREATE PROPERTY PageItem.value STRING");
      database.command("sql", "CREATE PROPERTY Page.`item-list` LIST OF PageItem");

      // Create index using the full path in backticks (this is the supported syntax)
      database.command("sql", "CREATE INDEX ON Page (`item-list.value` BY ITEM) NOTUNIQUE");

      // Insert test data
      database.command("sql",
          "INSERT INTO Page SET `item-list` = [{'@type':'PageItem', 'value': 'test1'}, {'@type':'PageItem', 'value': 'test2'}]");
      database.command("sql",
          "INSERT INTO Page SET `item-list` = [{'@type':'PageItem', 'value': 'test1'}, {'@type':'PageItem', 'value': 'test3'}]");
    });

    database.transaction(() -> {
      // Query using the mixed syntax: `item-list`.value (backtick only on first part)
      // This tests the case where left.toString() returns "`item-list`.value" with partial backticks
      ResultSet result = database.query("sql", "SELECT FROM Page WHERE `item-list`.value CONTAINSANY ['test1']");
      assertThat(result.stream().count()).isEqualTo(2);

      // THIS SHOULD USE THE INDEX but may not if the regex doesn't handle the partial backtick syntax
      String explain = database.query("sql", "EXPLAIN SELECT FROM Page WHERE `item-list`.value CONTAINSANY ['test1']")
          .next().getProperty("executionPlan").toString();
      System.out.println("Explain plan for partial backtick syntax: " + explain);
      assertThat(explain).contains("FETCH FROM INDEX Page[item-list.valuebyitem]");
    });
  }
}
