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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class UnwindExpandStepTest extends TestHelper {

  @Test
  void shouldUnwindArray() {
    database.getSchema().createDocumentType("TestUnwind");

    database.transaction(() -> {
      database.newDocument("TestUnwind").set("id", 1).set("tags", Arrays.asList("java", "database", "graph")).save();
      database.newDocument("TestUnwind").set("id", 2).set("tags", Arrays.asList("nosql", "performance")).save();
    });

    final ResultSet result = database.query("sql", "SELECT id, tags FROM TestUnwind UNWIND tags");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object id = item.getProperty("id");
      assertThat(id).isNotNull();
      final String tag = item.getProperty("tags");
      assertThat(tag).isNotNull().isInstanceOf(String.class);
      count++;
    }

    assertThat(count).isEqualTo(5); // 3 + 2 tags
    result.close();
  }

  @Test
  void shouldUnwindEmptyArray() {
    database.getSchema().createDocumentType("TestUnwindEmpty");

    database.transaction(() -> {
      database.newDocument("TestUnwindEmpty").set("id", 1).set("tags", Arrays.asList()).save();
      database.newDocument("TestUnwindEmpty").set("id", 2).set("tags", Arrays.asList("test")).save();
    });

    final ResultSet result = database.query("sql", "SELECT id, tags FROM TestUnwindEmpty UNWIND tags");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    // UNWIND on empty array still produces the record (with empty array unwound)
    // Both records are returned: one with empty tags, one with "test"
    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldUnwindNullValues() {
    database.getSchema().createDocumentType("TestUnwindNull");

    database.transaction(() -> {
      database.newDocument("TestUnwindNull").set("id", 1).save(); // No tags field
      database.newDocument("TestUnwindNull").set("id", 2).set("tags", Arrays.asList("test")).save();
    });

    final ResultSet result = database.query("sql", "SELECT id, tags FROM TestUnwindNull UNWIND tags");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    // UNWIND on null/missing field still produces the record
    // Both records are returned
    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldUnwindNestedArrays() {
    database.getSchema().createDocumentType("TestUnwindNested");

    database.transaction(() -> {
      database.newDocument("TestUnwindNested")
          .set("id", 1)
          .set("groups", Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("c", "d")))
          .save();
    });

    final ResultSet result = database.query("sql", "SELECT id, groups FROM TestUnwindNested UNWIND groups");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object groups = item.getProperty("groups");
      assertThat(groups).isInstanceOf(List.class);
      count++;
    }

    assertThat(count).isEqualTo(2); // 2 nested arrays
    result.close();
  }

  @Test
  void shouldExpandCollection() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createDocumentType("Group");

    database.transaction(() -> {
      final var person1 = database.newVertex("Person").set("name", "Alice").save();
      final var person2 = database.newVertex("Person").set("name", "Bob").save();
      final var person3 = database.newVertex("Person").set("name", "Charlie").save();

      database.newDocument("Group")
          .set("name", "Team")
          .set("members", Arrays.asList(person1.getIdentity(), person2.getIdentity(), person3.getIdentity()))
          .save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(members) FROM Group");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String name = item.getProperty("name");
      assertThat(name).isIn("Alice", "Bob", "Charlie");
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldExpandSingleValue() {
    database.getSchema().createDocumentType("TestExpandSingle");

    database.transaction(() -> {
      database.newDocument("TestExpandSingle").set("value", "test").save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(value) FROM TestExpandSingle");

    // expand() on a primitive value (non-collection) returns no results
    // expand() is designed to work with collections/arrays
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldUnwindWithFilter() {
    database.getSchema().createDocumentType("TestUnwindFilter");

    database.transaction(() -> {
      database.newDocument("TestUnwindFilter").set("id", 1).set("numbers", Arrays.asList(1, 2, 3, 4, 5)).save();
      database.newDocument("TestUnwindFilter").set("id", 2).set("numbers", Arrays.asList(6, 7, 8, 9, 10)).save();
    });

    // Correct syntax: WHERE comes before UNWIND
    final ResultSet result = database.query("sql", "SELECT id, numbers FROM TestUnwindFilter WHERE numbers.size() > 0 UNWIND numbers");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int number = item.getProperty("numbers");
      // All numbers from both records (both pass the WHERE filter)
      assertThat(number).isBetween(1, 10);
      count++;
    }

    assertThat(count).isEqualTo(10); // All numbers from both records
    result.close();
  }

  @Test
  void shouldUnwindMultipleFields() {
    database.getSchema().createDocumentType("TestUnwindMulti");

    database.transaction(() -> {
      database.newDocument("TestUnwindMulti")
          .set("id", 1)
          .set("tags", Arrays.asList("tag1", "tag2"))
          .set("categories", Arrays.asList("cat1", "cat2"))
          .save();
    });

    // Unwind tags first
    final ResultSet result = database.query("sql", "SELECT id, tags, categories FROM TestUnwindMulti UNWIND tags");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object tags = item.getProperty("tags");
      final Object categories = item.getProperty("categories");
      assertThat(tags).isInstanceOf(String.class);
      assertThat(categories).isInstanceOf(List.class);
      count++;
    }

    assertThat(count).isEqualTo(2); // 2 tags
    result.close();
  }

  @Test
  void shouldHandleUnwindWithGroupBy() {
    database.getSchema().createDocumentType("TestUnwindGroup");

    database.transaction(() -> {
      database.newDocument("TestUnwindGroup").set("category", "A").set("values", Arrays.asList(1, 2, 3)).save();
      database.newDocument("TestUnwindGroup").set("category", "B").set("values", Arrays.asList(4, 5)).save();
      database.newDocument("TestUnwindGroup").set("category", "A").set("values", Arrays.asList(6, 7)).save();
    });

    // UNWIND first, then use subquery for GROUP BY
    final ResultSet result = database.query("sql",
        "SELECT category, sum(values) as total FROM (SELECT category, values FROM TestUnwindGroup UNWIND values) GROUP BY category");

    int count = 0;
    int totalA = 0;
    int totalB = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String category = item.getProperty("category");
      final Object total = item.getProperty("total");
      assertThat(category).isIn("A", "B");
      assertThat(total).isNotNull();
      if ("A".equals(category))
        totalA = (int) total;
      else
        totalB = (int) total;
      count++;
    }

    assertThat(count).isEqualTo(2); // 2 categories
    assertThat(totalA).isEqualTo(19); // 1+2+3+6+7 = 19
    assertThat(totalB).isEqualTo(9); // 4+5 = 9
    result.close();
  }

  @Test
  void shouldExpandEmptyCollection() {
    database.getSchema().createDocumentType("TestExpandEmpty");

    database.transaction(() -> {
      database.newDocument("TestExpandEmpty").set("items", Arrays.asList()).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(items) FROM TestExpandEmpty");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  // Issue #1582: SELECT FROM doc UNWIND lst (no projection) returns one record per list element
  @Test
  void unwindWithoutProjection() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.lst LIST");
      database.command("sql", "INSERT INTO doc SET lst = [1,2,3]");
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT FROM doc UNWIND lst");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      assertThat(results).hasSize(3);
      assertThat(results.get(0).<Integer>getProperty("lst")).isEqualTo(1);
      assertThat(results.get(1).<Integer>getProperty("lst")).isEqualTo(2);
      assertThat(results.get(2).<Integer>getProperty("lst")).isEqualTo(3);
    });
  }

  // Issue #1582: SELECT @rid FROM doc UNWIND lst returns one record per list element with the @rid projection preserved
  @Test
  void unwindWithRidProjection() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.lst LIST");
      database.command("sql", "INSERT INTO doc SET lst = [1,2,3]");
    });

    database.transaction(() -> {
      final ResultSet ridResult = database.query("sql", "SELECT @rid FROM doc");
      assertThat(ridResult.hasNext()).isTrue();
      final RID expectedRid = ridResult.next().getProperty("@rid");

      final ResultSet result = database.query("sql", "SELECT @rid FROM doc UNWIND lst");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      assertThat(results).hasSize(3);

      for (final Result row : results) {
        assertThat(row.<RID>getProperty("@rid")).isEqualTo(expectedRid);
      }
    });
  }

  // Issue #1582: SELECT name FROM doc WHERE ... UNWIND lst returns one record per list element with a projection that does not include the unwind field
  @Test
  void unwindWithCustomProjection() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.lst LIST");
      database.command("sql", "INSERT INTO doc SET lst = [1,2,3]");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET name = 'test', lst = ['a','b','c']");

      final ResultSet result = database.query("sql", "SELECT name FROM doc WHERE name = 'test' UNWIND lst");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      assertThat(results).hasSize(3);

      for (final Result row : results) {
        assertThat(row.<String>getProperty("name")).isEqualTo("test");
      }
    });
  }

  // Issue #1582: SELECT lst FROM doc UNWIND lst returns one record per list element where the projection includes the unwind field
  @Test
  void unwindWithUnwindFieldInProjection() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.lst LIST");
      database.command("sql", "INSERT INTO doc SET lst = [1,2,3]");
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT lst FROM doc UNWIND lst");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      assertThat(results).hasSize(3);
      assertThat(results.get(0).<Integer>getProperty("lst")).isEqualTo(1);
      assertThat(results.get(1).<Integer>getProperty("lst")).isEqualTo(2);
      assertThat(results.get(2).<Integer>getProperty("lst")).isEqualTo(3);
    });
  }

  // Issue #1582: SELECT name, count FROM doc ... UNWIND tags returns one record per tag with multiple projections preserved
  @Test
  void unwindWithMultipleProjections() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.lst LIST");
      database.command("sql", "INSERT INTO doc SET lst = [1,2,3]");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET name = 'multi', tags = ['x','y','z'], count = 10");

      final ResultSet result = database.query("sql", "SELECT name, count FROM doc WHERE name = 'multi' UNWIND tags");

      final List<Result> results = new ArrayList<>();
      while (result.hasNext())
        results.add(result.next());

      assertThat(results).hasSize(3);

      for (final Result row : results) {
        assertThat(row.<String>getProperty("name")).isEqualTo("multi");
        assertThat(row.<Integer>getProperty("count")).isEqualTo(10);
      }
    });
  }

  // Issue #2776: SELECT $nrid FROM (SELECT expand(...) FROM Tag LET $nrid = @rid WHERE id = '1') keeps the LET-bound RID stable across all expanded rows
  @Test
  void expandWithLetVariable() {
    setupNewsTagsGraph();

    database.transaction(() -> {
      final ResultSet tagResult = database.query("sql", "SELECT @rid FROM Tag WHERE id = '1'");
      assertThat(tagResult.hasNext()).isTrue();
      final RID expectedRid = tagResult.next().getProperty("@rid");
      assertThat(expectedRid).isNotNull();

      final String query = "SELECT $nrid FROM (SELECT expand(out('HasTag').inE('HasTag')) FROM Tag LET $nrid = @rid WHERE id = '1')";
      final ResultSet result = database.query("sql", query);

      final List<RID> nridValues = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        final Object nridValue = row.getProperty("$nrid");
        assertThat(nridValue).isInstanceOf(RID.class);
        nridValues.add((RID) nridValue);
      }

      assertThat(nridValues).hasSize(7);

      for (int i = 0; i < nridValues.size(); i++) {
        assertThat(nridValues.get(i))
            .as("Row %d should have $nrid = %s but was %s", i, expectedRid, nridValues.get(i))
            .isEqualTo(expectedRid);
      }
    });
  }

  // Issue #2776: SELECT $nrid FROM (SELECT ... AS tags FROM Tag LET $nrid = @rid WHERE id = '1' UNWIND tags) keeps the LET-bound RID stable across all unwound rows
  @Test
  void unwindWithLetVariable() {
    setupNewsTagsGraph();

    database.transaction(() -> {
      final ResultSet tagResult = database.query("sql", "SELECT @rid FROM Tag WHERE id = '1'");
      assertThat(tagResult.hasNext()).isTrue();
      final RID expectedRid = tagResult.next().getProperty("@rid");
      assertThat(expectedRid).isNotNull();

      final String query = "SELECT $nrid FROM (SELECT out('HasTag').inE('HasTag') AS tags FROM Tag LET $nrid = @rid WHERE id = '1' UNWIND tags)";
      final ResultSet result = database.query("sql", query);

      final List<RID> nridValues = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        final Object nridValue = row.getProperty("$nrid");
        assertThat(nridValue).isInstanceOf(RID.class);
        nridValues.add((RID) nridValue);
      }

      assertThat(nridValues).hasSize(7);

      for (int i = 0; i < nridValues.size(); i++) {
        assertThat(nridValues.get(i))
            .as("Row %d should have $nrid = %s but was %s", i, expectedRid, nridValues.get(i))
            .isEqualTo(expectedRid);
      }
    });
  }

  private void setupNewsTagsGraph() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE News");
      database.command("sql", "CREATE VERTEX TYPE Tag");
      database.command("sql", "CREATE EDGE TYPE HasTag");

      for (int i = 1; i <= 10; i++) {
        database.command("sql", "INSERT INTO News CONTENT { \"id\": \"" + i + "\", \"title\": \"News " + i + "\", \"content\": \"Content " + i + "\" }");
      }

      for (int i = 1; i <= 10; i++) {
        database.command("sql", "INSERT INTO Tag CONTENT { \"id\": \"" + i + "\", \"name\": \"Tag " + i + "\" }");
      }

      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '1') TO (SELECT FROM News WHERE id = '1')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '2') TO (SELECT FROM News WHERE id = '1')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '3') TO (SELECT FROM News WHERE id = '1')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '4') TO (SELECT FROM News WHERE id = '1')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '5') TO (SELECT FROM News WHERE id = '1')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '1') TO (SELECT FROM News WHERE id = '2')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '2') TO (SELECT FROM News WHERE id = '2')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '6') TO (SELECT FROM News WHERE id = '10')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '9') TO (SELECT FROM News WHERE id = '10')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '10') TO (SELECT FROM News WHERE id = '10')");
    });
  }
}
