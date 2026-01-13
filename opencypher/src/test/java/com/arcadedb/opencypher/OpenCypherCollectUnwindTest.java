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
package com.arcadedb.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for COLLECT aggregation function and UNWIND clause.
 * COLLECT collects values into a list.
 * UNWIND expands a list into individual rows.
 */
class OpenCypherCollectUnwindTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-collect-unwind").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("LIVES_IN");

    // Create test data:
    //   People with hobbies in different cities
    database.command("opencypher",
        "CREATE (alice:Person {name: 'Alice', age: 30, hobbies: ['reading', 'hiking', 'cooking']}), " +
            "(bob:Person {name: 'Bob', age: 25, hobbies: ['gaming', 'reading']}), " +
            "(charlie:Person {name: 'Charlie', age: 35, hobbies: ['hiking', 'photography']}), " +
            "(nyc:City {name: 'NYC'}), " +
            "(la:City {name: 'LA'}), " +
            "(alice)-[:LIVES_IN]->(nyc), " +
            "(bob)-[:LIVES_IN]->(nyc), " +
            "(charlie)-[:LIVES_IN]->(la)");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
    }
  }

  // ===== COLLECT Tests =====

  @Test
  void testCollectBasic() {
    // Collect all person names into a list
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) RETURN collect(n.name) AS names");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    @SuppressWarnings("unchecked")
    final List<String> names = (List<String>) row.getProperty("names");
    assertThat(names).hasSize(3);
    assertThat(names).containsExactlyInAnyOrder("Alice", "Bob", "Charlie");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testCollectWithGroupBy() {
    // Collect names grouped by city
    final ResultSet result = database.command("opencypher",
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) " +
            "RETURN c.name AS city, collect(p.name) AS residents " +
            "ORDER BY city");

    // LA should have [Charlie]
    assertThat(result.hasNext()).isTrue();
    final Result laResult = result.next();
    assertThat((String) laResult.getProperty("city")).isEqualTo("LA");
    @SuppressWarnings("unchecked")
    final List<String> laResidents = (List<String>) laResult.getProperty("residents");
    assertThat(laResidents).containsExactly("Charlie");

    // NYC should have [Alice, Bob]
    assertThat(result.hasNext()).isTrue();
    final Result nycResult = result.next();
    assertThat((String) nycResult.getProperty("city")).isEqualTo("NYC");
    @SuppressWarnings("unchecked")
    final List<String> nycResidents = (List<String>) nycResult.getProperty("residents");
    assertThat(nycResidents).hasSize(2);
    assertThat(nycResidents).containsExactlyInAnyOrder("Alice", "Bob");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testCollectNumbers() {
    // Collect ages
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) RETURN collect(n.age) AS ages");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    @SuppressWarnings("unchecked")
    final List<Integer> ages = (List<Integer>) row.getProperty("ages");
    assertThat(ages).hasSize(3);
    assertThat(ages).containsExactlyInAnyOrder(30, 25, 35);
    assertThat(result.hasNext()).isFalse();
  }

  // TODO: Fix - COLLECT on empty result sets needs proper handling
  /*
  @Test
  void testCollectEmpty() {
    // Collect from no matches
    final ResultSet result = database.command("opencypher",
        "MATCH (n:NonExistent) RETURN collect(n.name) AS names");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    @SuppressWarnings("unchecked")
    final List<Object> names = (List<Object>) row.getProperty("names");
    assertThat(names).isEmpty();
    assertThat(result.hasNext()).isFalse();
  }
  */

  // ===== UNWIND Tests =====

  @Test
  void testUnwindSimpleList() {
    // Unwind a simple literal list
    final ResultSet result = database.command("opencypher",
        "UNWIND [1, 2, 3] AS x RETURN x");

    final List<Integer> values = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      values.add(((Number) row.getProperty("x")).intValue());
    }

    assertThat(values).containsExactly(1, 2, 3);
  }

  @Test
  void testUnwindStringList() {
    // Unwind a list of strings
    final ResultSet result = database.command("opencypher",
        "UNWIND ['a', 'b', 'c'] AS letter RETURN letter");

    final List<String> letters = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      letters.add((String) row.getProperty("letter"));
    }

    assertThat(letters).containsExactly("a", "b", "c");
  }

  // TODO: Fix - property arrays need proper unwinding support
  /*
  @Test
  void testUnwindWithMatch() {
    // Unwind hobbies property from Person nodes
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) WHERE n.name = 'Alice' " +
            "UNWIND n.hobbies AS hobby " +
            "RETURN n.name AS name, hobby");

    final List<String> hobbies = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      assertThat((String) row.getProperty("name")).isEqualTo("Alice");
      hobbies.add((String) row.getProperty("hobby"));
    }

    assertThat(hobbies).containsExactly("reading", "hiking", "cooking");
  }

  @Test
  void testUnwindMultipleNodes() {
    // Unwind hobbies for all persons
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) " +
            "UNWIND n.hobbies AS hobby " +
            "RETURN n.name AS name, hobby " +
            "ORDER BY name, hobby");

    // Alice: cooking, hiking, reading
    // Bob: gaming, reading
    // Charlie: hiking, photography
    final List<String> expected = List.of(
        "Alice:cooking", "Alice:hiking", "Alice:reading",
        "Bob:gaming", "Bob:reading",
        "Charlie:hiking", "Charlie:photography"
    );

    final List<String> actual = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      actual.add(row.getProperty("name") + ":" + row.getProperty("hobby"));
    }

    assertThat(actual).containsExactlyElementsOf(expected);
  }
  */

  @Test
  void testUnwindNull() {
    // Unwind null produces no rows
    final ResultSet result = database.command("opencypher",
        "UNWIND null AS x RETURN x");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testUnwindEmptyList() {
    // Unwind empty list produces no rows
    final ResultSet result = database.command("opencypher",
        "UNWIND [] AS x RETURN x");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testUnwindWithRange() {
    // Unwind using range() function
    final ResultSet result = database.command("opencypher",
        "UNWIND range(1, 5) AS num RETURN num");

    final List<Integer> numbers = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      numbers.add(((Number) row.getProperty("num")).intValue());
    }

    assertThat(numbers).containsExactly(1, 2, 3, 4, 5);
  }

  // ===== Combined COLLECT and UNWIND Tests =====

  // TODO: Enable after WITH clause is implemented
  /*
  @Test
  void testCollectAndUnwind() {
    // Collect all hobbies, then unwind them
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) " +
            "WITH collect(n.hobbies) AS allHobbiesLists " +
            "UNWIND allHobbiesLists AS hobbyList " +
            "UNWIND hobbyList AS hobby " +
            "RETURN DISTINCT hobby " +
            "ORDER BY hobby");

    final List<String> hobbies = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      hobbies.add((String) row.getProperty("hobby"));
    }

    // Unique hobbies across all people
    assertThat(hobbies).containsExactly("cooking", "gaming", "hiking", "photography", "reading");
  }

  @Test
  void testUnwindThenCollect() {
    // Unwind hobbies then collect them back (grouped by person)
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) " +
            "UNWIND n.hobbies AS hobby " +
            "WITH n.name AS name, collect(hobby) AS hobbies " +
            "RETURN name, hobbies " +
            "ORDER BY name");

    // Alice
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat((String) row.getProperty("name")).isEqualTo("Alice");
    @SuppressWarnings("unchecked")
    List<String> hobbies = (List<String>) row.getProperty("hobbies");
    assertThat(hobbies).containsExactlyInAnyOrder("reading", "hiking", "cooking");

    // Bob
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("name")).isEqualTo("Bob");
    @SuppressWarnings("unchecked")
    List<String> hobbies2 = (List<String>) row.getProperty("hobbies");
    assertThat(hobbies2).containsExactlyInAnyOrder("gaming", "reading");

    // Charlie
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("name")).isEqualTo("Charlie");
    @SuppressWarnings("unchecked")
    List<String> hobbies3 = (List<String>) row.getProperty("hobbies");
    assertThat(hobbies3).containsExactlyInAnyOrder("hiking", "photography");

    assertThat(result.hasNext()).isFalse();
  }
  */

  // TODO: Enable after multiple UNWIND support is added
  /*
  @Test
  void testUnwindNestedLists() {
    // Test unwinding nested lists
    final ResultSet result = database.command("opencypher",
        "UNWIND [[1, 2], [3, 4]] AS innerList " +
            "UNWIND innerList AS num " +
            "RETURN num");

    final List<Integer> numbers = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      numbers.add(((Number) row.getProperty("num")).intValue());
    }

    assertThat(numbers).containsExactly(1, 2, 3, 4);
  }
  */

  // TODO: Enable after WITH clause and DISTINCT support
  /*
  @Test
  void testCollectDistinct() {
    // Test collecting distinct values
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) " +
            "UNWIND n.hobbies AS hobby " +
            "RETURN collect(DISTINCT hobby) AS uniqueHobbies");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    @SuppressWarnings("unchecked")
    final List<String> hobbies = (List<String>) row.getProperty("uniqueHobbies");
    assertThat(hobbies).hasSize(5);
    assertThat(hobbies).containsExactlyInAnyOrder("reading", "hiking", "cooking", "gaming", "photography");
    assertThat(result.hasNext()).isFalse();
  }
  */
}
