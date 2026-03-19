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
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional coverage tests for MATCH statement executor steps.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLMatchAdditionalCoverageTest extends TestHelper {

  public SQLMatchAdditionalCoverageTest() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Department");
    database.command("sql", "CREATE EDGE TYPE WorksAt");
    database.command("sql", "CREATE EDGE TYPE Knows");
    database.command("sql", "CREATE EDGE TYPE Manages");

    // Create persons
    for (int i = 0; i < 8; i++)
      database.command("sql", "CREATE VERTEX Person SET name = 'p" + i + "', idx = " + i);

    // Create departments
    database.command("sql", "CREATE VERTEX Department SET name = 'Engineering'");
    database.command("sql", "CREATE VERTEX Department SET name = 'Sales'");

    // Person knows chain: p0->p1->p2->p3->p4->p5
    for (int i = 0; i < 5; i++)
      database.command("sql",
          "CREATE EDGE Knows FROM (SELECT FROM Person WHERE idx = ?) TO (SELECT FROM Person WHERE idx = ?)", i, i + 1);

    // p5->p6, p6->p7 (branch)
    database.command("sql",
        "CREATE EDGE Knows FROM (SELECT FROM Person WHERE idx = 5) TO (SELECT FROM Person WHERE idx = 6)");
    database.command("sql",
        "CREATE EDGE Knows FROM (SELECT FROM Person WHERE idx = 6) TO (SELECT FROM Person WHERE idx = 7)");

    // WorksAt: p0,p1,p2 -> Engineering; p3,p4 -> Sales
    for (int i = 0; i < 3; i++)
      database.command("sql",
          "CREATE EDGE WorksAt FROM (SELECT FROM Person WHERE idx = ?) TO (SELECT FROM Department WHERE name = 'Engineering')", i);
    for (int i = 3; i < 5; i++)
      database.command("sql",
          "CREATE EDGE WorksAt FROM (SELECT FROM Person WHERE idx = ?) TO (SELECT FROM Department WHERE name = 'Sales')", i);

    // Manages: p0 manages p1, p1 manages p2
    database.command("sql",
        "CREATE EDGE Manages FROM (SELECT FROM Person WHERE idx = 0) TO (SELECT FROM Person WHERE idx = 1)");
    database.command("sql",
        "CREATE EDGE Manages FROM (SELECT FROM Person WHERE idx = 1) TO (SELECT FROM Person WHERE idx = 2)");

    database.commit();
    database.begin();
  }

  // --- ReturnMatchElementsStep ---
  @Test
  void matchReturnElements() {
    final ResultSet rs = database.query("sql",
        "MATCH {type: Person, as: a, WHERE: (idx = 0)}-Knows->{type: Person, as: b} RETURN $elements");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  // --- ReturnMatchPathsStep ---
  @Test
  void matchReturnPaths() {
    final ResultSet rs = database.query("sql",
        "MATCH {type: Person, as: a, WHERE: (idx = 0)}-Knows->{type: Person, as: b} RETURN $paths");
    int count = 0;
    while (rs.hasNext()) {
      final Result item = rs.next();
      assertThat(item.getPropertyNames()).isNotEmpty();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  // --- ReturnMatchPathElementsStep ---
  @Test
  void matchReturnPathElements() {
    final ResultSet rs = database.query("sql",
        "MATCH {type: Person, as: a, WHERE: (idx = 0)}-Knows->{type: Person, as: b} RETURN $pathElements");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  // --- ReturnMatchPatternsStep ---
  @Test
  void matchReturnPatterns() {
    final ResultSet rs = database.query("sql",
        "MATCH {type: Person, as: a, WHERE: (idx = 0)}-Knows->{type: Person, as: b} RETURN $patterns");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  // --- MatchReverseEdgeTraverser ---
  @Test
  void matchReverseEdge() {
    final ResultSet rs = database.query("sql",
        "MATCH {type: Person, as: b, WHERE: (idx = 1)}<-Knows-{type: Person, as: a} RETURN a.name as aName, b.name as bName");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<String>getProperty("aName")).isEqualTo("p0");
    assertThat(item.<String>getProperty("bName")).isEqualTo("p1");
    rs.close();
  }

  // --- MatchMultiEdgeTraverser ---
  @Test
  void matchMultiHop() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx = 0)}-Knows->{type: Person}-Knows->{type: Person, as: c} \
        RETURN a.name as aName, c.name as cName""");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<String>getProperty("aName")).isEqualTo("p0");
    assertThat(item.<String>getProperty("cName")).isEqualTo("p2");
    rs.close();
  }

  // --- WhileMatchStep ---
  @Test
  void matchWithWhile() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx = 0)} --> {as: b, while: ($depth < 4)} \
        RETURN a.name as aName, b.name as bName""");
    int count = 0;
    while (rs.hasNext()) {
      final Result item = rs.next();
      assertThat(item.<String>getProperty("aName")).isEqualTo("p0");
      count++;
    }
    assertThat(count).isGreaterThan(1); // Should find multiple depths
    rs.close();
  }

  // --- OptionalMatchStep ---
  @Test
  void optionalMatch() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx = 7)}.out('Knows'){OPTIONAL: true, as: b} \
        RETURN a.name as aName, b.name as bName""");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<String>getProperty("aName")).isEqualTo("p7");
    // p7 has no outgoing Knows edges, b should be null
    assertThat(item.<String>getProperty("bName")).isNull();
    rs.close();
  }

  @Test
  void optionalMatchWithResults() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx = 0)}.out('Knows'){OPTIONAL: true, as: b} \
        RETURN a.name as aName, b.name as bName""");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<String>getProperty("aName")).isEqualTo("p0");
    assertThat(item.<String>getProperty("bName")).isEqualTo("p1");
    rs.close();
  }

  // --- MatchPrefetchStep ---
  @Test
  void matchWithTypeFilter() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: person, WHERE: (idx < 3)}.out('WorksAt'){type: Department, as: dept} \
        RETURN person.name as pName, dept.name as dName""");
    int count = 0;
    while (rs.hasNext()) {
      final Result item = rs.next();
      assertThat(item.<String>getProperty("dName")).isEqualTo("Engineering");
      count++;
    }
    assertThat(count).isEqualTo(3);
    rs.close();
  }

  // --- MATCH with complex pattern and WHERE ---
  @Test
  void matchWithWhereOnPattern() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx < 5)}-Knows->{type: Person, as: b, WHERE: (idx > 2)} \
        RETURN a.name as aName, b.name as bName""");
    int count = 0;
    while (rs.hasNext()) {
      final Result item = rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  // --- MATCH with both() ---
  @Test
  void matchBothDirection() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx = 2)}.both('Knows'){type: Person, as: b} \
        RETURN a.name as aName, b.name as bName""");
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("bName"));
    // p2 has incoming from p1 and outgoing to p3
    assertThat(names).contains("p1", "p3");
    rs.close();
  }

  // --- MATCH with multiple patterns ---
  @Test
  void matchMultiplePatterns() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx = 0)}-Knows->{type: Person, as: b}, \
        {as: a}.out('Manages'){as: c} \
        RETURN a.name as aName, b.name as bName, c.name as cName""");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<String>getProperty("aName")).isEqualTo("p0");
    rs.close();
  }

  // --- MATCH returning fields ---
  @Test
  void matchReturnFields() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx < 3)}-Knows->{type: Person, as: b} \
        RETURN a.name, a.idx, b.name, b.idx""");
    int count = 0;
    while (rs.hasNext()) {
      final Result item = rs.next();
      assertThat(item.getPropertyNames()).isNotEmpty();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  // --- MATCH with in() ---
  @Test
  void matchIncoming() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: manager}.out('Manages'){type: Person, as: employee} \
        RETURN manager.name as mName, employee.name as eName""");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(2); // p0->p1, p1->p2
    rs.close();
  }

  // --- MATCH with depth filter ---
  @Test
  void matchWithDepthFilter() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx = 0)} --> {as: b, while: ($depth < 3)} \
        RETURN a.name as aName, b.name as bName""");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  // --- MATCH filtering with WHERE in pattern ---
  @Test
  void matchFilteredPattern() {
    // Find persons who know someone with idx > 5
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx < 3)}-Knows->{type: Person, as: b, WHERE: (idx > 0)} \
        RETURN a.name as aName, b.name as bName""");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  // --- MATCH with edge type ---
  @Test
  void matchWithSpecificEdgeType() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx = 0)}-WorksAt->{type: Department, as: d} \
        RETURN a.name as person, d.name as dept""");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<String>getProperty("person")).isEqualTo("p0");
    assertThat(item.<String>getProperty("dept")).isEqualTo("Engineering");
    rs.close();
  }

  // --- MATCH with aggregation ---
  @Test
  void matchWithAggregation() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Department, as: d}<-WorksAt-{type: Person, as: p} \
        RETURN d.name as dept, count(p) as empCount GROUP BY dept ORDER BY dept""");
    final List<String> depts = new ArrayList<>();
    while (rs.hasNext()) {
      final Result item = rs.next();
      depts.add(item.getProperty("dept"));
      assertThat(item.<Long>getProperty("empCount")).isGreaterThan(0L);
    }
    assertThat(depts).contains("Engineering", "Sales");
    rs.close();
  }

  // --- MATCH with LIMIT ---
  @Test
  void matchWithLimit() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a}-Knows->{type: Person, as: b} \
        RETURN a.name, b.name LIMIT 3""");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(3);
    rs.close();
  }

  // --- MATCH with ORDER BY ---
  @Test
  void matchWithOrderBy() {
    final ResultSet rs = database.query("sql",
        """
        MATCH {type: Person, as: a, WHERE: (idx < 5)}-Knows->{type: Person, as: b} \
        RETURN a.idx as aIdx, b.idx as bIdx ORDER BY aIdx ASC""");
    int prevIdx = -1;
    while (rs.hasNext()) {
      final int aIdx = rs.next().getProperty("aIdx");
      assertThat(aIdx).isGreaterThanOrEqualTo(prevIdx);
      prevIdx = aIdx;
    }
    rs.close();
  }

  // --- MATCH with SKIP ---
  @Test
  void matchWithSkip() {
    final ResultSet allRs = database.query("sql",
        "MATCH {type: Person, as: a}-Knows->{type: Person, as: b} RETURN a.name, b.name");
    int total = 0;
    while (allRs.hasNext()) {
      allRs.next();
      total++;
    }
    allRs.close();

    final ResultSet rs = database.query("sql",
        "MATCH {type: Person, as: a}-Knows->{type: Person, as: b} RETURN a.name, b.name SKIP 2");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(total - 2);
    rs.close();
  }
}
