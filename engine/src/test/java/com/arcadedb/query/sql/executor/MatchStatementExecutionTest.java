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
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class MatchStatementExecutionTest extends TestHelper {
  public MatchStatementExecutionTest() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    database.command("sql", "CREATE VERTEX type Person");
    database.command("sql", "CREATE EDGE type Friend");
    database.command("sql", "CREATE VERTEX Person set name = 'n1'");
    database.command("sql", "CREATE VERTEX Person set name = 'n2'");
    database.command("sql", "CREATE VERTEX Person set name = 'n3'");
    database.command("sql", "CREATE VERTEX Person set name = 'n4'");
    database.command("sql", "CREATE VERTEX Person set name = 'n5'");
    database.command("sql", "CREATE VERTEX Person set name = 'n6'");

    final String[][] friendList = new String[][] { { "n1", "n2" }, { "n1", "n3" }, { "n2", "n4" }, { "n4", "n5" }, { "n4", "n6" } };

    for (final String[] pair : friendList) {
      database.command("sql", "CREATE EDGE Friend from (select from Person where name = ?) to (select from Person where name = ?)",
          pair[0], pair[1]);
    }

    database.command("sql", "CREATE VERTEX type MathOp");
    database.command("sql", "CREATE VERTEX MathOp set a = 1, b = 3, c = 2");
    database.command("sql", "CREATE VERTEX MathOp set a = 5, b = 3, c = 2");

    database.commit();

    database.begin();
    initOrgChart(database);
    database.commit();

    database.begin();
    initTriangleTest(database);
    database.commit();

    database.begin();
    initEdgeIndexTest(database);
    database.commit();

    database.begin();
    initDiamondTest(database);
    database.commit();

    database.begin();
  }

  private static void initEdgeIndexTest(final Database database) {
    database.command("sql", "CREATE vertex type IndexedVertex");
    database.command("sql", "CREATE property IndexedVertex.uid INTEGER");
    database.command("sql", "CREATE index on IndexedVertex (uid) NOTUNIQUE");

    final int nodes = 1000;
    for (int i = 0; i < nodes; i++) {
      final MutableDocument doc = database.newVertex("IndexedVertex");
      doc.set("uid", i);
      doc.save();
    }

    database.command("sql", "CREATE edge type IndexedEdge");

    for (int i = 0; i < 100; i++) {
      final String cmd =
          "CREATE EDGE IndexedEdge FROM (SELECT FROM IndexedVertex WHERE uid = 0) TO (SELECT FROM IndexedVertex WHERE uid > " + (
              i * nodes / 100) + " and uid <" + ((i + 1) * nodes / 100) + ")";
      database.command("sql", cmd);
    }

//    database.query("sql", "select expand(out()) from IndexedVertex where uid = 0").stream().forEach(x -> System.out.println("x = " + x));
  }

  private static void initOrgChart(final Database database) {

    // ______ [manager] department _______
    // _____ (employees in department)____
    // ___________________________________
    // ___________________________________
    // ____________[a]0___________________
    // _____________(p1)__________________
    // _____________/___\_________________
    // ____________/_____\________________
    // ___________/_______\_______________
    // _______[b]1_________2[d]___________
    // ______(p2, p3)_____(p4, p5)________
    // _________/_\_________/_\___________
    // ________3___4_______5___6__________
    // ______(p6)_(p7)___(p8)__(p9)_______
    // ______/__\_________________________
    // __[c]7_____8_______________________
    // __(p10)___(p11)____________________
    // ___/_______________________________
    // __9________________________________
    // (p12, p13)_________________________
    //
    // short description:
    // Department 0 is the company itself, "a" is the CEO
    // p10 works at department 7, his manager is "c"
    // p12 works at department 9, this department has no direct manager, so p12's manager is c (the
    // upper manager)

    database.command("sql", "CREATE vertex type Employee");
    database.command("sql", "CREATE vertex type Department");
    database.command("sql", "CREATE edge type ParentDepartment");
    database.command("sql", "CREATE edge type  WorksAt");
    database.command("sql", "CREATE edge type  ManagerOf ");

    final int[][] deptHierarchy = new int[10][];
    deptHierarchy[0] = new int[] { 1, 2 };
    deptHierarchy[1] = new int[] { 3, 4 };
    deptHierarchy[2] = new int[] { 5, 6 };
    deptHierarchy[3] = new int[] { 7, 8 };
    deptHierarchy[4] = new int[] {};
    deptHierarchy[5] = new int[] {};
    deptHierarchy[6] = new int[] {};
    deptHierarchy[7] = new int[] { 9 };
    deptHierarchy[8] = new int[] {};
    deptHierarchy[9] = new int[] {};

    final String[] deptManagers = { "a", "b", "d", null, null, null, null, "c", null, null };

    final String[][] employees = new String[10][];
    employees[0] = new String[] { "p1" };
    employees[1] = new String[] { "p2", "p3" };
    employees[2] = new String[] { "p4", "p5" };
    employees[3] = new String[] { "p6" };
    employees[4] = new String[] { "p7" };
    employees[5] = new String[] { "p8" };
    employees[6] = new String[] { "p9" };
    employees[7] = new String[] { "p10" };
    employees[8] = new String[] { "p11" };
    employees[9] = new String[] { "p12", "p13" };

    for (int i = 0; i < deptHierarchy.length; i++) {
      database.command("sql", "CREATE VERTEX Department set name = 'department" + i + "' ");
    }

    for (int parent = 0; parent < deptHierarchy.length; parent++) {
      final int[] children = deptHierarchy[parent];
      for (final int child : children) {
        database.command("sql", "CREATE EDGE ParentDepartment from (select from Department where name = 'department" + child
            + "') to (select from Department where name = 'department" + parent + "') ");
      }
    }

    for (int dept = 0; dept < deptManagers.length; dept++) {
      final String manager = deptManagers[dept];
      if (manager != null) {
        database.command("sql", "CREATE Vertex Employee set name = '" + manager + "' ");

        database.command("sql", "CREATE EDGE ManagerOf from (select from Employee where name = '" + manager + ""
            + "') to (select from Department where name = 'department" + dept + "') ");
      }
    }

    for (int dept = 0; dept < employees.length; dept++) {
      final String[] employeesForDept = employees[dept];
      for (final String employee : employeesForDept) {
        database.command("sql", "CREATE Vertex Employee set name = '" + employee + "' ");

        database.command("sql", "CREATE EDGE WorksAt from (select from Employee where name = '" + employee + ""
            + "') to (select from Department where name = 'department" + dept + "') ");
      }
    }
  }

  private static void initTriangleTest(final Database database) {
    database.command("sql", "CREATE vertex type TriangleV");
    database.command("sql", "CREATE property TriangleV.uid INTEGER");
    database.command("sql", "CREATE index on TriangleV (uid) UNIQUE");
    database.command("sql", "CREATE edge type TriangleE");
    for (int i = 0; i < 10; i++) {
      database.command("sql", "CREATE VERTEX TriangleV set uid = ?", i);
    }
    final int[][] edges = { { 0, 1 }, { 0, 2 }, { 1, 2 }, { 1, 3 }, { 2, 4 }, { 3, 4 }, { 3, 5 }, { 4, 0 }, { 4, 7 }, { 6, 7 },
        { 7, 8 }, { 7, 9 }, { 8, 9 }, { 9, 1 }, { 8, 3 }, { 8, 4 } };
    for (final int[] edge : edges) {
      database.command("sql",
          "CREATE EDGE TriangleE from (select from TriangleV where uid = ?) to (select from TriangleV where uid = ?)", edge[0],
          edge[1]);
    }
  }

  private static void initDiamondTest(final Database database) {
    database.command("sql", "CREATE vertex type DiamondV");
    database.command("sql", "CREATE edge type DiamondE");
    for (int i = 0; i < 4; i++) {
      database.command("sql", "CREATE VERTEX DiamondV set uid = ?", i);
    }
    final int[][] edges = { { 0, 1 }, { 0, 2 }, { 1, 3 }, { 2, 3 } };
    for (final int[] edge : edges) {
      database.command("sql",
          "CREATE EDGE DiamondE from (select from DiamondV where uid = ?) to (select from DiamondV where uid = ?)", edge[0],
          edge[1]);
    }
  }

  @Test
  void simple() {
    final ResultSet qResult = database.query("sql", "match {type:Person, as: person} return person");

    for (int i = 0; i < 6; i++) {
      final Result item = qResult.next();
      assertThat(item.getPropertyNames().size()).isEqualTo(1);
      final Document person = item.getProperty("person");

      final String name = person.getString("name");
      assertThat(name.startsWith("n")).isTrue();
    }
    qResult.close();
  }

  @Test
  void simpleWhere() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, as: person, where: (name = 'n1' or name = 'n2')} return person");

    for (int i = 0; i < 2; i++) {
      final Result item = qResult.next();
      assertThat(item.getPropertyNames().size()).isEqualTo(1);
      final Document personId = item.getProperty("person");

      final MutableDocument person = personId.getRecord().asVertex().modify();
      final String name = person.getString("name");
      assertThat(name.equals("n1") || name.equals("n2")).isTrue();
    }
    qResult.close();
  }

  @Test
  void simpleLimit() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit 1");
    assertThat(qResult.hasNext()).isTrue();
    qResult.next();
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void simpleLimit2() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit -1");
    for (int i = 0; i < 2; i++) {
      assertThat(qResult.hasNext()).isTrue();
      qResult.next();
    }
    qResult.close();
  }

  @Test
  void simpleLimit3() {

    final ResultSet qResult = database.query("sql",
        "match {type:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit 3");
    for (int i = 0; i < 2; i++) {
      assertThat(qResult.hasNext()).isTrue();
      qResult.next();
    }
    qResult.close();
  }

  @Test
  void simpleUnnamedParams() {
    final ResultSet qResult = database.query("sql", "match {type:Person, as: person, where: (name = ? or name = ?)} return person",
        "n1", "n2");

    for (int i = 0; i < 2; i++) {

      final Result item = qResult.next();
      assertThat(item.getPropertyNames().size()).isEqualTo(1);
      final Document person = item.getProperty("person");

      final String name = person.getString("name");
      assertThat(name.equals("n1") || name.equals("n2")).isTrue();
    }
    qResult.close();
  }

  @Test
  void commonFriends() {

    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend)");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void commonFriendsPatterns() {

    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $patterns)");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void pattens() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $patterns");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.getPropertyNames().size()).isEqualTo(1);
    assertThat(item.getPropertyNames().iterator().next()).isEqualTo("friend");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void paths() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $paths");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.getPropertyNames().size()).isEqualTo(3);
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void elements() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $elements");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void pathElements() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $pathElements");

    final Set<String> expected = new HashSet<>();
    expected.add("n1");
    expected.add("n2");
    expected.add("n4");
    for (int i = 0; i < 3; i++) {
      assertThat(qResult.hasNext()).isTrue();
      final Result item = qResult.next();
      expected.remove(item.<String>getProperty("name"));
    }
    assertThat(qResult.hasNext()).isFalse();
    assertThat(expected.isEmpty()).isTrue();
    qResult.close();
  }

  @Test
  void commonFriendsMatches() {

    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $matches)");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void commonFriendsArrows() {

    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return friend)");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void commonFriendsArrowsPatterns() {

    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return $patterns)");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void commonFriends2() {

    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend.name as name");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void commonFriends2Arrows() {

    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return friend.name as name");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void returnMethod() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend.name.toUpperCase(Locale.ENGLISH) as name");
    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("N2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void returnMethodArrows() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return friend.name.toUpperCase(Locale.ENGLISH) as name");
    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("N2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void returnExpression() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend.name + ' ' +friend.name as name");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n2 n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void returnExpressionArrows() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return friend.name + ' ' +friend.name as name");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n2 n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void returnDefaultAlias() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend.name");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("friend.name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void returnDefaultAliasArrows() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return friend.name");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("friend.name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void friendsOfFriends() {
    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend').out('Friend'){as:friend} return $matches)");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n4");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void friendsOfFriendsArrows() {
    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{}-Friend->{as:friend} return $matches)");

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("n4");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void friendsOfFriends2() {
    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1'), as: me}.both('Friend').both('Friend'){as:friend, where: ($matched.me != $currentMatch)} return $matches)");

    assertThat(qResult.hasNext()).isTrue();
    while (qResult.hasNext()) {
      final Result item = qResult.next();
      assertThat(item.<String>getProperty("name")).isNotEqualTo("n1");
    }
    qResult.close();
  }

  @Test
  void friendsOfFriends2Arrows() {
    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1'), as: me}-Friend-{}-Friend-{as:friend, where: ($matched.me != $currentMatch)} return $matches)");

    assertThat(qResult.hasNext()).isTrue();
    while (qResult.hasNext()) {
      assertThat(qResult.next().<String>getProperty("name")).isNotEqualTo("n1");
    }
    qResult.close();
  }

  @Test
  void friendsWithName() {
    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1' and 1 + 1 = 2)}.out('Friend'){as:friend, where:(name = 'n2' and 1 + 1 = 2)} return friend)");

    assertThat(qResult.hasNext()).isTrue();
    assertThat(qResult.next().<String>getProperty("name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void friendsWithNameArrows() {
    final ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1' and 1 + 1 = 2)}-Friend->{as:friend, where:(name = 'n2' and 1 + 1 = 2)} return friend)");
    assertThat(qResult.hasNext()).isTrue();
    assertThat(qResult.next().<String>getProperty("name")).isEqualTo("n2");
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
  }

  @Test
  void testWhile() {

    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 1)} return friend)");
    assertThat(size(qResult)).isEqualTo(3);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 2), where: ($depth=1) } return friend)");
    assertThat(size(qResult)).isEqualTo(2);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 4), where: ($depth=1) } return friend)");
    assertThat(size(qResult)).isEqualTo(2);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend)");
    assertThat(size(qResult)).isEqualTo(6);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend limit 3)");
    assertThat(size(qResult)).isEqualTo(3);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend) limit 3");
    assertThat(size(qResult)).isEqualTo(3);
    qResult.close();
  }

  private int size(final ResultSet qResult) {
    int result = 0;
    while (qResult.hasNext()) {
      result++;
      qResult.next();
    }
    return result;
  }

  @Test
  void whileArrows() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 1)} return friend)");
    assertThat(size(qResult)).isEqualTo(3);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 2), where: ($depth=1) } return friend)");
    assertThat(size(qResult)).isEqualTo(2);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 4), where: ($depth=1) } return friend)");
    assertThat(size(qResult)).isEqualTo(2);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, while: (true) } return friend)");
    assertThat(size(qResult)).isEqualTo(6);
    qResult.close();
  }

  @Test
  void maxDepth() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1, where: ($depth=1) } return friend)");
    assertThat(size(qResult)).isEqualTo(2);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1 } return friend)");
    assertThat(size(qResult)).isEqualTo(3);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 0 } return friend)");
    assertThat(size(qResult)).isEqualTo(1);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1, where: ($depth > 0) } return friend)");
    assertThat(size(qResult)).isEqualTo(2);
    qResult.close();
  }

  @Test
  void maxDepthArrow() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1, where: ($depth=1) } return friend)");
    assertThat(size(qResult)).isEqualTo(2);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1 } return friend)");
    assertThat(size(qResult)).isEqualTo(3);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 0 } return friend)");
    assertThat(size(qResult)).isEqualTo(1);
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1, where: ($depth > 0) } return friend)");
    assertThat(size(qResult)).isEqualTo(2);
    qResult.close();
  }

  @Test
  void manager() {
    // the manager of a person is the manager of the department that person belongs to.
    // if that department does not have a direct manager, climb up the hierarchy until you find one
    assertThat(getManager("p10").get("name")).isEqualTo("c");
    assertThat(getManager("p12").get("name")).isEqualTo("c");
    assertThat(getManager("p6").get("name")).isEqualTo("b");
    assertThat(getManager("p11").get("name")).isEqualTo("b");

    assertThat(getManagerArrows("p10").get("name")).isEqualTo("c");
    assertThat(getManagerArrows("p12").get("name")).isEqualTo("c");
    assertThat(getManagerArrows("p6").get("name")).isEqualTo("b");
    assertThat(getManagerArrows("p11").get("name")).isEqualTo("b");
  }

  @Test
  void expanded() {
    final StringBuilder query = new StringBuilder();
    query.append("select @type from ( ");
    query.append(" select expand(manager) from (");
    query.append("  match {type:Employee, where: (name = '" + "p10" + "')}");
    query.append("  .out('WorksAt')");
    query.append("  .out('ParentDepartment'){");
    query.append("      while: (in('ManagerOf').size() == 0),");
    query.append("      where: (in('ManagerOf').size() > 0)");
    query.append("  }");
    query.append("  .in('ManagerOf'){as: manager}");
    query.append("  return manager");
    query.append(" )");
    query.append(")");

    final ResultSet qResult = database.query("sql", query.toString());
    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();

    assertThat(item.<String>getProperty(Property.TYPE_PROPERTY)).isEqualTo("Employee");
  }

  private Document getManager(final String personName) {
    final StringBuilder query = new StringBuilder();
    query.append("select expand(manager) from (");
    query.append("  match {type:Employee, where: (name = '" + personName + "')}");
    query.append("  .out('WorksAt')");
    query.append("  .out('ParentDepartment'){");
    query.append("      while: (in('ManagerOf').size() == 0),");
    query.append("      where: (in('ManagerOf').size() > 0)");
    query.append("  }");
    query.append("  .in('ManagerOf'){as: manager}");
    query.append("  return manager");
    query.append(")");

    final ResultSet qResult = database.query("sql", query.toString());
    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
    return item.getElement().get().getRecord().asVertex();
  }

  private Document getManagerArrows(final String personName) {
    final StringBuilder query = new StringBuilder();
    query.append("select expand(manager) from (");
    query.append("  match {type:Employee, where: (name = '" + personName + "')}");
    query.append("  -WorksAt->{}-ParentDepartment->{");
    query.append("      while: (in('ManagerOf').size() == 0),");
    query.append("      where: (in('ManagerOf').size() > 0)");
    query.append("  }<-ManagerOf-{as: manager}");
    query.append("  return manager");
    query.append(")");

    final ResultSet qResult = database.query("sql", query.toString());

    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
    return item.getElement().get().getRecord().asVertex();
  }

  @Test
  void manager2() {
    // the manager of a person is the manager of the department that person belongs to.
    // if that department does not have a direct manager, climb up the hierarchy until you find one

    assertThat(getManager2("p10").<String>getProperty("name")).isEqualTo("c");
    assertThat(getManager2("p12").<String>getProperty("name")).isEqualTo("c");
    assertThat(getManager2("p6").<String>getProperty("name")).isEqualTo("b");
    assertThat(getManager2("p11").<String>getProperty("name")).isEqualTo("b");

    assertThat(getManager2Arrows("p10").<String>getProperty("name")).isEqualTo("c");
    assertThat(getManager2Arrows("p12").<String>getProperty("name")).isEqualTo("c");
    assertThat(getManager2Arrows("p6").<String>getProperty("name")).isEqualTo("b");
    assertThat(getManager2Arrows("p11").<String>getProperty("name")).isEqualTo("b");
  }

  private Result getManager2(final String personName) {
    final StringBuilder query = new StringBuilder();
    query.append("select expand(manager) from (");
    query.append("  match {type:Employee, where: (name = '" + personName + "')}");
    query.append("   .( out('WorksAt')");
    query.append("     .out('ParentDepartment'){");
    query.append("       while: (in('ManagerOf').size() == 0),");
    query.append("       where: (in('ManagerOf').size() > 0)");
    query.append("     }");
    query.append("   )");
    query.append("  .in('ManagerOf'){as: manager}");
    query.append("  return manager");
    query.append(")");

    final ResultSet qResult = database.query("sql", query.toString());
    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
    return item;
  }

  private Result getManager2Arrows(final String personName) {
    final StringBuilder query = new StringBuilder();
    query.append("select expand(manager) from (");
    query.append("  match {type:Employee, where: (name = '" + personName + "')}");
    query.append("   .( -WorksAt->{}-ParentDepartment->{");
    query.append("       while: (in('ManagerOf').size() == 0),");
    query.append("       where: (in('ManagerOf').size() > 0)");
    query.append("     }");
    query.append("   )<-ManagerOf-{as: manager}");
    query.append("  return manager");
    query.append(")");

    final ResultSet qResult = database.query("sql", query.toString());
    assertThat(qResult.hasNext()).isTrue();
    final Result item = qResult.next();
    assertThat(qResult.hasNext()).isFalse();
    qResult.close();
    return item;
  }

  @Test
  void managed() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    final ResultSet managedByA = getManagedBy("a");
    assertThat(managedByA.hasNext()).isTrue();
    final Result item = managedByA.next();
    assertThat(managedByA.hasNext()).isFalse();
    assertThat(item.<String>getProperty("name")).isEqualTo("p1");
    managedByA.close();

    final ResultSet managedByB = getManagedBy("b");

    final Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    final Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      assertThat(managedByB.hasNext()).isTrue();
      final Result id = managedByB.next();
      final String name = id.getProperty("name");
      names.add(name);
    }
    assertThat(names).isEqualTo(expectedNames);
    managedByB.close();
  }

  private ResultSet getManagedBy(final String managerName) {
    final StringBuilder query = new StringBuilder();
    query.append("select expand(managed) from (");
    query.append("  match {type:Employee, where: (name = '" + managerName + "')}");
    query.append("  .out('ManagerOf')");
    query.append("  .in('ParentDepartment'){");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }");
    query.append("  .in('WorksAt'){as: managed}");
    query.append("  return managed");
    query.append(")");

    return database.query("sql", query.toString());
  }

  @Test
  void managedArrows() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    final ResultSet managedByA = getManagedByArrows("a");
    assertThat(managedByA.hasNext()).isTrue();
    final Result item = managedByA.next();
    assertThat(managedByA.hasNext()).isFalse();
    assertThat(item.<String>getProperty("name")).isEqualTo("p1");
    managedByA.close();
    final ResultSet managedByB = getManagedByArrows("b");

    final Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    final Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      assertThat(managedByB.hasNext()).isTrue();
      final Result id = managedByB.next();
      final String name = id.getProperty("name");
      names.add(name);
    }
    assertThat(names).isEqualTo(expectedNames);
    managedByB.close();
  }

  private ResultSet getManagedByArrows(final String managerName) {
    final String query = """
        select expand(managed) from (
          match {type:Employee, where: (name = '%s')}
          -ManagerOf->{}<-ParentDepartment-{
              while: ($depth = 0 or in('ManagerOf').size() = 0),
              where: ($depth = 0 or in('ManagerOf').size() = 0)
          }<-WorksAt-{as: managed}
          return managed
        )
        """.formatted(managerName);

    return database.query("sql", query);
  }

  @Test
  void managed2() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    final ResultSet managedByA = getManagedBy2("a");
    assertThat(managedByA.hasNext()).isTrue();
    final Result item = managedByA.next();
    assertThat(managedByA.hasNext()).isFalse();
    assertThat(item.<String>getProperty("name")).isEqualTo("p1");
    managedByA.close();
    final ResultSet managedByB = getManagedBy2("b");

    final Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    final Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      assertThat(managedByB.hasNext()).isTrue();
      final Result id = managedByB.next();
      final String name = id.getProperty("name");
      names.add(name);
    }
    assertThat(names).isEqualTo(expectedNames);
    managedByB.close();
  }

  private ResultSet getManagedBy2(final String managerName) {
    final String query = """
        select expand(managed) from (
          match {type:Employee, where: (name = '%s')}
          .out('ManagerOf')
          .(inE('ParentDepartment').outV()){
              while: ($depth = 0 or in('ManagerOf').size() = 0),
              where: ($depth = 0 or in('ManagerOf').size() = 0)
          }
          .in('WorksAt'){as: managed}
          return managed
        )
        """.formatted(managerName);

    return database.query("sql", query);
  }

  @Test
  void managed2Arrows() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    final ResultSet managedByA = getManagedBy2Arrows("a");
    assertThat(managedByA.hasNext()).isTrue();
    final Result item = managedByA.next();
    assertThat(managedByA.hasNext()).isFalse();
    assertThat(item.<String>getProperty("name")).isEqualTo("p1");
    managedByA.close();
    final ResultSet managedByB = getManagedBy2Arrows("b");

    final Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    final Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      assertThat(managedByB.hasNext()).isTrue();
      final Result id = managedByB.next();
      final String name = id.getProperty("name");
      names.add(name);
    }
    assertThat(names).isEqualTo(expectedNames);
    managedByB.close();
  }

  private ResultSet getManagedBy2Arrows(final String managerName) {
    final String query = """
        select expand(managed) from (
          match {type:Employee, where: (name = '%s')}
          -ManagerOf->{}
          .(inE('ParentDepartment').outV()){
              while: ($depth = 0 or in('ManagerOf').size() = 0),
              where: ($depth = 0 or in('ManagerOf').size() = 0)
          }<-WorksAt-{as: managed}
          return managed
        )
        """.formatted(managerName);

    return database.query("sql", query);
  }

  @Test
  void triangle1() {
    final String query = """
        match {type:TriangleV, as: friend1, where: (uid = 0)}
          .out('TriangleE'){as: friend2}
          .out('TriangleE'){as: friend3},
        {type:TriangleV, as: friend1}
          .out('TriangleE'){as: friend3}
        return $matches
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void triangle1Arrows() {
    final String query = """
        match {type:TriangleV, as: friend1, where: (uid = 0)} -TriangleE-> {as: friend2} -TriangleE-> {as: friend3},
        {type:TriangleV, as: friend1} -TriangleE-> {as: friend3}
        return $matches
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void triangle2Old() {
    final String query = """
        match {type:TriangleV, as: friend1}
          .out('TriangleE'){type:TriangleV, as: friend2, where: (uid = 1)}
          .out('TriangleE'){as: friend3},
        {type:TriangleV, as: friend1}
          .out('TriangleE'){as: friend3}
        return $matches
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    final Document friend1 = doc.getProperty("friend1");
    final Document friend2 = doc.getProperty("friend2");
    final Document friend3 = doc.getProperty("friend3");
    assertThat(friend1.getInteger("uid")).isEqualTo(0);
    assertThat(friend2.getInteger("uid")).isEqualTo(1);
    assertThat(friend3.getInteger("uid")).isEqualTo(2);
    result.close();
  }

  @Test
  void triangle2() {
    final String query = """
        match {type:TriangleV, as: friend1}
          .out('TriangleE'){type:TriangleV, as: friend2, where: (uid = 1)}
          .out('TriangleE'){as: friend3},
        {type:TriangleV, as: friend1}
          .out('TriangleE'){as: friend3}
        return $patterns
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    final Document friend1 = doc.getProperty("friend1");
    final Document friend2 = doc.getProperty("friend2");
    final Document friend3 = doc.getProperty("friend3");
    assertThat(friend1.getInteger("uid")).isEqualTo(0);
    assertThat(friend2.getInteger("uid")).isEqualTo(1);
    assertThat(friend3.getInteger("uid")).isEqualTo(2);
    result.close();
  }

  @Test
  void triangle2Arrows() {
    final String query = """
        match {type:TriangleV, as: friend1}
          -TriangleE->{type:TriangleV, as: friend2, where: (uid = 1)}
          -TriangleE->{as: friend3},
        {type:TriangleV, as: friend1}
          -TriangleE->{as: friend3}
        return $matches
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    final Document friend1 = doc.getProperty("friend1");
    final Document friend2 = doc.getProperty("friend2");
    final Document friend3 = doc.getProperty("friend3");
    assertThat(friend1.getInteger("uid")).isEqualTo(0);
    assertThat(friend2.getInteger("uid")).isEqualTo(1);
    assertThat(friend3.getInteger("uid")).isEqualTo(2);
    result.close();
  }

  @Test
  void triangle3() {
    final String query = """
        match {type:TriangleV, as: friend1}
          -TriangleE->{as: friend2}
          -TriangleE->{as: friend3, where: (uid = 2)},
        {type:TriangleV, as: friend1}
          -TriangleE->{as: friend3}
        return $matches
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void triangle4() {
    final String query = """
        match {type:TriangleV, as: friend1}
          .out('TriangleE'){as: friend2, where: (uid = 1)}
          .out('TriangleE'){as: friend3},
        {type:TriangleV, as: friend1}
          .out('TriangleE'){as: friend3}
        return $matches
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void triangle4Arrows() {
    final String query = """
        match {type:TriangleV, as: friend1}
          -TriangleE->{as: friend2, where: (uid = 1)}
          -TriangleE->{as: friend3},
        {type:TriangleV, as: friend1}
          -TriangleE->{as: friend3}
        return $matches
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void triangleWithEdges4() {
    final String query = """
        match {type:TriangleV, as: friend1}
          .outE('TriangleE').inV(){as: friend2, where: (uid = 1)}
          .outE('TriangleE').inV(){as: friend3},
        {type:TriangleV, as: friend1}
          .outE('TriangleE').inV(){as: friend3}
        return $matches
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void cartesianProduct() {
    final String query = """
        match {type:TriangleV, as: friend1, where:(uid = 1)},
        {type:TriangleV, as: friend2, where:(uid = 2 or uid = 3)}
        return $matches
        """;

    final ResultSet result = database.query("sql", query);
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result doc = result.next();
      final Vertex friend1 = doc.getProperty("friend1");
      assertThat(friend1.getInteger("uid")).isEqualTo(1);
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void noPrefetch() {
    final String query = """
        match {type:IndexedVertex, as: one}
        return $patterns
        """;

    final ResultSet result = database.query("sql", query);
    result.getExecutionPlan()
        .ifPresent(x -> x.getSteps().stream().filter(y -> y instanceof MatchPrefetchStep).forEach(prefetchStepFound -> fail()));

    for (int i = 0; i < 1000; i++) {
      assertThat(result.hasNext()).isTrue();
      result.next();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void cartesianProductLimit() {
    final String query = """
        match {type:TriangleV, as: friend1, where:(uid = 1)},
        {type:TriangleV, as: friend2, where:(uid = 2 or uid = 3)}
        return $matches LIMIT 1
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result d = result.next();
    final Document friend1 = d.getProperty("friend1");
    assertThat(friend1.getInteger("uid")).isEqualTo(1);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void arrayNumber() {
    final StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0] as foo");

    final ResultSet result = database.query("sql", query.toString());

    assertThat(result.hasNext()).isTrue();

    final Result doc = result.next();
    final Object foo = doc.getProperty("foo");
    assertThat(foo).isNotNull();
    assertThat(foo instanceof Vertex).isTrue();
    result.close();
  }

  @Test
  void arraySingleSelectors2() {
    final StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0,1] as foo");

    final ResultSet result = database.query("sql", query.toString());
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    final Object foo = doc.getProperty("foo");
    assertThat(foo).isNotNull();
    assertThat(foo instanceof List).isTrue();
    assertThat(((List) foo).size()).isEqualTo(2);
    result.close();
  }

  @Test
  void arrayRangeSelectors1() {
    final StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0..1] as foo");

    final ResultSet result = database.query("sql", query.toString());
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();

    final Object foo = doc.getProperty("foo");
    assertThat(foo).isNotNull();
    assertThat(foo instanceof List).isTrue();
    assertThat(((List) foo).size()).isEqualTo(1);
    result.close();
  }

  @Test
  void arrayRange2() {
    final String query = """
        match {type:TriangleV, as: friend1, where: (uid = 0)}
        return friend1.out('TriangleE')[0..2] as foo
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();

    final Object foo = doc.getProperty("foo");
    assertThat(foo).isNotNull();
    assertThat(foo instanceof List).isTrue();
    assertThat(((List) foo).size()).isEqualTo(2);
    result.close();
  }

  @Test
  void arrayRange3() {
    final String query = """
        match {type:TriangleV, as: friend1, where: (uid = 0)}
        return friend1.out('TriangleE')[0..3] as foo
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();

    final Object foo = doc.getProperty("foo");
    assertThat(foo).isNotNull();
    assertThat(foo instanceof List).isTrue();
    assertThat(((List) foo).size()).isEqualTo(2);
    result.close();
  }

  @Test
  void conditionInSquareBrackets() {
    final String query = """
        match {type:TriangleV, as: friend1, where: (uid = 0)}
        return friend1.out('TriangleE')[uid = 2] as foo
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();

    final Object foo = doc.getProperty("foo");
    assertThat(foo).isNotNull();
    assertThat(foo instanceof List).isTrue();
    assertThat(((List) foo).size()).isEqualTo(1);
    final Vertex resultVertex = (Vertex) ((List) foo).getFirst();
    assertThat(resultVertex.getInteger("uid")).isEqualTo(2);
    result.close();
  }

  @Test
  void indexedEdge() {
    final String query = """
        match {type:IndexedVertex, as: one, where: (uid = 0)}
        .out('IndexedEdge'){type:IndexedVertex, as: two, where: (uid = 1)}
        return one, two
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void indexedEdgeArrows() {
    final String query = """
        match {type:IndexedVertex, as: one, where: (uid = 0)}
        -IndexedEdge->{type:IndexedVertex, as: two, where: (uid = 1)}
        return one, two
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void json() {
    final String query = """
        match {type:IndexedVertex, as: one, where: (uid = 0)}
        return {'name':'foo', 'uuid':one.uid}""";

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();

    //    Document doc = result.get(0);
    //    assertEquals("foo", doc.set("name");
    //    assertEquals(0, doc.set("uuid");
    result.close();
  }

  @Test
  void json2() {
    final String query = """
        match {type:IndexedVertex, as: one, where: (uid = 0)}
        return {'name':'foo', 'sub': {'uuid':one.uid}}
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    //    Document doc = result.get(0);
    //    assertEquals("foo", doc.set("name");
    //    assertEquals(0, doc.set("sub.uuid");
    result.close();
  }

  @Test
  void json3() {
    final String query = """
        match {type:IndexedVertex, as: one, where: (uid = 0)}
        return {'name':'foo', 'sub': [{'uuid':one.uid}]}
        """;

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result doc = result.next();
    assertThat(result.hasNext()).isFalse();
    //    Document doc = result.get(0);
    //    assertEquals("foo", doc.set("name");
    //    assertEquals(0, doc.set("sub[0].uuid");

    result.close();
  }

  @Test
  void unique() {
    String query = """
        match {type:DiamondV, as: one, where: (uid = 0)}
        .out('DiamondE').out('DiamondE'){as: two}
        return DISTINCT one, two
        """;

    ResultSet result = database.query("sql", query);

    assertThat(result.hasNext()).isTrue();
    Result doc = result.next();
    assertThat(result.hasNext()).isFalse();

    query = """
        match {type:DiamondV, as: one, where: (uid = 0)}
        .out('DiamondE').out('DiamondE'){as: two}
        return DISTINCT one.uid, two.uid
        """;

    result.close();

    result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    doc = result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void notUnique() {
    String query = """
        match {type:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two}
        return one, two
        """;

    ResultSet result = database.query("sql", query);

    assertThat(result.hasNext()).isTrue();
    Result doc = result.next();
    assertThat(result.hasNext()).isTrue();
    doc = result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();

    query = """
        match {type:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two}
        return one.uid, two.uid
        """;

    result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    doc = result.next();
    assertThat(result.hasNext()).isTrue();
    doc = result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void managedElements() {
    final ResultSet managedByB = getManagedElements("b");

    final Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("b");
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    final Set<String> names = new HashSet<String>();
    for (int i = 0; i < 6; i++) {
      assertThat(managedByB.hasNext()).isTrue();
      final Result doc = managedByB.next();
      final String name = doc.getProperty("name");
      names.add(name);
    }
    assertThat(managedByB.hasNext()).isFalse();
    assertThat(names).isEqualTo(expectedNames);
    managedByB.close();
  }

  private ResultSet getManagedElements(final String managerName) {
    final String query = """
        match {type:Employee, as:boss, where: (name = '%s')}
        -ManagerOf->{}<-ParentDepartment-{
            while: ($depth = 0 or in('ManagerOf').size() = 0),
            where: ($depth = 0 or in('ManagerOf').size() = 0)
        }<-WorksAt-{as: managed}
        return distinct $elements
        """.formatted(managerName);

    return database.query("sql", query);
  }

  @Test
  void managedPathElements() {
    final ResultSet managedByB = getManagedPathElements("b");

    final Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("department1");
    expectedNames.add("department3");
    expectedNames.add("department4");
    expectedNames.add("department8");
    expectedNames.add("b");
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    final Set<String> names = new HashSet<String>();
    for (int i = 0; i < 10; i++) {
      assertThat(managedByB.hasNext()).isTrue();
      final Result doc = managedByB.next();
      final String name = doc.getProperty("name");
      names.add(name);
    }
    assertThat(managedByB.hasNext()).isFalse();
    assertThat(names).isEqualTo(expectedNames);
    managedByB.close();
  }

  @Test
  void optional() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, as: person} -NonExistingEdge-> {as:b, optional:true} return person, b.name");

    for (int i = 0; i < 6; i++) {
      assertThat(qResult.hasNext()).isTrue();
      final Result doc = qResult.next();
      assertThat(doc.getPropertyNames().size()).isEqualTo(2);
      final Vertex person = doc.getProperty("person");

      final String name = person.getString("name");
      assertThat(name.startsWith("n")).isTrue();
    }
  }

  @Test
  void optional2() {
    final ResultSet qResult = database.query("sql",
        "match {type:Person, as: person} --> {as:b, optional:true, where:(nonExisting = 12)} return person, b.name");

    for (int i = 0; i < 6; i++) {
      assertThat(qResult.hasNext()).isTrue();
      final Result doc = qResult.next();
      assertThat(doc.getPropertyNames().size()).isEqualTo(2);
      final Vertex person = doc.getProperty("person");

      final String name = person.getString("name");
      assertThat(name.startsWith("n")).isTrue();
    }
  }

  @Test
  void optional3() {
    final ResultSet qResult = database.query("sql", """
        select friend.name as name, b from (
        match {type:Person, as:a, where:(name = 'n1' and 1 + 1 = 2)}.out('Friend'){as:friend, where:(name = 'n2' and 1 + 1 = 2)},
        {as:a}.out(){as:b, where:(nonExisting = 12), optional:true},
        {as:friend}.out(){as:b, optional:true}
        return friend, b)
        """);
    assertThat(qResult.hasNext()).isTrue();
    final Result doc = qResult.next();
    assertThat(doc.<String>getProperty("name")).isEqualTo("n2");
    assertThat(doc.<String>getProperty("b")).isNull();
    assertThat(qResult.hasNext()).isFalse();
  }

  @Test
  void orderByAsc() {
    database.command("sql", "CREATE vertex type testOrderByAsc ");

    database.command("sql", "CREATE VERTEX testOrderByAsc SET name = 'bbb'");
    database.command("sql", "CREATE VERTEX testOrderByAsc SET name = 'zzz'");
    database.command("sql", "CREATE VERTEX testOrderByAsc SET name = 'aaa'");
    database.command("sql", "CREATE VERTEX testOrderByAsc SET name = 'ccc'");

    final String query = "MATCH { type: testOrderByAsc, as:a} RETURN a.name as name order by name asc";

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("aaa");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("bbb");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("ccc");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("zzz");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void orderByDesc() {
    database.command("sql", "CREATE vertex type testOrderByDesc");

    database.command("sql", "CREATE VERTEX testOrderByDesc SET name = 'bbb'");
    database.command("sql", "CREATE VERTEX testOrderByDesc SET name = 'zzz'");
    database.command("sql", "CREATE VERTEX testOrderByDesc SET name = 'aaa'");
    database.command("sql", "CREATE VERTEX testOrderByDesc SET name = 'ccc'");

    final String query = "MATCH { type: testOrderByDesc, as:a} RETURN a.name as name order by name desc";

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("zzz");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("ccc");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("bbb");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("name")).isEqualTo("aaa");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void nestedProjections() {
    final String clazz = "testNestedProjections";
    database.command("sql", "CREATE vertex type " + clazz + " ");

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', surname = 'ccc'");

    final String query = "MATCH { type: " + clazz + ", as:a} RETURN a:{name}, 'x' ";

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    final Result a = item.getProperty("a");
    assertThat(a.<String>getProperty("name")).isEqualTo("bbb");
    assertThat(a.<String>getProperty("surname")).isNull();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void nestedProjectionsStar() {
    final String clazz = "testNestedProjectionsStar";
    database.command("sql", "CREATE vertex type " + clazz + " ");

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', surname = 'ccc'");

    if (database.isTransactionActive()) {
      database.commit();
      database.begin();
    }

    final String query = "MATCH { type: " + clazz + ", as:a} RETURN a:{*, @rid}, 'x' ";

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    final Result a = item.getProperty("a");
    assertThat(a.<String>getProperty("name")).isEqualTo("bbb");
    assertThat(a.<String>getProperty("surname")).isEqualTo("ccc");
    assertThat(a.<RID>getProperty(RID_PROPERTY)).isNotNull();
    assertThat(a.getPropertyNames().size()).isEqualTo(4);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void expand() {
    final String clazz = "testExpand";
    database.command("sql", "CREATE vertex type " + clazz + " ");

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', surname = 'ccc'");

    if (database.isTransactionActive()) {
      database.commit();
      database.begin();
    }

    final String query = "MATCH { type: " + clazz + ", as:a} RETURN expand(a) ";

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    final Result a = result.next();
    assertThat(a.<String>getProperty("name")).isEqualTo("bbb");
    assertThat(a.<String>getProperty("surname")).isEqualTo("ccc");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void aggregate() {
    final String clazz = "testAggregate";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 1");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 2");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 3");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', num = 4");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', num = 5");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', num = 6");

    final String query =
        "MATCH { type: " + clazz + ", as:a} RETURN a.name as a, max(a.num) as maxNum group by a.name order by a.name";

    final ResultSet result = database.query("sql", query);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item.<String>getProperty("a")).isEqualTo("aaa");
    assertThat((int) item.getProperty("maxNum")).isEqualTo(3);

    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item.<String>getProperty("a")).isEqualTo("bbb");
    assertThat((int) item.getProperty("maxNum")).isEqualTo(6);

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void orderByOutOfProjAsc() {
    final String clazz = "testOrderByOutOfProjAsc";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 0, num2 = 1");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 1, num2 = 2");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 2, num2 = 3");

    final String query = "MATCH { type: " + clazz + ", as:a} RETURN a.name as name, a.num as num order by a.num2 asc";

    final ResultSet result = database.query("sql", query);
    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item.<String>getProperty("name")).isEqualTo("aaa");
      assertThat((int) item.getProperty("num")).isEqualTo(i);
    }

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void orderByOutOfProjDesc() {
    final String clazz = "testOrderByOutOfProjDesc";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 0, num2 = 1");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 1, num2 = 2");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 2, num2 = 3");

    final String query = "MATCH { type: " + clazz + ", as:a} RETURN a.name as name, a.num as num order by a.num2 desc";

    final ResultSet result = database.query("sql", query);

    for (int i = 2; i >= 0; i--) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item.<String>getProperty("name")).isEqualTo("aaa");
      assertThat((int) item.getProperty("num")).isEqualTo(i);
    }

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void unwind() {
    final String clazz = "testUnwind";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', coll = [1, 2]");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', coll = [3, 4]");

    final String query = "MATCH { type: " + clazz + ", as:a} RETURN a.name as name, a.coll as num unwind num";

    int sum = 0;
    final ResultSet result = database.query("sql", query);
    for (int i = 0; i < 4; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      sum += item.<Integer>getProperty("num");
    }

    assertThat(result.hasNext()).isFalse();

    result.close();
    assertThat(sum).isEqualTo(10);
  }

  @Test
  void skip() {
    final String clazz = "testSkip";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ccc'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ddd'");

    final String query = "MATCH { type: " + clazz + ", as:a} RETURN a.name as name ORDER BY name ASC skip 1 limit 2";

    final ResultSet result = database.query("sql", query);

    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("bbb");

    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("ccc");

    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void depthAlias() {
    final String clazz = "testDepthAlias";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ccc'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ddd'");

    database.command("sql",
        "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'aaa') TO (SELECT FROM " + clazz + " WHERE name = 'bbb')");
    database.command("sql",
        "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'bbb') TO (SELECT FROM " + clazz + " WHERE name = 'ccc')");
    database.command("sql",
        "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'ccc') TO (SELECT FROM " + clazz + " WHERE name = 'ddd')");

    final String query = "MATCH { type: " + clazz
        + ", as:a, where:(name = 'aaa')} --> {as:b, while:($depth<10), depthAlias: xy} RETURN a.name as name, b.name as bname, xy";

    final ResultSet result = database.query("sql", query);

    int sum = 0;
    for (int i = 0; i < 4; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      final Object depth = item.getProperty("xy");
      assertThat(depth instanceof Integer).isTrue();
      assertThat(item.<String>getProperty("name")).isEqualTo("aaa");
      switch ((int) depth) {
      case 0:
        assertThat(item.<String>getProperty("bname")).isEqualTo("aaa");
        break;
      case 1:
        assertThat(item.<String>getProperty("bname")).isEqualTo("bbb");
        break;
      case 2:
        assertThat(item.<String>getProperty("bname")).isEqualTo("ccc");
        break;
      case 3:
        assertThat(item.<String>getProperty("bname")).isEqualTo("ddd");
        break;
      default:
        fail("");
      }
      sum += (int) depth;
    }
    assertThat(sum).isEqualTo(6);
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void pathAlias() {
    final String clazz = "testPathAlias";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ccc'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ddd'");

    database.command("sql",
        "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'aaa') TO (SELECT FROM " + clazz + " WHERE name = 'bbb')");
    database.command("sql",
        "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'bbb') TO (SELECT FROM " + clazz + " WHERE name = 'ccc')");
    database.command("sql",
        "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'ccc') TO (SELECT FROM " + clazz + " WHERE name = 'ddd')");

    final String query = "MATCH { type: " + clazz
        + ", as:a, where:(name = 'aaa')} --> {as:b, while:($depth<10), pathAlias: xy} RETURN a.name as name, b.name as bname, xy";

    final ResultSet result = database.query("sql", query);

    for (int i = 0; i < 4; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      final Object path = item.getProperty("xy");
      assertThat(path instanceof List).isTrue();
      final List<Identifiable> thePath = (List<Identifiable>) path;

      final String bname = item.getProperty("bname");
      switch (bname) {
      case "aaa" -> assertThat(thePath.size()).isEqualTo(0);
      case "bbb" -> {
        assertThat(thePath.size()).isEqualTo(1);
        assertThat(thePath.getFirst().getRecord().asDocument().getString("name")).isEqualTo("bbb");
      }
      case "ccc" -> {
        assertThat(thePath.size()).isEqualTo(2);
        assertThat(thePath.getFirst().getRecord().asDocument().getString("name")).isEqualTo("bbb");
        assertThat(thePath.get(1).getRecord().asDocument().getString("name")).isEqualTo("ccc");
      }
      case "ddd" -> {
        assertThat(thePath.size()).isEqualTo(3);
        assertThat(thePath.getFirst().getRecord().asDocument().getString("name")).isEqualTo("bbb");
        assertThat(thePath.get(1).getRecord().asDocument().getString("name")).isEqualTo("ccc");
        assertThat(thePath.get(2).getRecord().asDocument().getString("name")).isEqualTo("ddd");
      }
      }
    }
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void bucketTarget() {
    final String clazz = "testBucketTarget";
    database.command("SQL", "CREATE vertex type " + clazz).close();
    database.command("SQL", "CREATE property " + clazz + ".name STRING").close();
    database.command("SQL", "CREATE index on " + clazz + " (name) unique").close();

    database.command("SQL", "ALTER TYPE " + clazz + " BUCKET +" + clazz + "_one").close();
    database.command("SQL", "ALTER TYPE " + clazz + " BUCKET +" + clazz + "_two").close();
    database.command("SQL", "ALTER TYPE " + clazz + " BUCKET +" + clazz + "_three").close();

    final MutableVertex v1 = database.newVertex(clazz);
    v1.set("name", "one");
    v1.save(clazz + "_one");

    final MutableVertex vx = database.newVertex(clazz);
    vx.set("name", "onex");
    vx.save(clazz + "_one");

    final MutableVertex v2 = database.newVertex(clazz);
    v2.set("name", "two");
    v2.save(clazz + "_two");

    final MutableVertex v3 = database.newVertex(clazz);
    v3.set("name", "three");
    v3.save(clazz + "_three");

    v1.newEdge("Friend", v2).save();
    v2.newEdge("Friend", v3).save();
    v1.newEdge("Friend", v3).save();

    final String query =
        "MATCH { bucket: " + clazz + "_one, as:a} --> {as:b, bucket:" + clazz + "_two} RETURN a.name as aname, b.name as bname";

    final ResultSet result = database.query("SQL", query);

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("aname")).isEqualTo("one");
    assertThat(item.<String>getProperty("bname")).isEqualTo("two");

    assertThat(result.hasNext()).isFalse();

    assertThat(database.getSchema().getIndexByName(clazz + "[name]").get(new String[] { "one" }).hasNext()).isTrue();
    assertThat(database.getSchema().getIndexByName(clazz + "[name]").get(new String[] { "onex" }).hasNext()).isTrue();
    assertThat(database.getSchema().getIndexByName(clazz + "[name]").get(new String[] { "two" }).hasNext()).isTrue();
    assertThat(database.getSchema().getIndexByName(clazz + "[name]").get(new String[] { "three" }).hasNext()).isTrue();

    //--------------------------------------------------------------------------------------------------------
    // CHECK THE SUB-INDEX EXISTS
    assertThat(Set.of(((TypeIndex) database.getSchema().getIndexByName(clazz + "[name]")).getIndexesOnBuckets()).stream()
        .map(Index::getAssociatedBucketId).collect(Collectors.toSet())
        .contains(database.getSchema().getBucketByName(clazz + "_one").getFileId())).isTrue();

    database.command("SQL", "ALTER TYPE " + clazz + " BUCKET -" + clazz + "_one").close();

    // CHECK THE SUB-INDEX HAS BEN REMOVED
    assertThat(Set.of(((TypeIndex) database.getSchema().getIndexByName(clazz + "[name]")).getIndexesOnBuckets()).stream()
        .map(Index::getAssociatedBucketId).collect(Collectors.toSet())
        .contains(database.getSchema().getBucketByName(clazz + "_one").getFileId())).isFalse();

    //--------------------------------------------------------------------------------------------------------
    // CHECK THE SUB-INDEX EXISTS
    assertThat(Set.of(((TypeIndex) database.getSchema().getIndexByName(clazz + "[name]")).getIndexesOnBuckets()).stream()
        .map(Index::getAssociatedBucketId).collect(Collectors.toSet())
        .contains(database.getSchema().getBucketByName(clazz + "_two").getFileId())).isTrue();

    database.command("SQL", "ALTER TYPE " + clazz + " BUCKET -" + clazz + "_two").close();

    // CHECK THE SUB-INDEX HAS BEN REMOVED
    assertThat(Set.of(((TypeIndex) database.getSchema().getIndexByName(clazz + "[name]")).getIndexesOnBuckets()).stream()
        .map(Index::getAssociatedBucketId).collect(Collectors.toSet())
        .contains(database.getSchema().getBucketByName(clazz + "_two").getFileId())).isFalse();

    //--------------------------------------------------------------------------------------------------------
    // CHECK THE SUB-INDEX EXISTS
    assertThat(Set.of(((TypeIndex) database.getSchema().getIndexByName(clazz + "[name]")).getIndexesOnBuckets()).stream()
        .map(Index::getAssociatedBucketId).collect(Collectors.toSet())
        .contains(database.getSchema().getBucketByName(clazz + "_three").getFileId())).isTrue();

    database.command("SQL", "ALTER TYPE " + clazz + " BUCKET -" + clazz + "_three").close();

    // CHECK THE SUB-INDEX HAS BEN REMOVED
    assertThat(Set.of(((TypeIndex) database.getSchema().getIndexByName(clazz + "[name]")).getIndexesOnBuckets()).stream()
        .map(Index::getAssociatedBucketId).collect(Collectors.toSet())
        .contains(database.getSchema().getBucketByName(clazz + "_three").getFileId())).isFalse();

    result.close();
  }

  @Test
  void negativePattern() {
    final String clazz = "testNegativePattern";
    database.command("SQL", "CREATE vertex type " + clazz).close();

    final MutableVertex v1 = database.newVertex(clazz);
    v1.set("name", "a");
    v1.save();

    final MutableVertex v2 = database.newVertex(clazz);
    v2.set("name", "b");
    v2.save();

    final MutableVertex v3 = database.newVertex(clazz);
    v3.set("name", "c");
    v3.save();

    v1.newEdge("Friend", v2).save();
    v2.newEdge("Friend", v3).save();

    String query = "MATCH { type:" + clazz + ", as:a} --> {as:b} --> {as:c}, ";
    query += " NOT {as:a} --> {as:c}";
    query += " RETURN $patterns";

    final ResultSet result = database.query("SQL", query);
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void negativePattern2() {
    final String clazz = "testNegativePattern2";
    database.command("SQL", "CREATE vertex type " + clazz).close();

    final MutableVertex v1 = database.newVertex(clazz);
    v1.set("name", "a");
    v1.save();

    final MutableVertex v2 = database.newVertex(clazz);
    v2.set("name", "b");
    v2.save();

    final MutableVertex v3 = database.newVertex(clazz);
    v3.set("name", "c");
    v3.save();

    v1.newEdge("Friend", v2).save();
    v2.newEdge("Friend", v3).save();
    v1.newEdge("Friend", v3).save();

    String query = "MATCH { type:" + clazz + ", as:a} --> {as:b} --> {as:c}, ";
    query += " NOT {as:a} --> {as:c}";
    query += " RETURN $patterns";

    final ResultSet result = database.query("SQL", query);
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void negativePattern3() {
    final String clazz = "testNegativePattern3";
    database.command("SQL", "CREATE vertex type " + clazz).close();

    final MutableVertex v1 = database.newVertex(clazz);
    v1.set("name", "a");
    v1.save();

    final MutableVertex v2 = database.newVertex(clazz);
    v2.set("name", "b");
    v2.save();

    final MutableVertex v3 = database.newVertex(clazz);
    v3.set("name", "c");
    v3.save();

    v1.newEdge("Friend", v2).save();
    v2.newEdge("Friend", v3).save();
    v1.newEdge("Friend", v3).save();

    String query = "MATCH { type:" + clazz + ", as:a} --> {as:b} --> {as:c}, ";
    query += " NOT {as:a} --> {as:c, where:(name <> 'c')}";
    query += " RETURN $patterns";

    final ResultSet result = database.query("SQL", query);
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void pathTraversal() {
    final String clazz = "testPathTraversal";
    database.command("SQL", "CREATE vertex type " + clazz).close();

    final MutableVertex v1 = database.newVertex(clazz);
    v1.set("name", "a");
    v1.save();

    final MutableVertex v2 = database.newVertex(clazz);
    v2.set("name", "b");
    v2.save();

    final MutableVertex v3 = database.newVertex(clazz);
    v3.set("name", "c");
    v3.save();

    v1.set("next", v2);
    v2.set("next", v3);

    v1.save();
    v2.save();

    String query = "MATCH { type:" + clazz + ", as:a}.next{as:b, where:(name ='b')}";
    query += " RETURN a.name as a, b.name as b";

    ResultSet result = database.query("SQL", query);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item.<String>getProperty("a")).isEqualTo("a");
    assertThat(item.<String>getProperty("b")).isEqualTo("b");

    assertThat(result.hasNext()).isFalse();

    result.close();

    query = "MATCH { type:" + clazz + ", as:a, where:(name ='a')}.next{as:b}";
    query += " RETURN a.name as a, b.name as b";

    result = database.query("SQL", query);
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item.<String>getProperty("a")).isEqualTo("a");
    assertThat(item.<String>getProperty("b")).isEqualTo("b");

    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  private ResultSet getManagedPathElements(final String managerName) {
    final String query = """
        match {type:Employee, as:boss, where: (name = '%s')}
        -ManagerOf->{}<-ParentDepartment-{
            while: ($depth = 0 or in('ManagerOf').size() = 0),
            where: ($depth = 0 or in('ManagerOf').size() = 0)
        }<-WorksAt-{as: managed}
        return distinct $pathElements
        """.formatted(managerName);

    return database.query("sql", query);
  }

  @Test
  void quotedClassName() {
    final String className = "testQuotedClassName";
    database.command("sql", "CREATE vertex type " + className);
    database.command("sql", "CREATE VERTEX " + className + " SET name = 'a'");

    final String query = "MATCH {type: `" + className + "`, as:foo} RETURN $elements";

    try (final ResultSet rs = database.query("SQL", query)) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }

  @Test
  void matchInSubQuery() {
    try (final ResultSet rs = database.query("SQL", "SELECT $a LET $a=(MATCH{type:Person,as:Person_0}RETURN expand(Person_0))")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }
}
