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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.MatchPrefetchStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.fail;

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

    String[][] friendList = new String[][] { { "n1", "n2" }, { "n1", "n3" }, { "n2", "n4" }, { "n4", "n5" }, { "n4", "n6" } };

    for (String[] pair : friendList) {
      database.command("sql", "CREATE EDGE Friend from (select from Person where name = ?) to (select from Person where name = ?)", pair[0], pair[1]);
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

  private static void initEdgeIndexTest(Database database) {
    database.command("sql", "CREATE vertex type IndexedVertex");
    database.command("sql", "CREATE property IndexedVertex.uid INTEGER");
    database.command("sql", "CREATE index on IndexedVertex (uid) NOTUNIQUE");

    int nodes = 1000;
    for (int i = 0; i < nodes; i++) {
      MutableDocument doc = database.newVertex("IndexedVertex");
      doc.set("uid", i);
      doc.save();
    }

    database.command("sql", "CREATE edge type IndexedEdge");

    for (int i = 0; i < 100; i++) {
      String cmd =
          "CREATE EDGE IndexedEdge FROM (SELECT FROM IndexedVertex WHERE uid = 0) TO (SELECT FROM IndexedVertex WHERE uid > " + (i * nodes / 100) + " and uid <"
              + ((i + 1) * nodes / 100) + ")";
      database.command("sql", cmd);
    }

//    database.query("sql", "select expand(out()) from IndexedVertex where uid = 0").stream().forEach(x -> System.out.println("x = " + x));
  }

  private static void initOrgChart(Database database) {

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

    int[][] deptHierarchy = new int[10][];
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

    String[] deptManagers = { "a", "b", "d", null, null, null, null, "c", null, null };

    String[][] employees = new String[10][];
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
      int[] children = deptHierarchy[parent];
      for (int child : children) {
        database.command("sql", "CREATE EDGE ParentDepartment from (select from Department where name = 'department" + child
            + "') to (select from Department where name = 'department" + parent + "') ");
      }
    }

    for (int dept = 0; dept < deptManagers.length; dept++) {
      String manager = deptManagers[dept];
      if (manager != null) {
        database.command("sql", "CREATE Vertex Employee set name = '" + manager + "' ");

        database.command("sql",
            "CREATE EDGE ManagerOf from (select from Employee where name = '" + manager + "" + "') to (select from Department where name = 'department" + dept
                + "') ");
      }
    }

    for (int dept = 0; dept < employees.length; dept++) {
      String[] employeesForDept = employees[dept];
      for (String employee : employeesForDept) {
        database.command("sql", "CREATE Vertex Employee set name = '" + employee + "' ");

        database.command("sql",
            "CREATE EDGE WorksAt from (select from Employee where name = '" + employee + "" + "') to (select from Department where name = 'department" + dept
                + "') ");
      }
    }
  }

  private static void initTriangleTest(Database database) {
    database.command("sql", "CREATE vertex type TriangleV");
    database.command("sql", "CREATE property TriangleV.uid INTEGER");
    database.command("sql", "CREATE index on TriangleV (uid) UNIQUE");
    database.command("sql", "CREATE edge type TriangleE");
    for (int i = 0; i < 10; i++) {
      database.command("sql", "CREATE VERTEX TriangleV set uid = ?", i);
    }
    int[][] edges = { { 0, 1 }, { 0, 2 }, { 1, 2 }, { 1, 3 }, { 2, 4 }, { 3, 4 }, { 3, 5 }, { 4, 0 }, { 4, 7 }, { 6, 7 }, { 7, 8 }, { 7, 9 }, { 8, 9 },
        { 9, 1 }, { 8, 3 }, { 8, 4 } };
    for (int[] edge : edges) {
      database.command("sql", "CREATE EDGE TriangleE from (select from TriangleV where uid = ?) to (select from TriangleV where uid = ?)", edge[0], edge[1]);
    }
  }

  private static void initDiamondTest(Database database) {
    database.command("sql", "CREATE vertex type DiamondV");
    database.command("sql", "CREATE edge type DiamondE");
    for (int i = 0; i < 4; i++) {
      database.command("sql", "CREATE VERTEX DiamondV set uid = ?", i);
    }
    int[][] edges = { { 0, 1 }, { 0, 2 }, { 1, 3 }, { 2, 3 } };
    for (int[] edge : edges) {
      database.command("sql", "CREATE EDGE DiamondE from (select from DiamondV where uid = ?) to (select from DiamondV where uid = ?)", edge[0], edge[1]);
    }
  }

  @Test
  public void testSimple() {
    ResultSet qResult = database.query("sql", "match {type:Person, as: person} return person");
    printExecutionPlan(qResult);

    for (int i = 0; i < 6; i++) {
      Result item = qResult.next();
      Assertions.assertEquals(1, item.getPropertyNames().size());
      Document person = item.getProperty("person");

      String name = person.getString("name");
      Assertions.assertTrue(name.startsWith("n"));
    }
    qResult.close();
  }

  @Test
  public void testSimpleWhere() {
    ResultSet qResult = database.query("sql", "match {type:Person, as: person, where: (name = 'n1' or name = 'n2')} return person");

    for (int i = 0; i < 2; i++) {
      Result item = qResult.next();
      Assertions.assertTrue(item.getPropertyNames().size() == 1);
      Document personId = item.getProperty("person");

      MutableDocument person = personId.getRecord().asVertex().modify();
      String name = person.getString("name");
      Assertions.assertTrue(name.equals("n1") || name.equals("n2"));
    }
    qResult.close();
  }

  @Test
  public void testSimpleLimit() {
    ResultSet qResult = database.query("sql", "match {type:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit 1");
    Assertions.assertTrue(qResult.hasNext());
    qResult.next();
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testSimpleLimit2() {
    ResultSet qResult = database.query("sql", "match {type:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit -1");
    for (int i = 0; i < 2; i++) {
      Assertions.assertTrue(qResult.hasNext());
      qResult.next();
    }
    qResult.close();
  }

  @Test
  public void testSimpleLimit3() {

    ResultSet qResult = database.query("sql", "match {type:Person, as: person, where: (name = 'n1' or name = 'n2')} return person limit 3");
    for (int i = 0; i < 2; i++) {
      Assertions.assertTrue(qResult.hasNext());
      qResult.next();
    }
    qResult.close();
  }

  @Test
  public void testSimpleUnnamedParams() {
    ResultSet qResult = database.query("sql", "match {type:Person, as: person, where: (name = ? or name = ?)} return person", "n1", "n2");

    printExecutionPlan(qResult);
    for (int i = 0; i < 2; i++) {

      Result item = qResult.next();
      Assertions.assertEquals(1, item.getPropertyNames().size());
      Document person = item.getProperty("person");

      String name = person.getString("name");
      Assertions.assertTrue(name.equals("n1") || name.equals("n2"));
    }
    qResult.close();
  }

  @Test
  public void testCommonFriends() {

    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend)");
    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testCommonFriendsPatterns() {

    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $patterns)");
    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testPattens() {
    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $patterns");
    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals(1, item.getPropertyNames().size());
    Assertions.assertEquals("friend", item.getPropertyNames().iterator().next());
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testPaths() {
    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $paths");
    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals(3, item.getPropertyNames().size());
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testElements() {
    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $elements");
    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testPathElements() {
    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $pathElements");
    printExecutionPlan(qResult);
    Set<String> expected = new HashSet<>();
    expected.add("n1");
    expected.add("n2");
    expected.add("n4");
    for (int i = 0; i < 3; i++) {
      Assertions.assertTrue(qResult.hasNext());
      Result item = qResult.next();
      expected.remove(item.getProperty("name"));
    }
    Assertions.assertFalse(qResult.hasNext());
    Assertions.assertTrue(expected.isEmpty());
    qResult.close();
  }

  @Test
  public void testCommonFriendsMatches() {

    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return $matches)");
    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testCommonFriendsArrows() {

    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return friend)");
    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testCommonFriendsArrowsPatterns() {

    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return $patterns)");
    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testCommonFriends2() {

    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend.name as name");

    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testCommonFriends2Arrows() {

    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return friend.name as name");

    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnMethod() {
    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend.name.toUpperCase(Locale.ENGLISH) as name");
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("N2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnMethodArrows() {
    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return friend.name.toUpperCase(Locale.ENGLISH) as name");
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("N2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnExpression() {
    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend.name + ' ' +friend.name as name");

    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2 n2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnExpressionArrows() {
    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return friend.name + ' ' +friend.name as name");

    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2 n2", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnDefaultAlias() {
    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}.both('Friend'){as:friend}.both('Friend'){type: Person, where:(name = 'n4')} return friend.name");

    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2", item.getProperty("friend.name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testReturnDefaultAliasArrows() {
    ResultSet qResult = database.query("sql",
        "match {type:Person, where:(name = 'n1')}-Friend-{as:friend}-Friend-{type: Person, where:(name = 'n4')} return friend.name");

    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n2", item.getProperty("friend.name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testFriendsOfFriends() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend').out('Friend'){as:friend} return $matches)");

    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n4", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testFriendsOfFriendsArrows() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{}-Friend->{as:friend} return $matches)");

    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertEquals("n4", item.getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testFriendsOfFriends2() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1'), as: me}.both('Friend').both('Friend'){as:friend, where: ($matched.me != $currentMatch)} return $matches)");

    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    while (qResult.hasNext()) {
      Result item = qResult.next();
      Assertions.assertNotEquals("n1", item.getProperty("name"));
    }
    qResult.close();
  }

  @Test
  public void testFriendsOfFriends2Arrows() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1'), as: me}-Friend-{}-Friend-{as:friend, where: ($matched.me != $currentMatch)} return $matches)");

    Assertions.assertTrue(qResult.hasNext());
    while (qResult.hasNext()) {
      Assertions.assertNotEquals("n1", qResult.next().getProperty("name"));
    }
    qResult.close();
  }

  @Test
  public void testFriendsWithName() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1' and 1 + 1 = 2)}.out('Friend'){as:friend, where:(name = 'n2' and 1 + 1 = 2)} return friend)");

    Assertions.assertTrue(qResult.hasNext());
    Assertions.assertEquals("n2", qResult.next().getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testFriendsWithNameArrows() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1' and 1 + 1 = 2)}-Friend->{as:friend, where:(name = 'n2' and 1 + 1 = 2)} return friend)");
    Assertions.assertTrue(qResult.hasNext());
    Assertions.assertEquals("n2", qResult.next().getProperty("name"));
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
  }

  @Test
  public void testWhile() {

    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 1)} return friend)");
    Assertions.assertEquals(3, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 2), where: ($depth=1) } return friend)");
    Assertions.assertEquals(2, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: ($depth < 4), where: ($depth=1) } return friend)");
    Assertions.assertEquals(2, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend)");
    Assertions.assertEquals(6, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend limit 3)");
    Assertions.assertEquals(3, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, while: (true) } return friend) limit 3");
    Assertions.assertEquals(3, size(qResult));
    qResult.close();
  }

  private int size(ResultSet qResult) {
    int result = 0;
    while (qResult.hasNext()) {
      result++;
      qResult.next();
    }
    return result;
  }

  @Test
  public void testWhileArrows() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 1)} return friend)");
    Assertions.assertEquals(3, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 2), where: ($depth=1) } return friend)");
    Assertions.assertEquals(2, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, while: ($depth < 4), where: ($depth=1) } return friend)");
    Assertions.assertEquals(2, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, while: (true) } return friend)");
    Assertions.assertEquals(6, size(qResult));
    qResult.close();
  }

  @Test
  public void testMaxDepth() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1, where: ($depth=1) } return friend)");
    Assertions.assertEquals(2, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1 } return friend)");
    Assertions.assertEquals(3, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 0 } return friend)");
    Assertions.assertEquals(1, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}.out('Friend'){as:friend, maxDepth: 1, where: ($depth > 0) } return friend)");
    Assertions.assertEquals(2, size(qResult));
    qResult.close();
  }

  @Test
  public void testMaxDepthArrow() {
    ResultSet qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1, where: ($depth=1) } return friend)");
    Assertions.assertEquals(2, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1 } return friend)");
    Assertions.assertEquals(3, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 0 } return friend)");
    Assertions.assertEquals(1, size(qResult));
    qResult.close();

    qResult = database.query("sql",
        "select friend.name as name from (match {type:Person, where:(name = 'n1')}-Friend->{as:friend, maxDepth: 1, where: ($depth > 0) } return friend)");
    Assertions.assertEquals(2, size(qResult));
    qResult.close();
  }

  @Test
  public void testManager() {
    // the manager of a person is the manager of the department that person belongs to.
    // if that department does not have a direct manager, climb up the hierarchy until you find one
    Assertions.assertEquals("c", getManager("p10").get("name"));
    Assertions.assertEquals("c", getManager("p12").get("name"));
    Assertions.assertEquals("b", getManager("p6").get("name"));
    Assertions.assertEquals("b", getManager("p11").get("name"));

    Assertions.assertEquals("c", getManagerArrows("p10").get("name"));
    Assertions.assertEquals("c", getManagerArrows("p12").get("name"));
    Assertions.assertEquals("b", getManagerArrows("p6").get("name"));
    Assertions.assertEquals("b", getManagerArrows("p11").get("name"));
  }

  @Test
  public void testExpanded() {
    StringBuilder query = new StringBuilder();
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

    ResultSet qResult = database.query("sql", query.toString());
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();

    Assertions.assertEquals("Employee", item.getProperty("@type"));
  }

  private Document getManager(String personName) {
    StringBuilder query = new StringBuilder();
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

    ResultSet qResult = database.query("sql", query.toString());
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
    return item.getElement().get().getRecord().asVertex();
  }

  private Document getManagerArrows(String personName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(manager) from (");
    query.append("  match {type:Employee, where: (name = '" + personName + "')}");
    query.append("  -WorksAt->{}-ParentDepartment->{");
    query.append("      while: (in('ManagerOf').size() == 0),");
    query.append("      where: (in('ManagerOf').size() > 0)");
    query.append("  }<-ManagerOf-{as: manager}");
    query.append("  return manager");
    query.append(")");

    ResultSet qResult = database.query("sql", query.toString());
    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
    return item.getElement().get().getRecord().asVertex();
  }

  @Test
  public void testManager2() {
    // the manager of a person is the manager of the department that person belongs to.
    // if that department does not have a direct manager, climb up the hierarchy until you find one

    Assertions.assertEquals("c", getManager2("p10").getProperty("name"));
    Assertions.assertEquals("c", getManager2("p12").getProperty("name"));
    Assertions.assertEquals("b", getManager2("p6").getProperty("name"));
    Assertions.assertEquals("b", getManager2("p11").getProperty("name"));

    Assertions.assertEquals("c", getManager2Arrows("p10").getProperty("name"));
    Assertions.assertEquals("c", getManager2Arrows("p12").getProperty("name"));
    Assertions.assertEquals("b", getManager2Arrows("p6").getProperty("name"));
    Assertions.assertEquals("b", getManager2Arrows("p11").getProperty("name"));
  }

  private Result getManager2(String personName) {
    StringBuilder query = new StringBuilder();
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

    ResultSet qResult = database.query("sql", query.toString());
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
    return item;
  }

  private Result getManager2Arrows(String personName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(manager) from (");
    query.append("  match {type:Employee, where: (name = '" + personName + "')}");
    query.append("   .( -WorksAt->{}-ParentDepartment->{");
    query.append("       while: (in('ManagerOf').size() == 0),");
    query.append("       where: (in('ManagerOf').size() > 0)");
    query.append("     }");
    query.append("   )<-ManagerOf-{as: manager}");
    query.append("  return manager");
    query.append(")");

    ResultSet qResult = database.query("sql", query.toString());
    Assertions.assertTrue(qResult.hasNext());
    Result item = qResult.next();
    Assertions.assertFalse(qResult.hasNext());
    qResult.close();
    return item;
  }

  @Test
  public void testManaged() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    ResultSet managedByA = getManagedBy("a");
    Assertions.assertTrue(managedByA.hasNext());
    Result item = managedByA.next();
    Assertions.assertFalse(managedByA.hasNext());
    Assertions.assertEquals("p1", item.getProperty("name"));
    managedByA.close();

    ResultSet managedByB = getManagedBy("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      Assertions.assertTrue(managedByB.hasNext());
      Result id = managedByB.next();
      String name = id.getProperty("name");
      names.add(name);
    }
    Assertions.assertEquals(expectedNames, names);
    managedByB.close();
  }

  private ResultSet getManagedBy(String managerName) {
    StringBuilder query = new StringBuilder();
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
  public void testManagedArrows() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    ResultSet managedByA = getManagedByArrows("a");
    Assertions.assertTrue(managedByA.hasNext());
    Result item = managedByA.next();
    Assertions.assertFalse(managedByA.hasNext());
    Assertions.assertEquals("p1", item.getProperty("name"));
    managedByA.close();
    ResultSet managedByB = getManagedByArrows("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      Assertions.assertTrue(managedByB.hasNext());
      Result id = managedByB.next();
      String name = id.getProperty("name");
      names.add(name);
    }
    Assertions.assertEquals(expectedNames, names);
    managedByB.close();
  }

  private ResultSet getManagedByArrows(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(managed) from (");
    query.append("  match {type:Employee, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}<-ParentDepartment-{");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return managed");
    query.append(")");

    return database.query("sql", query.toString());
  }

  @Test
  public void testManaged2() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    ResultSet managedByA = getManagedBy2("a");
    Assertions.assertTrue(managedByA.hasNext());
    Result item = managedByA.next();
    Assertions.assertFalse(managedByA.hasNext());
    Assertions.assertEquals("p1", item.getProperty("name"));
    managedByA.close();
    ResultSet managedByB = getManagedBy2("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      Assertions.assertTrue(managedByB.hasNext());
      Result id = managedByB.next();
      String name = id.getProperty("name");
      names.add(name);
    }
    Assertions.assertEquals(expectedNames, names);
    managedByB.close();
  }

  private ResultSet getManagedBy2(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(managed) from (");
    query.append("  match {type:Employee, where: (name = '" + managerName + "')}");
    query.append("  .out('ManagerOf')");
    query.append("  .(inE('ParentDepartment').outV()){");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }");
    query.append("  .in('WorksAt'){as: managed}");
    query.append("  return managed");
    query.append(")");

    return database.query("sql", query.toString());
  }

  @Test
  public void testManaged2Arrows() {
    // people managed by a manager are people who belong to his department or people who belong to
    // sub-departments without a manager
    ResultSet managedByA = getManagedBy2Arrows("a");
    Assertions.assertTrue(managedByA.hasNext());
    Result item = managedByA.next();
    Assertions.assertFalse(managedByA.hasNext());
    Assertions.assertEquals("p1", item.getProperty("name"));
    managedByA.close();
    ResultSet managedByB = getManagedBy2Arrows("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 5; i++) {
      Assertions.assertTrue(managedByB.hasNext());
      Result id = managedByB.next();
      String name = id.getProperty("name");
      names.add(name);
    }
    Assertions.assertEquals(expectedNames, names);
    managedByB.close();
  }

  private ResultSet getManagedBy2Arrows(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("select expand(managed) from (");
    query.append("  match {type:Employee, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}");
    query.append("  .(inE('ParentDepartment').outV()){");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return managed");
    query.append(")");

    return database.query("sql", query.toString());
  }

  @Test
  public void testTriangle1() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("  .out('TriangleE'){as: friend2}");
    query.append("  .out('TriangleE'){as: friend3},");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){as: friend3}");
    query.append("return $matches");

    ResultSet result = database.query("sql", query.toString());

    printExecutionPlan(result);

    Assertions.assertTrue(result.hasNext());
    result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTriangle1Arrows() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)} -TriangleE-> {as: friend2} -TriangleE-> {as: friend3},");
    query.append("{type:TriangleV, as: friend1} -TriangleE-> {as: friend3}");
    query.append("return $matches");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTriangle2Old() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){type:TriangleV, as: friend2, where: (uid = 1)}");
    query.append("  .out('TriangleE'){as: friend3},");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){as: friend3}");
    query.append("return $matches");

    ResultSet result = database.query("sql", query.toString());
    printExecutionPlan(result);
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Document friend1 = doc.getProperty("friend1");
    Document friend2 = doc.getProperty("friend2");
    Document friend3 = doc.getProperty("friend3");
    Assertions.assertEquals(0, friend1.getInteger("uid"));
    Assertions.assertEquals(1, friend2.getInteger("uid"));
    Assertions.assertEquals(2, friend3.getInteger("uid"));
    result.close();
  }

  @Test
  public void testTriangle2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){type:TriangleV, as: friend2, where: (uid = 1)}");
    query.append("  .out('TriangleE'){as: friend3},");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){as: friend3}");
    query.append("return $patterns");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    Document friend1 = doc.getProperty("friend1");
    Document friend2 = doc.getProperty("friend2");
    Document friend3 = doc.getProperty("friend3");
    Assertions.assertEquals(0, friend1.getInteger("uid"));
    Assertions.assertEquals(1, friend2.getInteger("uid"));
    Assertions.assertEquals(2, friend3.getInteger("uid"));
    result.close();
  }

  @Test
  public void testTriangle2Arrows() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  -TriangleE->{type:TriangleV, as: friend2, where: (uid = 1)}");
    query.append("  -TriangleE->{as: friend3},");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  -TriangleE->{as: friend3}");
    query.append("return $matches");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    Document friend1 = doc.getProperty("friend1");
    Document friend2 = doc.getProperty("friend2");
    Document friend3 = doc.getProperty("friend3");
    Assertions.assertEquals(0, friend1.getInteger("uid"));
    Assertions.assertEquals(1, friend2.getInteger("uid"));
    Assertions.assertEquals(2, friend3.getInteger("uid"));
    result.close();
  }

  @Test
  public void testTriangle3() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  -TriangleE->{as: friend2}");
    query.append("  -TriangleE->{as: friend3, where: (uid = 2)},");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  -TriangleE->{as: friend3}");
    query.append("return $matches");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTriangle4() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){as: friend2, where: (uid = 1)}");
    query.append("  .out('TriangleE'){as: friend3},");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  .out('TriangleE'){as: friend3}");
    query.append("return $matches");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTriangle4Arrows() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  -TriangleE->{as: friend2, where: (uid = 1)}");
    query.append("  -TriangleE->{as: friend3},");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  -TriangleE->{as: friend3}");
    query.append("return $matches");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTriangleWithEdges4() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  .outE('TriangleE').inV(){as: friend2, where: (uid = 1)}");
    query.append("  .outE('TriangleE').inV(){as: friend3},");
    query.append("{type:TriangleV, as: friend1}");
    query.append("  .outE('TriangleE').inV(){as: friend3}");
    query.append("return $matches");

    ResultSet result = database.query("sql", query.toString());
    printExecutionPlan(result);
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCartesianProduct() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where:(uid = 1)},");
    query.append("{type:TriangleV, as: friend2, where:(uid = 2 or uid = 3)}");
    query.append("return $matches");

    ResultSet result = database.query("sql", query.toString());
    printExecutionPlan(result);

    for (int i = 0; i < 2; i++) {
      Assertions.assertTrue(result.hasNext());
      Result doc = result.next();
      Vertex friend1 = doc.getProperty("friend1");
      Assertions.assertEquals(friend1.getInteger("uid"), 1);
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNoPrefetch() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:IndexedVertex, as: one}");
    query.append("return $patterns");

    ResultSet result = database.query("sql", query.toString());
    printExecutionPlan(result);

    result.getExecutionPlan().ifPresent(x -> x.getSteps().stream().filter(y -> y instanceof MatchPrefetchStep).forEach(prefetchStepFound -> fail()));

    for (int i = 0; i < 1000; i++) {
      Assertions.assertTrue(result.hasNext());
      result.next();
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCartesianProductLimit() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where:(uid = 1)},");
    query.append("{type:TriangleV, as: friend2, where:(uid = 2 or uid = 3)}");
    query.append("return $matches LIMIT 1");

    ResultSet result = database.query("sql", query.toString());

    Assertions.assertTrue(result.hasNext());
    Result d = result.next();
    Document friend1 = d.getProperty("friend1");
    Assertions.assertEquals(friend1.getInteger("uid"), 1);
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testArrayNumber() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0] as foo");

    ResultSet result = database.query("sql", query.toString());

    Assertions.assertTrue(result.hasNext());

    Result doc = result.next();
    Object foo = doc.getProperty("foo");
    Assertions.assertNotNull(foo);
    Assertions.assertTrue(foo instanceof Vertex);
    result.close();
  }

  @Test
  public void testArraySingleSelectors2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0,1] as foo");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    Object foo = doc.getProperty("foo");
    Assertions.assertNotNull(foo);
    Assertions.assertTrue(foo instanceof List);
    Assertions.assertEquals(2, ((List) foo).size());
    result.close();
  }

  @Test
  public void testArrayRangeSelectors1() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0..1] as foo");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());

    Object foo = doc.getProperty("foo");
    Assertions.assertNotNull(foo);
    Assertions.assertTrue(foo instanceof List);
    Assertions.assertEquals(1, ((List) foo).size());
    result.close();
  }

  @Test
  public void testArrayRange2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0..2] as foo");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());

    Object foo = doc.getProperty("foo");
    Assertions.assertNotNull(foo);
    Assertions.assertTrue(foo instanceof List);
    Assertions.assertEquals(2, ((List) foo).size());
    result.close();
  }

  @Test
  public void testArrayRange3() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[0..3] as foo");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());

    Object foo = doc.getProperty("foo");
    Assertions.assertNotNull(foo);
    Assertions.assertTrue(foo instanceof List);
    Assertions.assertEquals(2, ((List) foo).size());
    result.close();
  }

  @Test
  public void testConditionInSquareBrackets() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:TriangleV, as: friend1, where: (uid = 0)}");
    query.append("return friend1.out('TriangleE')[uid = 2] as foo");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());

    Object foo = doc.getProperty("foo");
    Assertions.assertNotNull(foo);
    Assertions.assertTrue(foo instanceof List);
    Assertions.assertEquals(1, ((List) foo).size());
    Vertex resultVertex = (Vertex) ((List) foo).get(0);
    Assertions.assertEquals(2, resultVertex.getInteger("uid"));
    result.close();
  }

  @Test
  public void testIndexedEdge() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:IndexedVertex, as: one, where: (uid = 0)}");
    query.append(".out('IndexedEdge'){type:IndexedVertex, as: two, where: (uid = 1)}");
    query.append("return one, two");

    ResultSet result = database.query("sql", query.toString());
    printExecutionPlan(result);
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testIndexedEdgeArrows() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:IndexedVertex, as: one, where: (uid = 0)}");
    query.append("-IndexedEdge->{type:IndexedVertex, as: two, where: (uid = 1)}");
    query.append("return one, two");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testJson() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:IndexedVertex, as: one, where: (uid = 0)} ");
    query.append("return {'name':'foo', 'uuid':one.uid}");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());

    //    Document doc = result.get(0);
    //    assertEquals("foo", doc.set("name");
    //    assertEquals(0, doc.set("uuid");
    result.close();
  }

  @Test
  public void testJson2() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:IndexedVertex, as: one, where: (uid = 0)} ");
    query.append("return {'name':'foo', 'sub': {'uuid':one.uid}}");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    //    Document doc = result.get(0);
    //    assertEquals("foo", doc.set("name");
    //    assertEquals(0, doc.set("sub.uuid");
    result.close();
  }

  @Test
  public void testJson3() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:IndexedVertex, as: one, where: (uid = 0)} ");
    query.append("return {'name':'foo', 'sub': [{'uuid':one.uid}]}");

    ResultSet result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());
    //    Document doc = result.get(0);
    //    assertEquals("foo", doc.set("name");
    //    assertEquals(0, doc.set("sub[0].uuid");

    result.close();
  }

  @Test
  public void testUnique() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two} ");
    query.append("return DISTINCT one, two");

    ResultSet result = database.query("sql", query.toString());
    printExecutionPlan(result);
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertFalse(result.hasNext());

    query = new StringBuilder();
    query.append("match ");
    query.append("{type:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two} ");
    query.append("return DISTINCT one.uid, two.uid");

    result.close();

    result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    doc = result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();
    //    Document doc = result.get(0);
    //    assertEquals("foo", doc.set("name");
    //    assertEquals(0, doc.set("sub[0].uuid");
  }

  @Test
  public void testNotUnique() {
    StringBuilder query = new StringBuilder();
    query.append("match ");
    query.append("{type:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two} ");
    query.append("return one, two");

    ResultSet result = database.query("sql", query.toString());
    printExecutionPlan(result);
    Assertions.assertTrue(result.hasNext());
    Result doc = result.next();
    Assertions.assertTrue(result.hasNext());
    doc = result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();

    query = new StringBuilder();
    query.append("match ");
    query.append("{type:DiamondV, as: one, where: (uid = 0)}.out('DiamondE').out('DiamondE'){as: two} ");
    query.append("return one.uid, two.uid");

    result = database.query("sql", query.toString());
    Assertions.assertTrue(result.hasNext());
    doc = result.next();
    Assertions.assertTrue(result.hasNext());
    doc = result.next();
    Assertions.assertFalse(result.hasNext());
    result.close();
    //    Document doc = result.get(0);
    //    assertEquals("foo", doc.set("name");
    //    assertEquals(0, doc.set("sub[0].uuid");
  }

  @Test
  public void testManagedElements() {
    ResultSet managedByB = getManagedElements("b");

    Set<String> expectedNames = new HashSet<String>();
    expectedNames.add("b");
    expectedNames.add("p2");
    expectedNames.add("p3");
    expectedNames.add("p6");
    expectedNames.add("p7");
    expectedNames.add("p11");
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 6; i++) {
      Assertions.assertTrue(managedByB.hasNext());
      Result doc = managedByB.next();
      String name = doc.getProperty("name");
      names.add(name);
    }
    Assertions.assertFalse(managedByB.hasNext());
    Assertions.assertEquals(expectedNames, names);
    managedByB.close();
  }

  private ResultSet getManagedElements(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("  match {type:Employee, as:boss, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}<-ParentDepartment-{");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return distinct $elements");

    return database.query("sql", query.toString());
  }

  @Test
  public void testManagedPathElements() {
    ResultSet managedByB = getManagedPathElements("b");

    Set<String> expectedNames = new HashSet<String>();
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
    Set<String> names = new HashSet<String>();
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(managedByB.hasNext());
      Result doc = managedByB.next();
      String name = doc.getProperty("name");
      names.add(name);
    }
    Assertions.assertFalse(managedByB.hasNext());
    Assertions.assertEquals(expectedNames, names);
    managedByB.close();
  }

  @Test
  public void testOptional() {
    ResultSet qResult = database.query("sql", "match {type:Person, as: person} -NonExistingEdge-> {as:b, optional:true} return person, b.name");

    printExecutionPlan(qResult);
    for (int i = 0; i < 6; i++) {
      Assertions.assertTrue(qResult.hasNext());
      Result doc = qResult.next();
      Assertions.assertTrue(doc.getPropertyNames().size() == 2);
      Vertex person = doc.getProperty("person");

      String name = person.getString("name");
      Assertions.assertTrue(name.startsWith("n"));
    }
  }

  @Test
  public void testOptional2() {
    ResultSet qResult = database.query("sql", "match {type:Person, as: person} --> {as:b, optional:true, where:(nonExisting = 12)} return person, b.name");

    for (int i = 0; i < 6; i++) {
      Assertions.assertTrue(qResult.hasNext());
      Result doc = qResult.next();
      Assertions.assertTrue(doc.getPropertyNames().size() == 2);
      Vertex person = doc.getProperty("person");

      String name = person.getString("name");
      Assertions.assertTrue(name.startsWith("n"));
    }
  }

  @Test
  public void testOptional3() {
    ResultSet qResult = database.query("sql", "select friend.name as name, b from ("
        + "match {type:Person, as:a, where:(name = 'n1' and 1 + 1 = 2)}.out('Friend'){as:friend, where:(name = 'n2' and 1 + 1 = 2)},"
        + "{as:a}.out(){as:b, where:(nonExisting = 12), optional:true}," + "{as:friend}.out(){as:b, optional:true}" + " return friend, b)");

    printExecutionPlan(qResult);
    Assertions.assertTrue(qResult.hasNext());
    Result doc = qResult.next();
    Assertions.assertEquals("n2", doc.getProperty("name"));
    Assertions.assertNull(doc.getProperty("b"));
    Assertions.assertFalse(qResult.hasNext());
  }

  @Test
  public void testOrderByAsc() {
    database.command("sql", "CREATE vertex type testOrderByAsc ");

    database.command("sql", "CREATE VERTEX testOrderByAsc SET name = 'bbb'");
    database.command("sql", "CREATE VERTEX testOrderByAsc SET name = 'zzz'");
    database.command("sql", "CREATE VERTEX testOrderByAsc SET name = 'aaa'");
    database.command("sql", "CREATE VERTEX testOrderByAsc SET name = 'ccc'");

    String query = "MATCH { type: testOrderByAsc, as:a} RETURN a.name as name order by name asc";

    ResultSet result = database.query("sql", query);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals("aaa", result.next().getProperty("name"));
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals("bbb", result.next().getProperty("name"));
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals("ccc", result.next().getProperty("name"));
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals("zzz", result.next().getProperty("name"));
    Assertions.assertFalse(result.hasNext());
  }

  @Test
  public void testOrderByDesc() {
    database.command("sql", "CREATE vertex type testOrderByDesc");

    database.command("sql", "CREATE VERTEX testOrderByDesc SET name = 'bbb'");
    database.command("sql", "CREATE VERTEX testOrderByDesc SET name = 'zzz'");
    database.command("sql", "CREATE VERTEX testOrderByDesc SET name = 'aaa'");
    database.command("sql", "CREATE VERTEX testOrderByDesc SET name = 'ccc'");

    String query = "MATCH { type: testOrderByDesc, as:a} RETURN a.name as name order by name desc";

    ResultSet result = database.query("sql", query);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals("zzz", result.next().getProperty("name"));
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals("ccc", result.next().getProperty("name"));
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals("bbb", result.next().getProperty("name"));
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals("aaa", result.next().getProperty("name"));
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNestedProjections() {
    String clazz = "testNestedProjections";
    database.command("sql", "CREATE vertex type " + clazz + " ");

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', surname = 'ccc'");

    String query = "MATCH { type: " + clazz + ", as:a} RETURN a:{name}, 'x' ";

    ResultSet result = database.query("sql", query);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Result a = item.getProperty("a");
    Assertions.assertEquals("bbb", a.getProperty("name"));
    Assertions.assertNull(a.getProperty("surname"));
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNestedProjectionsStar() {
    String clazz = "testNestedProjectionsStar";
    database.command("sql", "CREATE vertex type " + clazz + " ");

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', surname = 'ccc'");

    if (database.isTransactionActive()) {
      database.commit();
      database.begin();
    }

    String query = "MATCH { type: " + clazz + ", as:a} RETURN a:{*, @rid}, 'x' ";

    ResultSet result = database.query("sql", query);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Result a = item.getProperty("a");
    Assertions.assertEquals("bbb", a.getProperty("name"));
    Assertions.assertEquals("ccc", a.getProperty("surname"));
    Assertions.assertNotNull(a.getProperty("@rid"));
    Assertions.assertEquals(3, a.getPropertyNames().size());
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testExpand() {
    String clazz = "testExpand";
    database.command("sql", "CREATE vertex type " + clazz + " ");

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', surname = 'ccc'");

    if (database.isTransactionActive()) {
      database.commit();
      database.begin();
    }

    String query = "MATCH { type: " + clazz + ", as:a} RETURN expand(a) ";

    ResultSet result = database.query("sql", query);
    Assertions.assertTrue(result.hasNext());
    Result a = result.next();
    Assertions.assertEquals("bbb", a.getProperty("name"));
    Assertions.assertEquals("ccc", a.getProperty("surname"));
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testAggregate() {
    String clazz = "testAggregate";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 1");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 2");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 3");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', num = 4");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', num = 5");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', num = 6");

    String query = "MATCH { type: " + clazz + ", as:a} RETURN a.name as a, max(a.num) as maxNum group by a.name order by a.name";

    ResultSet result = database.query("sql", query);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertEquals("aaa", item.getProperty("a"));
    Assertions.assertEquals(3, (int) item.getProperty("maxNum"));

    Assertions.assertTrue(result.hasNext());
    item = result.next();
    Assertions.assertEquals("bbb", item.getProperty("a"));
    Assertions.assertEquals(6, (int) item.getProperty("maxNum"));

    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testOrderByOutOfProjAsc() {
    String clazz = "testOrderByOutOfProjAsc";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 0, num2 = 1");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 1, num2 = 2");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 2, num2 = 3");

    String query = "MATCH { type: " + clazz + ", as:a} RETURN a.name as name, a.num as num order by a.num2 asc";

    ResultSet result = database.query("sql", query);
    for (int i = 0; i < 3; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertEquals("aaa", item.getProperty("name"));
      Assertions.assertEquals(i, (int) item.getProperty("num"));
    }

    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testOrderByOutOfProjDesc() {
    String clazz = "testOrderByOutOfProjDesc";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 0, num2 = 1");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 1, num2 = 2");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', num = 2, num2 = 3");

    String query = "MATCH { type: " + clazz + ", as:a} RETURN a.name as name, a.num as num order by a.num2 desc";

    ResultSet result = database.query("sql", query);
    printExecutionPlan(result);
    for (int i = 2; i >= 0; i--) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertEquals("aaa", item.getProperty("name"));
      Assertions.assertEquals(i, (int) item.getProperty("num"));
    }

    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUnwind() {
    String clazz = "testUnwind";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa', coll = [1, 2]");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb', coll = [3, 4]");

    String query = "MATCH { type: " + clazz + ", as:a} RETURN a.name as name, a.coll as num unwind num";

    int sum = 0;
    ResultSet result = database.query("sql", query);
    for (int i = 0; i < 4; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      sum += (Integer) item.getProperty("num");
    }

    Assertions.assertFalse(result.hasNext());

    result.close();
    Assertions.assertEquals(10, sum);
  }

  @Test
  public void testSkip() {
    String clazz = "testSkip";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ccc'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ddd'");

    String query = "MATCH { type: " + clazz + ", as:a} RETURN a.name as name ORDER BY name ASC skip 1 limit 2";

    ResultSet result = database.query("sql", query);

    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertEquals("bbb", item.getProperty("name"));

    Assertions.assertTrue(result.hasNext());
    item = result.next();
    Assertions.assertEquals("ccc", item.getProperty("name"));

    Assertions.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testDepthAlias() {
    String clazz = "testDepthAlias";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ccc'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ddd'");

    database.command("sql", "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'aaa') TO (SELECT FROM " + clazz + " WHERE name = 'bbb')");
    database.command("sql", "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'bbb') TO (SELECT FROM " + clazz + " WHERE name = 'ccc')");
    database.command("sql", "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'ccc') TO (SELECT FROM " + clazz + " WHERE name = 'ddd')");

    String query =
        "MATCH { type: " + clazz + ", as:a, where:(name = 'aaa')} --> {as:b, while:($depth<10), depthAlias: xy} RETURN a.name as name, b.name as bname, xy";

    ResultSet result = database.query("sql", query);

    int sum = 0;
    for (int i = 0; i < 4; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Object depth = item.getProperty("xy");
      Assertions.assertTrue(depth instanceof Integer);
      Assertions.assertEquals("aaa", item.getProperty("name"));
      switch ((int) depth) {
      case 0:
        Assertions.assertEquals("aaa", item.getProperty("bname"));
        break;
      case 1:
        Assertions.assertEquals("bbb", item.getProperty("bname"));
        break;
      case 2:
        Assertions.assertEquals("ccc", item.getProperty("bname"));
        break;
      case 3:
        Assertions.assertEquals("ddd", item.getProperty("bname"));
        break;
      default:
        fail();
      }
      sum += (int) depth;
    }
    Assertions.assertEquals(sum, 6);
    Assertions.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testPathAlias() {
    String clazz = "testPathAlias";
    database.command("sql", "CREATE vertex type " + clazz);

    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'aaa'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'bbb'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ccc'");
    database.command("sql", "CREATE VERTEX " + clazz + " SET name = 'ddd'");

    database.command("sql", "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'aaa') TO (SELECT FROM " + clazz + " WHERE name = 'bbb')");
    database.command("sql", "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'bbb') TO (SELECT FROM " + clazz + " WHERE name = 'ccc')");
    database.command("sql", "CREATE EDGE Friend FROM (SELECT FROM " + clazz + " WHERE name = 'ccc') TO (SELECT FROM " + clazz + " WHERE name = 'ddd')");

    String query =
        "MATCH { type: " + clazz + ", as:a, where:(name = 'aaa')} --> {as:b, while:($depth<10), pathAlias: xy} RETURN a.name as name, b.name as bname, xy";

    ResultSet result = database.query("sql", query);

    for (int i = 0; i < 4; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Object path = item.getProperty("xy");
      Assertions.assertTrue(path instanceof List);
      List<Identifiable> thePath = (List<Identifiable>) path;

      String bname = item.getProperty("bname");
      if (bname.equals("aaa")) {
        Assertions.assertEquals(0, thePath.size());
      } else if (bname.equals("aaa")) {
        Assertions.assertEquals(1, thePath.size());
        Assertions.assertEquals("bbb", ((Document) thePath.get(0).getRecord()).getString("name"));
      } else if (bname.equals("ccc")) {
        Assertions.assertEquals(2, thePath.size());
        Assertions.assertEquals("bbb", ((Document) thePath.get(0).getRecord()).getString("name"));
        Assertions.assertEquals("ccc", ((Document) thePath.get(1).getRecord()).getString("name"));
      } else if (bname.equals("ddd")) {
        Assertions.assertEquals(3, thePath.size());
        Assertions.assertEquals("bbb", ((Document) thePath.get(0).getRecord()).getString("name"));
        Assertions.assertEquals("ccc", ((Document) thePath.get(1).getRecord()).getString("name"));
        Assertions.assertEquals("ddd", ((Document) thePath.get(2).getRecord()).getString("name"));
      }
    }
    Assertions.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testBucketTarget() {
    String clazz = "testBucketTarget";
    database.command("SQL", "CREATE vertex type " + clazz).close();

    database.command("SQL", "ALTER TYPE " + clazz + " BUCKET +" + clazz + "_one").close();
    database.command("SQL", "ALTER TYPE " + clazz + " BUCKET +" + clazz + "_two").close();
    database.command("SQL", "ALTER TYPE " + clazz + " BUCKET +" + clazz + "_three").close();

    MutableVertex v1 = database.newVertex(clazz);
    v1.set("name", "one");
    v1.save(clazz + "_one");

    MutableVertex vx = database.newVertex(clazz);
    vx.set("name", "onex");
    vx.save(clazz + "_one");

    MutableVertex v2 = database.newVertex(clazz);
    v2.set("name", "two");
    v2.save(clazz + "_two");

    MutableVertex v3 = database.newVertex(clazz);
    v3.set("name", "three");
    v3.save(clazz + "_three");

    v1.newEdge("Friend", v2, true).save();
    v2.newEdge("Friend", v3, true).save();
    v1.newEdge("Friend", v3, true).save();

    String query = "MATCH { bucket: " + clazz + "_one, as:a} --> {as:b, bucket:" + clazz + "_two} RETURN a.name as aname, b.name as bname";

    ResultSet result = database.query("SQL", query);

    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertEquals("one", item.getProperty("aname"));
    Assertions.assertEquals("two", item.getProperty("bname"));

    Assertions.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testNegativePattern() {
    String clazz = "testNegativePattern";
    database.command("SQL", "CREATE vertex type " + clazz).close();

    MutableVertex v1 = database.newVertex(clazz);
    v1.set("name", "a");
    v1.save();

    MutableVertex v2 = database.newVertex(clazz);
    v2.set("name", "b");
    v2.save();

    MutableVertex v3 = database.newVertex(clazz);
    v3.set("name", "c");
    v3.save();

    v1.newEdge("Friend", v2, true).save();
    v2.newEdge("Friend", v3, true).save();

    String query = "MATCH { type:" + clazz + ", as:a} --> {as:b} --> {as:c}, ";
    query += " NOT {as:a} --> {as:c}";
    query += " RETURN $patterns";

    ResultSet result = database.query("SQL", query);
    Assertions.assertTrue(result.hasNext());
    result.next();
    Assertions.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testNegativePattern2() {
    String clazz = "testNegativePattern2";
    database.command("SQL", "CREATE vertex type " + clazz).close();

    MutableVertex v1 = database.newVertex(clazz);
    v1.set("name", "a");
    v1.save();

    MutableVertex v2 = database.newVertex(clazz);
    v2.set("name", "b");
    v2.save();

    MutableVertex v3 = database.newVertex(clazz);
    v3.set("name", "c");
    v3.save();

    v1.newEdge("Friend", v2, true).save();
    v2.newEdge("Friend", v3, true).save();
    v1.newEdge("Friend", v3, true).save();

    String query = "MATCH { type:" + clazz + ", as:a} --> {as:b} --> {as:c}, ";
    query += " NOT {as:a} --> {as:c}";
    query += " RETURN $patterns";

    ResultSet result = database.query("SQL", query);
    Assertions.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testNegativePattern3() {
    String clazz = "testNegativePattern3";
    database.command("SQL", "CREATE vertex type " + clazz).close();

    MutableVertex v1 = database.newVertex(clazz);
    v1.set("name", "a");
    v1.save();

    MutableVertex v2 = database.newVertex(clazz);
    v2.set("name", "b");
    v2.save();

    MutableVertex v3 = database.newVertex(clazz);
    v3.set("name", "c");
    v3.save();

    v1.newEdge("Friend", v2, true).save();
    v2.newEdge("Friend", v3, true).save();
    v1.newEdge("Friend", v3, true).save();

    String query = "MATCH { type:" + clazz + ", as:a} --> {as:b} --> {as:c}, ";
    query += " NOT {as:a} --> {as:c, where:(name <> 'c')}";
    query += " RETURN $patterns";

    ResultSet result = database.query("SQL", query);
    Assertions.assertTrue(result.hasNext());
    result.next();
    Assertions.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testPathTraversal() {
    String clazz = "testPathTraversal";
    database.command("SQL", "CREATE vertex type " + clazz).close();

    MutableVertex v1 = database.newVertex(clazz);
    v1.set("name", "a");
    v1.save();

    MutableVertex v2 = database.newVertex(clazz);
    v2.set("name", "b");
    v2.save();

    MutableVertex v3 = database.newVertex(clazz);
    v3.set("name", "c");
    v3.save();

    v1.set("next", v2);
    v2.set("next", v3);

    v1.save();
    v2.save();

    String query = "MATCH { type:" + clazz + ", as:a}.next{as:b, where:(name ='b')}";
    query += " RETURN a.name as a, b.name as b";

    ResultSet result = database.query("SQL", query);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertEquals("a", item.getProperty("a"));
    Assertions.assertEquals("b", item.getProperty("b"));

    Assertions.assertFalse(result.hasNext());

    result.close();

    query = "MATCH { type:" + clazz + ", as:a, where:(name ='a')}.next{as:b}";
    query += " RETURN a.name as a, b.name as b";

    result = database.query("SQL", query);
    Assertions.assertTrue(result.hasNext());
    item = result.next();
    Assertions.assertEquals("a", item.getProperty("a"));
    Assertions.assertEquals("b", item.getProperty("b"));

    Assertions.assertFalse(result.hasNext());

    result.close();
  }

  private ResultSet getManagedPathElements(String managerName) {
    StringBuilder query = new StringBuilder();
    query.append("  match {type:Employee, as:boss, where: (name = '" + managerName + "')}");
    query.append("  -ManagerOf->{}<-ParentDepartment-{");
    query.append("      while: ($depth = 0 or in('ManagerOf').size() = 0),");
    query.append("      where: ($depth = 0 or in('ManagerOf').size() = 0)");
    query.append("  }<-WorksAt-{as: managed}");
    query.append("  return distinct $pathElements");

    return database.query("sql", query.toString());
  }

  @Test
  public void testQuotedClassName() {
    String className = "testQuotedClassName";
    database.command("sql", "CREATE vertex type " + className);
    database.command("sql", "CREATE VERTEX " + className + " SET name = 'a'");

    String query = "MATCH {type: `" + className + "`, as:foo} RETURN $elements";

    try (ResultSet rs = database.query("SQL", query)) {
      Assertions.assertEquals(1L, rs.stream().count());
    }
  }

  private void printExecutionPlan(ResultSet result) {
    printExecutionPlan(null, result);
  }

  private void printExecutionPlan(String query, ResultSet result) {
//        if (query != null) {
//          System.out.println(query);
//        }
//        result.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 3)));
//        System.out.println();
  }
}
