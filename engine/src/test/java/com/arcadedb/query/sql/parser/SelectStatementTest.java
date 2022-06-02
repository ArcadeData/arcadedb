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
package com.arcadedb.query.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class SelectStatementTest {

  protected SimpleNode checkRightSyntax(String query) {
    SimpleNode result = checkSyntax(query, true);
    StringBuilder builder = new StringBuilder();
    result.toString(null, builder);
    return checkSyntax(builder.toString(), true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    SqlParser osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      if (!isCorrect) {
        //        System.out.println(query);
        //        if(result!= null ) {
        //          System.out.println("->");
        //          StringBuilder builder = new StringBuilder();
        //          result.toString(null, builder);
        //          System.out.println(builder.toString());
        //          System.out.println("............");
        //        }
        fail();
      }

      return result;
    } catch (Exception e) {
      if (isCorrect) {
        //System.out.println(query);
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  @Test
  public void testParserSimpleSelect1() {
    SimpleNode stm = checkRightSyntax("select from Foo");
    assertTrue(stm instanceof SelectStatement);
    SelectStatement select = (SelectStatement) stm;
    assertNull(select.getProjection());
    assertNotNull(select.getTarget());
    assertNull(select.getWhereClause());
  }

  @Test
  public void testParserSimpleSelect2() {
    SimpleNode stm = checkRightSyntax("select bar from Foo");
    assertTrue(stm instanceof SelectStatement);
    SelectStatement select = (SelectStatement) stm;
    assertNotNull(select.getProjection());
    assertNotNull(select.getProjection().getItems());
    assertEquals(select.getProjection().getItems().size(), 1);
    assertNotNull(select.getTarget());
    assertNull(select.getWhereClause());
  }

  @Test
  public void testComments() {
    checkRightSyntax("select from Foo");

    checkRightSyntax("select /* aaa bbb ccc*/from Foo");
    checkRightSyntax("select /* aaa bbb \nccc*/from Foo");
    checkRightSyntax("select /** aaa bbb ccc**/from Foo");
    checkRightSyntax("select /** aaa bbb ccc*/from Foo");

    checkRightSyntax("/* aaa bbb ccc*/select from Foo");
    checkRightSyntax("select from Foo/* aaa bbb ccc*/");
    checkRightSyntax("/* aaa bbb ccc*/select from Foo/* aaa bbb ccc*/");

    checkWrongSyntax("select /** aaa bbb */ccc*/from Foo");

    checkWrongSyntax("select /**  /*aaa bbb */ccc*/from Foo");
    checkWrongSyntax("*/ select from Foo");
  }

  @Test
  public void testSimpleSelect() {
    checkRightSyntax("select from Foo");
    checkRightSyntax("select * from Foo");

    checkWrongSyntax("select from Foo bar");
    checkWrongSyntax("select * from Foo bar");

    checkWrongSyntax("select * Foo");
  }

  @Test
  public void testUnwind() {
    checkRightSyntax("select from Foo unwind foo");
    checkRightSyntax("select from Foo unwind foo, bar");
    checkRightSyntax("select from Foo where foo = 1 unwind foo, bar");
    checkRightSyntax("select from Foo where foo = 1 order by foo unwind foo, bar");
    checkRightSyntax("select from Foo where foo = 1 group by bar order by foo unwind foo, bar");
  }

  @Test
  public void testSubSelect() {
    checkRightSyntax("select from (select from Foo)");

    checkWrongSyntax("select from select from foo");
  }

  @Test
  public void testSimpleSelectWhere() {
    checkRightSyntax("select from Foo where name = 'foo'");
    checkRightSyntax("select * from Foo where name = 'foo'");

    checkRightSyntax("select from Foo where name = 'foo' and surname = \"bar\"");
    checkRightSyntax("select * from Foo where name = 'foo' and surname = \"bar\"");

    checkWrongSyntax("select * from Foo name = 'foo'");
    checkWrongSyntax("select from Foo bar where name = 'foo'");
    checkWrongSyntax("select * from Foo bar where name = 'foo'");
    checkWrongSyntax("select Foo where name = 'foo'");
    checkWrongSyntax("select * Foo where name = 'foo'");

    // issue #5221
    checkRightSyntax("select from $1");
  }

  @Test
  public void testIn() {
    SimpleNode result = checkRightSyntax("select count(*) from OFunction where name in [\"a\"]");
    // result.dump("    ");
    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testNotIn() {
    SimpleNode result = checkRightSyntax("select count(*) from OFunction where name not in [\"a\"]");
    // result.dump("    ");
    assertTrue(result instanceof Statement);

  }

  @Test
  public void testMath1() {
    SimpleNode result = checkRightSyntax("" + "select * from sqlSelectIndexReuseTestClass where prop1 = 1 + 1");
    // result.dump("    ");
    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testMath2() {
    SimpleNode result = checkRightSyntax("" + "select * from sqlSelectIndexReuseTestClass where prop1 = foo + 1");
    // result.dump("    ");
    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testMath5() {
    SimpleNode result = checkRightSyntax("" + "select * from sqlSelectIndexReuseTestClass where prop1 = foo + 1 * bar - 5");

    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testContainsWithCondition() {
    SimpleNode result = checkRightSyntax("select from Profile where customReferences.values() CONTAINS 'a'");

    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testNamedParam() {
    SimpleNode result = checkRightSyntax("select from JavaComplexTestClass where enumField = :enumItem");

    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testBoolean() {
    SimpleNode result = checkRightSyntax("select from Foo where bar = true");
    // result.dump("    ");
    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testDottedAtField() {
    SimpleNode result = checkRightSyntax("select from City where country.@type = 'Country'");
    // result.dump("    ");
    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testQuotedFieldNameFrom() {
    SimpleNode result = checkRightSyntax("select `from` from City where country.@type = 'Country'");
    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testQuotedTargetName() {
    checkRightSyntax("select from `edge`");
    checkRightSyntax("select from `from`");
    checkRightSyntax("select from `vertex`");
    checkRightSyntax("select from `select`");
  }

  @Test
  public void testQuotedFieldName() {
    checkRightSyntax("select `foo` from City where country.@type = 'Country'");
  }

  @Test
  public void testLongDotted() {
    SimpleNode result = checkRightSyntax("select from Profile where location.city.country.name = 'Spain'");
    // result.dump("    ");
    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testInIsNotAReservedWord() {
    SimpleNode result = checkRightSyntax("select count(*) from TRVertex where in.type() not in [\"LINKSET\"] ");
    // result.dump("    ");
    assertTrue(result instanceof SelectStatement);

  }

  @Test
  public void testSelectFunction() {
    SimpleNode result = checkRightSyntax("select max(1,2,7,0,-2,3), 'pluto'");
    // result.dump("    ");
    assertTrue(result instanceof SelectWithoutTargetStatement);
  }

  @Test
  public void testEscape1() {
    SimpleNode result = checkRightSyntax("select from bucket:internal where \"\\u005C\\u005C\" = \"\\u005C\\u005C\" ");
    assertTrue(result instanceof SelectStatement);
  }

  @Test
  public void testWildcardSuffix() {
    checkRightSyntax("select foo.* from bar ");
  }

  @Test
  public void testEmptyCollection() {
    String query = "select from bar where name not in :param1";
    SqlParser osql = getParserFor(query);
    try {
      final SimpleNode result = osql.parse();
      final SelectStatement stm = (SelectStatement) result;
      final Map<String, Object> params = new HashMap<>();
      params.put("param1", new HashSet<>());

      final StringBuilder parsed = new StringBuilder();
      stm.toString(params, parsed);
      assertEquals(parsed.toString(), "SELECT FROM bar WHERE name NOT IN []");
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testEscape2() {
    try {
      checkWrongSyntax("select from bucket:internal where \"\\u005C\" = \"\\u005C\" ");
      fail();
    } catch (Error e) {
      // EXPECTED
    }
  }

  @Test
  public void testSpatial() {
    checkRightSyntax("select *,$distance from Place where [latitude,longitude,$spatial] NEAR [41.893056,12.482778,{\"maxDistance\": 0.5}]");
    checkRightSyntax("select * from Place where [latitude,longitude] WITHIN [[51.507222,-0.1275],[55.507222,-0.1275]]");
  }

  @Test
  public void testSubConditions() {
    checkRightSyntax(
        "SELECT @rid as rid, localName FROM Person WHERE ( 'milano' IN out('lives').localName OR 'roma' IN out('lives').localName ) ORDER BY age ASC");
  }

  @Test
  public void testRecordAttributes() {
    // issue #4430
    checkRightSyntax("SELECT @this, @rid, @rid_id, @rid_pos, @type, @fields, @out, @in from V");
    checkRightSyntax("SELECT @THIS, @RID, @RID_ID, @RID_POS, @TYPE, @FIELDS, @OUT, @IN from V");
  }

  @Test
  public void testDoubleEquals() {
    // issue #4413
    checkRightSyntax("SELECT from V where name = 'foo'");
    checkRightSyntax("SELECT from V where name == 'foo'");
  }

  @Test
  public void testMatches() {

    checkRightSyntax("select from Person where name matches 'a'");

    checkRightSyntax("select from Person where name matches '(?i)(^\\\\Qname1\\\\E$)|(^\\\\Qname2\\\\E$)|(^\\\\Qname3\\\\E$)' and age=30");
  }

  @Test
  // issue #3718
  public void testComplexTarget1() {
    checkRightSyntax("SELECT $e FROM [#1:1,#1:2] LET $e = (SELECT FROM $current.prop1)");
    checkRightSyntax("SELECT $e FROM [#1:1,#1:2] let $e = (SELECT FROM (SELECT FROM $parent.$current))");
  }

  @Test
  public void testEval() {
    checkRightSyntax("  select  sum(weight) , f.name as name from (\n" + "      select weight, if(eval(\"out.name = 'one'\"),out,in) as f  from (\n"
        + "      select expand(bothE('E')) from V\n" + "  )\n" + "      ) group by name\n");
  }

  @Test
  public void testNewLine() {
    checkRightSyntax("INSERT INTO Country SET name=\"one\\ntwo\" RETURN @rid");
  }

  @Test
  public void testJsonWithUrl() {
    checkRightSyntax("insert into V content { \"url\": \"http://www.google.com\" } ");
  }

  @Test
  public void testGroupBy() {
    // issue #4245
    checkRightSyntax("select in.name from (  \n" + "select expand(outE()) from V\n" + ")\n" + "group by in.name");
  }

  @Test
  public void testInputParams() {

    checkRightSyntax("select from foo where name like  '%'+ :param1 + '%'");

    checkRightSyntax("select from foo where name like  'aaa'+ :param1 + 'a'");
  }

  @Test
  public void testSlashInQuery() {
    checkRightSyntax("insert into test content {\"node_id\": \"MFmqvmht//sYYWB8=\"}");
    checkRightSyntax("insert into test content { \"node_id\": \"MFmqvmht\\/\\/GYsYYWB8=\"}");
  }

  @Test
  public void testBucketList() {
    checkRightSyntax("select from bucket:[foo,bar]");
  }

  @Test
  public void checkOrderBySyntax() {
    checkRightSyntax("select from test order by something ");
    checkRightSyntax("select from test order by something, somethingElse ");
    checkRightSyntax("select from test order by something asc, somethingElse desc");
    checkRightSyntax("select from test order by something asc, somethingElse ");
    checkRightSyntax("select from test order by something, somethingElse asc");
    checkRightSyntax("select from test order by something asc");
    checkRightSyntax("select from test order by something desc");
    checkRightSyntax("select from test order by (something desc)");
    checkRightSyntax("select from test order by (something asc)");
    checkRightSyntax("select from test order by (something asc),somethingElse");
    checkRightSyntax("select from test order by (something),(somethingElse)");
    checkRightSyntax("select from test order by something,(somethingElse)");
    checkRightSyntax("select from test order by (something asc),(somethingElse desc)");
  }

  @Test
  public void testBacktick() {
    checkRightSyntax("select `foo` from foo where `foo` = 'bar'");
    checkRightSyntax("select `SELECT` from foo where `SELECT` = 'bar'");
    checkRightSyntax("select `TRAVERSE` from foo where `TRAVERSE` = 'bar'");
    checkRightSyntax("select `INSERT` from foo where `INSERT` = 'bar'");
    checkRightSyntax("select `CREATE` from foo where `CREATE` = 'bar'");
    checkRightSyntax("select `DELETE` from foo where `DELETE` = 'bar'");
    checkRightSyntax("select `VERTEX` from foo where `VERTEX` = 'bar'");
    checkRightSyntax("select `EDGE` from foo where `EDGE` = 'bar'");
    checkRightSyntax("select `UPDATE` from foo where `UPDATE` = 'bar'");
    checkRightSyntax("select `UPSERT` from foo where `UPSERT` = 'bar'");
    checkRightSyntax("select `FROM` from foo where `FROM` = 'bar'");
    checkRightSyntax("select `TO` from foo where `TO` = 'bar'");
    checkRightSyntax("select `WHERE` from foo where `WHERE` = 'bar'");
    checkRightSyntax("select `WHILE` from foo where `WHILE` = 'bar'");
    checkRightSyntax("select `INTO` from foo where `INTO` = 'bar'");
    checkRightSyntax("select `VALUES` from foo where `VALUES` = 'bar'");
    checkRightSyntax("select `SET` from foo where `SET` = 'bar'");
    checkRightSyntax("select `ADD` from foo where `ADD` = 'bar'");
    checkRightSyntax("select `PUT` from foo where `PUT` = 'bar'");
    checkRightSyntax("select `MERGE` from foo where `MERGE` = 'bar'");
    checkRightSyntax("select `CONTENT` from foo where `CONTENT` = 'bar'");
    checkRightSyntax("select `REMOVE` from foo where `REMOVE` = 'bar'");
    checkRightSyntax("select `INCREMENT` from foo where `INCREMENT` = 'bar'");
    checkRightSyntax("select `AND` from foo where `AND` = 'bar'");
    checkRightSyntax("select `OR` from foo where `OR` = 'bar'");
    checkRightSyntax("select `NULL` from foo where `NULL` = 'bar'");
    checkRightSyntax("select `DEFINED` from foo where `DEFINED` = 'bar'");
    checkRightSyntax("select `ORDER` from foo where `ORDER` = 'bar'");
    checkRightSyntax("select `GROUP` from foo where `GROUP` = 'bar'");
    checkRightSyntax("select `BY` from foo where `BY` = 'bar'");
    checkRightSyntax("select `LIMIT` from foo where `LIMIT` = 'bar'");
    checkRightSyntax("select `SKIP2` from foo where `SKIP2` = 'bar'");
    checkRightSyntax("select `OFFSET` from foo where `OFFSET` = 'bar'");
    checkRightSyntax("select `TIMEOUT` from foo where `TIMEOUT` = 'bar'");
    checkRightSyntax("select `ASC` from foo where `ASC` = 'bar'");
    checkRightSyntax("select `AS` from foo where `AS` = 'bar'");
    checkRightSyntax("select `DESC` from foo where `DESC` = 'bar'");
    checkRightSyntax("select `RETURN` from foo where `RETURN` = 'bar'");
    checkRightSyntax("select `BEFORE` from foo where `BEFORE` = 'bar'");
    checkRightSyntax("select `AFTER` from foo where `AFTER` = 'bar'");
    checkRightSyntax("select `LOCK` from foo where `LOCK` = 'bar'");
    checkRightSyntax("select `RECORD` from foo where `RECORD` = 'bar'");
    checkRightSyntax("select `WAIT` from foo where `WAIT` = 'bar'");

    checkRightSyntax("select `identifier` from foo where `identifier` = 'bar'");
    checkRightSyntax("select `select` from foo where `select` = 'bar'");
    checkRightSyntax("select `traverse` from foo where `traverse` = 'bar'");
    checkRightSyntax("select `insert` from foo where `insert` = 'bar'");
    checkRightSyntax("select `create` from foo where `create` = 'bar'");
    checkRightSyntax("select `delete` from foo where `delete` = 'bar'");
    checkRightSyntax("select `vertex` from foo where `vertex` = 'bar'");
    checkRightSyntax("select `edge` from foo where `edge` = 'bar'");
    checkRightSyntax("select `update` from foo where `update` = 'bar'");
    checkRightSyntax("select `upsert` from foo where `upsert` = 'bar'");
    checkRightSyntax("select `from` from foo where `from` = 'bar'");
    checkRightSyntax("select `to` from foo where `to` = 'bar'");
    checkRightSyntax("select `where` from foo where `where` = 'bar'");
    checkRightSyntax("select `while` from foo where `while` = 'bar'");
    checkRightSyntax("select `into` from foo where `into` = 'bar'");
    checkRightSyntax("select `values` from foo where `values` = 'bar'");
    checkRightSyntax("select `set` from foo where `set` = 'bar'");
    checkRightSyntax("select `add` from foo where `add` = 'bar'");
    checkRightSyntax("select `put` from foo where `put` = 'bar'");
    checkRightSyntax("select `merge` from foo where `merge` = 'bar'");
    checkRightSyntax("select `content` from foo where `content` = 'bar'");
    checkRightSyntax("select `remove` from foo where `remove` = 'bar'");
    checkRightSyntax("select `increment` from foo where `increment` = 'bar'");
    checkRightSyntax("select `and` from foo where `and` = 'bar'");
    checkRightSyntax("select `or` from foo where `or` = 'bar'");
    checkRightSyntax("select `null` from foo where `null` = 'bar'");
    checkRightSyntax("select `defined` from foo where `defined` = 'bar'");
    checkRightSyntax("select `order` from foo where `order` = 'bar'");
    checkRightSyntax("select `group` from foo where `group` = 'bar'");
    checkRightSyntax("select `by` from foo where `by` = 'bar'");
    checkRightSyntax("select `limit` from foo where `limit` = 'bar'");
    checkRightSyntax("select `skip2` from foo where `skip2` = 'bar'");
    checkRightSyntax("select `offset` from foo where `offset` = 'bar'");
    checkRightSyntax("select `timeout` from foo where `timeout` = 'bar'");
    checkRightSyntax("select `asc` from foo where `asc` = 'bar'");
    checkRightSyntax("select `as` from foo where `as` = 'bar'");
    checkRightSyntax("select `desc` from foo where `desc` = 'bar'");
    checkRightSyntax("select `return` from foo where `return` = 'bar'");
    checkRightSyntax("select `before` from foo where `before` = 'bar'");
    checkRightSyntax("select `after` from foo where `after` = 'bar'");
    checkRightSyntax("select `lock` from foo where `lock` = 'bar'");
    checkRightSyntax("select `record` from foo where `record` = 'bar'");
    checkRightSyntax("select `wait` from foo where `wait` = 'bar'");
    checkRightSyntax("select `retry` from foo where `retry` = 'bar'");
    checkRightSyntax("select `let` from foo where `let` = 'bar'");
    checkRightSyntax("select `nocache` from foo where `nocache` = 'bar'");
    checkRightSyntax("select `unsafe` from foo where `unsafe` = 'bar'");
    checkRightSyntax("select `parallel` from foo where `parallel` = 'bar'");
    checkRightSyntax("select `strategy` from foo where `strategy` = 'bar'");
    checkRightSyntax("select `depth_first` from foo where `depth_first` = 'bar'");
    checkRightSyntax("select `breadth_first` from foo where `breadth_first` = 'bar'");
    checkRightSyntax("select `near` from foo where `near` = 'bar'");
    checkRightSyntax("select `within` from foo where `within` = 'bar'");
    checkRightSyntax("select `unwind` from foo where `unwind` = 'bar'");
    checkRightSyntax("select `maxdepth` from foo where `maxdepth` = 'bar'");
    checkRightSyntax("select `not` from foo where `not` = 'bar'");
    checkRightSyntax("select `in` from foo where `in` = 'bar'");
    checkRightSyntax("select `like` from foo where `like` = 'bar'");
    checkRightSyntax("select `is` from foo where `is` = 'bar'");
    checkRightSyntax("select `between` from foo where `between` = 'bar'");
    checkRightSyntax("select `contains` from foo where `contains` = 'bar'");
    checkRightSyntax("select `containsall` from foo where `containsall` = 'bar'");
    checkRightSyntax("select `containskey` from foo where `containskey` = 'bar'");
    checkRightSyntax("select `containsvalue` from foo where `containsvalue` = 'bar'");
    checkRightSyntax("select `containstext` from foo where `containstext` = 'bar'");
    checkRightSyntax("select `matches` from foo where `matches` = 'bar'");
    checkRightSyntax("select `key` from foo where `key` = 'bar'");
    checkRightSyntax("select `instanceof` from foo where `instanceof` = 'bar'");
    checkRightSyntax("select `bucket` from foo where `bucket` = 'bar'");

    checkRightSyntax("select `foo-bar` from foo where `bucket` = 'bar'");

    checkWrongSyntax("select `bucket from foo where `bucket` = 'bar'");
    checkWrongSyntax("select `bucket from foo where bucket` = 'bar'");
  }

  @Test
  public void testReturn() {
    checkRightSyntax("select from ouser timeout 1 exception");
    checkRightSyntax("select from ouser timeout 1 return");
  }

  @Test
  public void testFlatten() {
    SelectStatement stm = (SelectStatement) checkRightSyntax("select from ouser where name = 'foo'");
    List<AndBlock> flattened = stm.whereClause.flatten();
    assertTrue(((BinaryCondition) flattened.get(0).subBlocks.get(0)).left.isBaseIdentifier());
    assertFalse(((BinaryCondition) flattened.get(0).subBlocks.get(0)).right.isBaseIdentifier());
    assertFalse(((BinaryCondition) flattened.get(0).subBlocks.get(0)).left.isEarlyCalculated());
    assertTrue(((BinaryCondition) flattened.get(0).subBlocks.get(0)).right.isEarlyCalculated());
  }

  @Test
  public void testParamWithMatches() {
    // issue #5229
    checkRightSyntax("select from Person where name matches :param1");
  }

  @Test
  public void testInstanceOfE() {
    // issue #5212
    checkRightSyntax("select from Friend where @type instanceof 'E'");
  }

  @Test
  public void testSelectFromBucketNumber() {
    checkRightSyntax("select from bucket:12");
  }

  @Test
  public void testReservedWordsAsNamedParams() {
    // issue #5493
    checkRightSyntax("select from V limit :limit");
  }

  @Test
  public void testJsonQuoting() {
    // issue #5911
    checkRightSyntax("SELECT '\\/\\/'");
    checkRightSyntax("SELECT \"\\/\\/\"");
  }

  @Test
  public void testSkipLimitInQueryWithNoTarget() {
    // issue #5589
    checkRightSyntax("SELECT eval('$TotalListsQuery[0].Count') AS TotalLists\n"
        + "   LET $TotalListsQuery = ( SELECT Count(1) AS Count FROM ContactList WHERE Account=#20:1 AND EntityInfo.State=0)\n" + " LIMIT 1");

    checkRightSyntax("SELECT eval('$TotalListsQuery[0].Count') AS TotalLists\n"
        + "   LET $TotalListsQuery = ( SELECT Count(1) AS Count FROM ContactList WHERE Account=#20:1 AND EntityInfo.State=0)\n" + " SKIP 10 LIMIT 1");
  }

  @Test
  public void testQuotedBacktick() {
    checkRightSyntax("SELECT \"\" as `bla\\`bla` from foo");
  }

  @Test
  public void testParamConcat() {
    // issue #6049
    checkRightSyntax("Select * From ACNodeAuthentication where acNodeID like ? ");
    checkRightSyntax("Select * From ACNodeAuthentication where acNodeID like ? + '%'");
    checkRightSyntax("Select * From ACNodeAuthentication where acNodeID like \"%\" + ? + '%'");
  }

  @Test
  public void testAppendParams() {
    checkRightSyntax("select from User where Account.Name like :name + '%'");
  }

  @Test
  public void testLetMatch() {
    checkRightSyntax("select $a let $a = (MATCH {type:Foo, as:bar, where:(name = 'foo')} return $elements)");
  }

  @Test
  public void testDistinct() {
    checkRightSyntax("select distinct(foo) from V");
    checkRightSyntax("select distinct foo, bar, baz from V");
  }

  @Test
  public void testRange() {
    checkRightSyntax("select foo[1..5] from V");
    checkRightSyntax("select foo[1...5] from V");
    checkRightSyntax("select foo[?..?] from V");
    checkRightSyntax("select foo[?...?] from V");
    checkRightSyntax("select foo[:a..:b] from V");
    checkRightSyntax("select foo[:a...:b] from V");
    checkWrongSyntax("select foo[1....5] from V");
  }

  @Test
  public void testRidString() {
    checkRightSyntax("select \"@rid\" as v from V");
    checkRightSyntax("select {\"@rid\": \"#12:0\"} as v from V");
    //System.out.println(stm2);
  }

  @Test
  public void testFromAsNamedParam() {
    checkRightSyntax("select from V where fromDate = :from");
  }

  @Test
  public void testNestedProjections() {
    checkRightSyntax("select foo:{*} from V");
    checkRightSyntax("select foo:{name, surname, address:{*}} from V");
    checkRightSyntax("select foo:{!name} from V");
    checkRightSyntax("select foo:{!out_*} from V");
    checkRightSyntax("select foo:{!out_*, !in_*} from V");
    checkRightSyntax("select foo:{*, !out_*, !in_*} from V");
  }

  @Test
  public void testCollectionFilteringByValue() {
    checkRightSyntax("select foo[= 'foo'] from V");
    checkRightSyntax("select foo[like '%foo'] from V");
    checkRightSyntax("select foo[> 2] from V");
    checkRightSyntax("select foo[> 2][< 5] from V");

    checkRightSyntax("select foo[ IN ['a', 'b']] from V");
    checkRightSyntax("select bar[IN (select from foo) ] from V");
    checkRightSyntax("select bar[IN $a ] from V LET $a = (SELECT FROM V)");

    checkRightSyntax("select foo[not IN ['a', 'b']] from V");
    checkRightSyntax("select bar[not IN (select from foo) ] from V");
    checkRightSyntax("select bar[not IN $a ] from V LET $a = (SELECT FROM V)");
  }

  @Test
  public void testContainsAny() {
    checkRightSyntax("select from V WHERE foo containsany ['foo', 'bar']");
    checkRightSyntax("select from V WHERE foo CONTAINSANY ['foo', 'bar']");
    checkWrongSyntax("select from V WHERE foo CONTAINSANY ");
  }

  @Test
  public void testQueryIndex() {
    checkRightSyntax("select from index:foo WHERE key = 'foo'");
    checkRightSyntax("select from index:foo WHERE key = ['foo', 'bar']");
    checkRightSyntax("select from index:foo WHERE key > 10");
    checkRightSyntax("select from index:foo WHERE key CONTAINS 'foo'");
    checkRightSyntax("select from index:foo WHERE key BETWEEN 'bar' AND 'foo'");
  }

  protected SqlParser getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes(DatabaseFactory.getDefaultCharset()));
    return new SqlParser(is);
  }
}
