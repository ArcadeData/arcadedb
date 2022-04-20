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

import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.fail;

public class MatchStatementTest {

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
        fail();
      }
      return result;
    } catch (Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  @Test
  public void testWrongFilterKey() {
    checkWrongSyntax("MATCH {clasx: 'V'} RETURN foo");
  }

  @Test
  public void testBasicMatch() {
    checkRightSyntax("MATCH { type: 'V', as: foo} RETURN foo");
  }

  @Test
  public void testNoReturn() {
    checkWrongSyntax("MATCH { type: 'V', as: foo}");
  }

  @Test
  public void testSingleMethod() {
    checkRightSyntax("MATCH { type: 'V', as: foo}.out() RETURN foo");
  }

  @Test
  public void testArrowsNoBrackets() {
    checkWrongSyntax("MATCH {}-->-->{as:foo} RETURN foo");
  }

  @Test
  public void testSingleMethodAndFilter() {
    checkRightSyntax("MATCH { type: 'V', as: foo}.out(){ type: 'V', as: bar} RETURN foo");
    checkRightSyntax("MATCH { type: 'V', as: foo}-E->{ type: 'V', as: bar} RETURN foo");
    checkRightSyntax("MATCH { type: 'V', as: foo}-->{ type: 'V', as: bar} RETURN foo");
  }

  @Test
  public void testLongPath() {
    checkRightSyntax(
        "MATCH { type: 'V', as: foo}.out().in('foo').both('bar').out(){as: bar} RETURN foo");

    checkRightSyntax("MATCH { type: 'V', as: foo}-->{}<-foo-{}-bar-{}-->{as: bar} RETURN foo");
  }

  @Test
  public void testLongPath2() {
    checkRightSyntax(
        "MATCH { type: 'V', as: foo}.out().in('foo'){}.both('bar'){ TYPE: 'bar'}.out(){as: bar} RETURN foo");
  }

  @Test
  public void testFilterTypes() {
    StringBuilder query = new StringBuilder();
    query.append("MATCH {");
    query.append("   type: 'v', ");
    query.append("   as: foo, ");
    query.append("   where: (name = 'foo' and surname = 'bar' or aaa in [1,2,3]), ");
    query.append("   maxDepth: 10 ");
    query.append("} return foo");
    checkRightSyntax(query.toString());
  }

  @Test
  public void testFilterTypes2() {
    StringBuilder query = new StringBuilder();
    query.append("MATCH {");
    query.append("   types: ['V', 'E'], ");
    query.append("   as: foo, ");
    query.append("   where: (name = 'foo' and surname = 'bar' or aaa in [1,2,3]), ");
    query.append("   maxDepth: 10 ");
    query.append("} return foo");
    checkRightSyntax(query.toString());
  }

  @Test
  public void testMultiPath() {
    StringBuilder query = new StringBuilder();
    query.append("MATCH {}");
    query.append("  .(out().in(){type:'v'}.both('Foo')){maxDepth: 3}.out() return foo");
    checkRightSyntax(query.toString());
  }

  @Test
  public void testMultiPathArrows() {
    StringBuilder query = new StringBuilder();
    query.append("MATCH {}");
    query.append("  .(-->{}<--{type:'v'}--){maxDepth: 3}-->{} return foo");
    checkRightSyntax(query.toString());
  }

  @Test
  public void testMultipleMatches() {
    String query = "MATCH {type: 'V', as: foo}.out(){type: 'V', as: bar}, ";
    query += " {type: 'V', as: foo}.out(){type: 'V', as: bar},";
    query += " {type: 'V', as: foo}.out(){type: 'V', as: bar} RETURN foo";
    checkRightSyntax(query);
  }

  @Test
  public void testMultipleMatchesArrow() {
    String query = "MATCH {type: 'V', as: foo}-->{type: 'V', as: bar}, ";
    query += " {type: 'V', as: foo}-->{type: 'V', as: bar},";
    query += " {type: 'V', as: foo}-->{type: 'V', as: bar} RETURN foo";
    checkRightSyntax(query);
  }

  @Test
  public void testWhile() {
    checkRightSyntax("MATCH {type: 'V', as: foo}.out(){while:($depth<4), as:bar} RETURN bar ");
  }

  @Test
  public void testWhileArrow() {
    checkRightSyntax("MATCH {type: 'V', as: foo}-->{while:($depth<4), as:bar} RETURN bar ");
  }

  @Test
  public void testLimit() {
    checkRightSyntax("MATCH {type: 'V'} RETURN foo limit 10");
  }

  @Test
  public void testReturnJson() {
    checkRightSyntax("MATCH {type: 'V'} RETURN {'name':'foo', 'value': bar}");
  }

  @Test
  public void testOptional() {
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}-->{}<-foo-{}-bar-{}-->{as: bar, optional:true} RETURN foo");
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}-->{}<-foo-{}-bar-{}-->{as: bar, optional:false} RETURN foo");
  }

  @Test
  public void testOrderBy() {
    checkRightSyntax("MATCH {type: 'V', as: foo}-->{} RETURN foo ORDER BY foo");
    checkRightSyntax("MATCH {type: 'V', as: foo}-->{} RETURN foo ORDER BY foo limit 10");
  }

  @Test
  public void testNestedProjections() {
    checkRightSyntax("MATCH {type: 'V', as: foo}-->{} RETURN foo:{name, surname}");
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}-->{as:bar} RETURN foo:{name, surname} as bloo, bar:{*}");
  }

  @Test
  public void testUnwind() {
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}-->{as:bar} RETURN foo.name, bar.name as x unwind x");
  }

  @Test
  public void testDepthAlias() {
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}-->{as:bar, while:($depth < 2), depthAlias: depth} RETURN depth");
  }

  @Test
  public void testPathAlias() {
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}-->{as:bar, while:($depth < 2), pathAlias: barPath} RETURN barPath");
  }

  @Test
  public void testBucketTarget() {
    checkRightSyntax("MATCH {bucket:v, as: foo} RETURN $elements");
    checkRightSyntax("MATCH {bucket:12, as: foo} RETURN $elements");
    checkRightSyntax("MATCH {bucket: v, as: foo} RETURN $elements");
    checkRightSyntax("MATCH {bucket: `v`, as: foo} RETURN $elements");
    checkRightSyntax("MATCH {bucket:`v`, as: foo} RETURN $elements");
    checkRightSyntax("MATCH {bucket: 12, as: foo} RETURN $elements");

    checkWrongSyntax("MATCH {bucket: 12.1, as: foo} RETURN $elements");
  }

  @Test
  public void testNot() {
    checkRightSyntax("MATCH {bucket:v, as: foo}, NOT {as:foo}-->{as:bar} RETURN $elements");
  }

  @Test
  public void testSkip() {
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}-->{as:bar} RETURN foo.name, bar.name skip 10 limit 10");
  }

  @Test
  public void testFieldTraversal() {
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}.toBar{as:bar} RETURN foo.name, bar.name skip 10 limit 10");
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}.toBar{as:bar}.out(){as:c} RETURN foo.name, bar.name skip 10 limit 10");
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}.toBar.baz{as:bar} RETURN foo.name, bar.name skip 10 limit 10");
    checkRightSyntax(
        "MATCH {type: 'V', as: foo}.toBar.out(){as:bar} RETURN foo.name, bar.name skip 10 limit 10");
  }

  protected SqlParser getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    SqlParser osql = new SqlParser(null, is);
    return osql;
  }
}
