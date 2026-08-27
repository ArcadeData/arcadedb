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
package com.arcadedb.console;

import org.jline.reader.Parser.ParseContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the statement splitting done by the console before handing over the single commands to the engine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TerminalParserTest {
  private final TerminalParser parser = new TerminalParser();

  private List<String> split(final String line) {
    return parser.parse(line, 0, ParseContext.ACCEPT_LINE).words();
  }

  @Test
  void plainStatementsAreSplitOnSemicolon() {
    assertThat(split("SELECT 1; SELECT 2")).containsExactly("SELECT 1", " SELECT 2");
  }

  @Test
  void semicolonInsideStringIsNotADelimiter() {
    assertThat(split("SELECT 'a;b'")).containsExactly("SELECT 'a;b'");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/5457: a semicolon inside a line comment used to terminate the
   * statement, leaving the rest of the comment to be executed as a command.
   */
  @Test
  void semicolonInsideLineCommentIsNotADelimiter() {
    assertThat(split("SELECT 2  -- A comment with a semicolon ; errors here")).containsExactly("SELECT 2  ");
  }

  @Test
  void lineCommentEndsAtNewLine() {
    assertThat(split("SELECT 1 -- comment ; here\nSELECT 2")).containsExactly("SELECT 1 \nSELECT 2");
    assertThat(split("SELECT 1 -- comment ; here\n; SELECT 2")).containsExactly("SELECT 1 \n", " SELECT 2");
  }

  @Test
  void wholeLineCommentIsDropped() {
    assertThat(split("-- just a comment ; with a semicolon")).isEmpty();
  }

  @Test
  void semicolonInsideBlockCommentIsNotADelimiter() {
    assertThat(split("SELECT /* a ; comment */ 1")).containsExactly("SELECT  1");
    assertThat(split("SELECT 1 /* multi\nline ; comment */; SELECT 2")).containsExactly("SELECT 1 ", " SELECT 2");
  }

  @Test
  void bracesInsideCommentsDoNotAffectTheSplitting() {
    assertThat(split("SELECT 1 -- { unbalanced brace\n; SELECT 2")).containsExactly("SELECT 1 \n", " SELECT 2");
  }

  @Test
  void commentMarkersInsideStringsArePreserved() {
    assertThat(split("SELECT 'a -- b; c'")).containsExactly("SELECT 'a -- b; c'");
    assertThat(split("SELECT \"a /* b ; c */ d\"")).containsExactly("SELECT \"a /* b ; c */ d\"");
  }

  /**
   * The engine grammar reads a line comment as <code>'--' ' '</code>, so two dashes glued to an operand stay arithmetic.
   */
  @Test
  void doubleDashWithoutSpaceIsNotAComment() {
    assertThat(split("SELECT 1--2; SELECT 3")).containsExactly("SELECT 1--2", " SELECT 3");
  }

  @Test
  void singleDashIsNotAComment() {
    assertThat(split("SELECT 1 - 2; SELECT 3")).containsExactly("SELECT 1 - 2", " SELECT 3");
  }

  @Test
  void unterminatedBlockCommentSwallowsTheRest() {
    assertThat(split("SELECT 1; SELECT 2 /* unterminated ; comment")).containsExactly("SELECT 1", " SELECT 2 ");
  }

  @Test
  void divisionIsNotABlockComment() {
    assertThat(split("SELECT 4 / 2; SELECT 3")).containsExactly("SELECT 4 / 2", " SELECT 3");
  }

  @Test
  void doubleSlashIsNotACommentWithSql() {
    assertThat(split("SELECT 4 // 2; SELECT 3")).containsExactly("SELECT 4 // 2", " SELECT 3");
  }

  /**
   * With Cypher, Gremlin and Mongo the line comment is `//`, while `--` is a legal undirected relationship in a Cypher pattern.
   */
  @Test
  void lineCommentFollowsTheLanguage() {
    parser.setLanguage("cypher");

    assertThat(split("MATCH (a) -- (b) RETURN a; RETURN 1")).containsExactly("MATCH (a) -- (b) RETURN a", " RETURN 1");
    assertThat(split("MATCH (a) // a comment ; here\nRETURN a")).containsExactly("MATCH (a) \nRETURN a");
    assertThat(split("// only a comment ; here")).isEmpty();
    assertThat(split("MATCH (a) /* block ; comment */ RETURN a")).containsExactly("MATCH (a)  RETURN a");

    parser.setLanguage("sqlscript");
    assertThat(split("SELECT 1 -- a comment ; here")).containsExactly("SELECT 1 ");
  }

  @Test
  void jsonContentIsNotSplit() {
    assertThat(split("INSERT INTO doc CONTENT {\"a\": \"b;c\", \"d\": 1}")).containsExactly(
        "INSERT INTO doc CONTENT {\"a\": \"b;c\", \"d\": 1}");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/6392: one closing brace too many used to drive the brace depth below
   * zero, and from there no semicolon could ever match the `depth == 0` test again, so every following command was appended to
   * the malformed one instead of being executed on its own.
   */
  @Test
  void unbalancedClosingBraceDoesNotDisableTheSeparator() {
    assertThat(split("INSERT INTO doc CONTENT {\"a\":{\"b\":1}}}; SELECT 1; SELECT 2")).containsExactly(
        "INSERT INTO doc CONTENT {\"a\":{\"b\":1}}}", " SELECT 1", " SELECT 2");
  }

  @Test
  void closingBraceWithNothingOpenIsJustText() {
    assertThat(split("SELECT 1 }; SELECT 2")).containsExactly("SELECT 1 }", " SELECT 2");
  }

  /**
   * Every stray brace used to dig the depth one level deeper, so the damage survived any number of well formed JSON objects
   * coming after it.
   */
  @Test
  void strayBracesDoNotAccumulate() {
    assertThat(split("} ; } ; SELECT 1")).containsExactly("} ", " } ", " SELECT 1");
    assertThat(split("SELECT 1 }; INSERT INTO doc CONTENT {\"a\": 1}; SELECT 2")).containsExactly("SELECT 1 }",
        " INSERT INTO doc CONTENT {\"a\": 1}", " SELECT 2");
  }

  @Test
  void unbalancedClosingBraceInsideAStringIsNotCounted() {
    assertThat(split("SELECT '}}}' ; SELECT 1")).containsExactly("SELECT '}}}' ", " SELECT 1");
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/6439: an unclosed '{' drives the brace depth to 1 and, since the
   * delimiter branch requires depth zero, no semicolon after it can separate anything for the rest of the text.
   */
  @Test
  void unbalancedOpeningBraceSwallowsFollowingCommands() {
    final String line = "INSERT INTO doc CONTENT {\"a\": 1 ; SELECT 1; SELECT 2";
    assertThat(split(line)).containsExactly(line);
    assertThat(parser.getUnbalancedBraceOffset()).isEqualTo(line.indexOf('{'));
  }

  @Test
  void balancedBracesReportNoUnbalancedOffset() {
    split("INSERT INTO doc CONTENT {\"a\": 1}; SELECT 1; SELECT 2");
    assertThat(parser.getUnbalancedBraceOffset()).isEqualTo(-1);
  }

  @Test
  void nestedUnbalancedBraceReportsTheOutermostOffset() {
    final String line = "INSERT INTO doc CONTENT {\"a\": {\"b\": 1} ; SELECT 1";
    split(line);
    assertThat(parser.getUnbalancedBraceOffset()).isEqualTo(line.indexOf('{'));
  }

  @Test
  void unbalancedBraceInsideAStringIsNotCounted() {
    split("SELECT '{' ; SELECT 1");
    assertThat(parser.getUnbalancedBraceOffset()).isEqualTo(-1);
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/6827: the parser used to consume one level of backslash escaping
   * from every word before handing it to the query engine, so `'C:\Users\bob'` arrived as `'C:Usersbob'` and was stored
   * that way - silently, with no warning and no error. The engine, not the console, owns string-literal escaping, so the
   * backslash has to reach it as typed.
   */
  @Test
  void aBackslashInsideAStringReachesTheEngineAsTyped() {
    assertThat(split("insert into Doc set winPath = 'C:\\Users\\bob'"))
        .containsExactly("insert into Doc set winPath = 'C:\\Users\\bob'");
  }

  /**
   * Not only inside string literals: `load` takes a file path, and a Windows one is nothing but backslashes.
   */
  @Test
  void aBackslashOutsideAStringIsKeptToo() {
    assertThat(split("load C:\\scripts\\init.sql")).containsExactly("load C:\\scripts\\init.sql");
  }

  /**
   * The escape character keeps doing its structural job: an escaped quote must not close the string, otherwise the rest
   * of the statement would be parsed as if it were outside one.
   */
  @Test
  void anEscapedQuoteStillDoesNotCloseTheStringAndKeepsItsBackslash() {
    assertThat(split("select from V where name = 'it\\'s'; select 1"))
        .containsExactly("select from V where name = 'it\\'s'", " select 1");
  }

  @Test
  void anEscapedSemicolonStillDoesNotSplitTheCommand() {
    assertThat(split("select 'a' \\; select 'b'")).containsExactly("select 'a' \\; select 'b'");
  }

  /**
   * A doubled backslash is a single escaped backslash: it used to collapse to one, which turned the literal
   * `'\\'` a user typed for a single backslash into an unterminated-looking oddity for the engine.
   */
  @Test
  void aDoubledBackslashIsNotCollapsed() {
    assertThat(split("insert into Doc set sep = '\\\\'")).containsExactly("insert into Doc set sep = '\\\\'");
  }

  @Test
  void aTrailingBackslashIsKept() {
    assertThat(split("select from V where p = 'a\\")).containsExactly("select from V where p = 'a\\");
  }
}
