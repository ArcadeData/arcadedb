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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7800: two re-render defects (an unparseable {@code PERIODIC} keyword and an
 * unquoted URL that loses its quotes) plus a batch of DDL statements whose {@code copy()} either threw
 * {@code UnsupportedOperationException} or silently dropped a field.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7800ReRenderAndCopyTest extends AbstractParserTest {

  /** Item 1: CREATE MATERIALIZED VIEW ... REFRESH EVERY n UNIT used to re-render as the unparseable "REFRESH PERIODIC EVERY n UNIT". */
  @Test
  void createMaterializedViewPeriodicRefreshReRenders() {
    final Statement result = (Statement) checkRightSyntax(
        "CREATE MATERIALIZED VIEW AlterPMView AS SELECT name FROM Account REFRESH EVERY 1 SECOND");
    assertThat(result.toString()).doesNotContain("PERIODIC");
    assertThat(result.toString()).contains("REFRESH EVERY 1 SECOND");
  }

  /** Item 2: a URL that arrived as a quoted STRING_LITERAL without a recognized scheme must be re-quoted, not printed raw. */
  @Test
  void backupDatabaseWithPlainStringUrlReRenders() {
    final Statement result = (Statement) checkRightSyntax("BACKUP DATABASE 'mybackup.zip'");
    assertThat(result.toString()).isEqualTo("BACKUP DATABASE 'mybackup.zip'");
  }

  @Test
  void backupDatabaseWithSchemeUrlIsRenderedUnquoted() {
    final Statement result = (Statement) checkRightSyntax("BACKUP DATABASE file://mybackup.zip");
    assertThat(result.toString()).isEqualTo("BACKUP DATABASE file://mybackup.zip");
  }

  /**
   * Review finding on the initial version of this fix: {@code urlString} is stored UN-DECODED (only the outer
   * quotes are stripped, the lexer's escape sequences are not resolved), so re-escaping it in {@code toString()}
   * double-encoded an already-escaped {@code \'}, growing the literal on every render/reparse cycle instead of
   * round-tripping. Rendering the original quoted literal verbatim (like {@code CreateTriggerStatement.actionCodeQuoted})
   * must reproduce the exact source and stay stable no matter how many times it is rendered.
   */
  @Test
  void backupDatabaseWithEscapedQuoteInUrlRoundTripsExactlyAndStably() {
    final String sql = "BACKUP DATABASE 'it\\'s a test.zip'";
    final Statement result = (Statement) checkRightSyntax(sql);
    assertThat(result.toString()).isEqualTo(sql);

    // re-parse what was rendered and render it again: must be byte-for-byte identical, not growing
    final Statement reparsed = new com.arcadedb.query.sql.antlr.SQLAntlrParser(null).parse(result.toString());
    assertThat(reparsed.toString()).isEqualTo(sql);
  }

  /**
   * CodeRabbit follow-up (fourth round): {@code urlString} used to be the un-decoded literal body (quotes stripped,
   * escapes untouched), and {@code BackupDatabaseStatement} uses {@code Url.getUrlString()} as the actual backup
   * file path - so a URL containing an escape sequence would target a path with a literal backslash-n instead of
   * the real character. {@code urlString} is now decoded the same way {@code DefineFunctionStatement.code} already
   * is, while {@code quotedLiteral} stays the raw source text for rendering.
   */
  @Test
  void backupDatabaseUrlIsDecodedForExecutionButRendersVerbatim() {
    final BackupDatabaseStatement stmt = (BackupDatabaseStatement) new com.arcadedb.query.sql.antlr.SQLAntlrParser(null)
        .parse("BACKUP DATABASE 'line1\\nline2'");

    assertThat(stmt.url.getUrlString()).isEqualTo("line1\nline2");
    assertThat(stmt.toString()).isEqualTo("BACKUP DATABASE 'line1\\nline2'");
  }

  /**
   * CodeRabbit follow-up: {@code quotedLiteral} is only set by the parser, but {@code Url}'s two-arg constructor is
   * public. A {@code Url} built any other way (with a plain, non-scheme-prefixed value and no {@code quotedLiteral})
   * must still be quoted on render instead of printed raw and unparseable - the same fallback gap as
   * {@code CreateTriggerStatement.actionCode}/{@code actionCodeQuoted}.
   */
  @Test
  void urlBuiltDirectlyWithoutAQuotedLiteralIsStillQuotedOnRender() {
    final Url url = new Url("mybackup.zip");
    final StringBuilder builder = new StringBuilder();
    url.toString(null, builder);
    assertThat(builder.toString()).isEqualTo("'mybackup.zip'");
  }

  /**
   * Code review follow-up (third round): {@code isRecognizedScheme} checked only the scheme prefix, not whether
   * the rest of the value could actually re-lex as the grammar's {@code FILE_URL} token ({@code URL_CHAR} excludes
   * space among other characters), so a directly-constructed {@code file://} URL containing a space would render
   * unquoted and fail to reparse.
   */
  @Test
  void urlBuiltDirectlyWithSchemeAndDisallowedCharacterIsStillQuoted() {
    final Url url = new Url("file://my backup.zip");
    final StringBuilder builder = new StringBuilder();
    url.toString(null, builder);
    assertThat(builder.toString()).isEqualTo("'file://my backup.zip'");
  }

  /**
   * Code review follow-up (second round): the fallback quoting escaped only the quote character and the
   * backslash, but the grammar's STRING_LITERAL rule also forbids a raw CR/LF inside the literal body.
   */
  @Test
  void urlBuiltDirectlyWithNewlineIsEscapedAndReparses() {
    final Url url = new Url("line1\nline2");
    final StringBuilder builder = new StringBuilder();
    url.toString(null, builder);
    assertThat(builder.toString()).doesNotContain("\n");

    // must reparse without throwing: a raw newline inside the '...' literal is a lexer/syntax error
    new com.arcadedb.query.sql.antlr.SQLAntlrParser(null).parse("BACKUP DATABASE " + builder);
  }

  /**
   * Item 3: DROP INDEX copy() silently dropped ifExists. CodeRabbit also found that the class's (pre-existing,
   * manual) equals()/hashCode() never included it either, so the strict and idempotent forms compared equal.
   */
  @Test
  void dropIndexCopyPreservesIfExists() {
    final DropIndexStatement stmt = (DropIndexStatement) new com.arcadedb.query.sql.antlr.SQLAntlrParser(null)
        .parse("DROP INDEX Foo IF EXISTS");
    final DropIndexStatement copy = stmt.copy();
    assertThat(copy.ifExists).isTrue();
    assertThat(copy.toString()).isEqualTo(stmt.toString());
    assertThat(copy).isEqualTo(stmt);

    final DropIndexStatement strict = (DropIndexStatement) new com.arcadedb.query.sql.antlr.SQLAntlrParser(null)
        .parse("DROP INDEX Foo");
    assertThat(stmt).isNotEqualTo(strict);
    assertThat(stmt.hashCode()).isNotEqualTo(strict.hashCode());
  }

  /**
   * Item 3: TraverseStatement.copy() dropped skip. The grammar never exposes SKIP on TRAVERSE (only LIMIT does),
   * so this is set directly through the setter, exactly as the issue's own scope note says: latent, reachable only
   * through the field/setter, not through SQL text. The code review's third round also found that equals()/hashCode()
   * still omitted skip even after copy() was fixed to preserve it - the same "silently drop a field" class of bug
   * as DropIndexStatement.ifExists/UpdateStatement.returnCount above.
   */
  @Test
  void traverseCopyPreservesSkip() {
    final TraverseStatement stmt = (TraverseStatement) new com.arcadedb.query.sql.antlr.SQLAntlrParser(null)
        .parse("TRAVERSE out() FROM V LIMIT 10");
    final Skip skip = new Skip();
    skip.num = new PInteger().setValue(5);
    stmt.setSkip(skip);

    final TraverseStatement copy = (TraverseStatement) stmt.copy();
    assertThat(copy.getSkip()).isNotNull();
    assertThat(copy.getSkip().getValue(null)).isEqualTo(5);
    assertThat(copy).isEqualTo(stmt);
    assertThat(copy.hashCode()).isEqualTo(stmt.hashCode());

    final TraverseStatement noSkip = (TraverseStatement) new com.arcadedb.query.sql.antlr.SQLAntlrParser(null)
        .parse("TRAVERSE out() FROM V LIMIT 10");
    assertThat(stmt).isNotEqualTo(noSkip);
    assertThat(stmt.hashCode()).isNotEqualTo(noSkip.hashCode());
  }

  /**
   * Item 3: UpdateStatement.copy() dropped returnCount. CodeRabbit also found that the class's (pre-existing,
   * manual) equals()/hashCode() never included it either, so RETURN COUNT compared equal to no RETURN clause at all.
   */
  @Test
  void updateCopyPreservesReturnCount() {
    final UpdateStatement stmt = (UpdateStatement) new com.arcadedb.query.sql.antlr.SQLAntlrParser(null)
        .parse("UPDATE Foo SET a = 1 RETURN COUNT");
    final UpdateStatement copy = stmt.copy();
    assertThat(copy.returnCount).isTrue();
    assertThat(copy.toString()).isEqualTo(stmt.toString());
    assertThat(copy).isEqualTo(stmt);

    final UpdateStatement noReturn = (UpdateStatement) new com.arcadedb.query.sql.antlr.SQLAntlrParser(null)
        .parse("UPDATE Foo SET a = 1");
    assertThat(stmt).isNotEqualTo(noReturn);
    assertThat(stmt.hashCode()).isNotEqualTo(noReturn.hashCode());
  }

  /** Item 3: the 12 DDL statements that used to throw "IMPLEMENT copy() ON ..." must now copy every field. */
  @Test
  void everyPreviouslyUncopyableDdlStatementNowCopies() {
    checkCopyRoundTrips("CREATE MATERIALIZED VIEW V1 AS SELECT FROM Account REFRESH MANUAL");
    checkCopyRoundTrips("ALTER MATERIALIZED VIEW V1 REFRESH INCREMENTAL");
    checkCopyRoundTrips("DROP MATERIALIZED VIEW IF EXISTS V1");
    checkCopyRoundTrips("REFRESH MATERIALIZED VIEW V1");
    checkCopyRoundTrips("CREATE GRAPH ANALYTICAL VIEW IF NOT EXISTS G1 VERTEX TYPES (Person) EDGE TYPES (Knows)");
    checkCopyRoundTrips("ALTER GRAPH ANALYTICAL VIEW G1 UPDATE MODE SYNCHRONOUS");
    checkCopyRoundTrips("DROP GRAPH ANALYTICAL VIEW IF EXISTS G1");
    checkCopyRoundTrips("REBUILD GRAPH ANALYTICAL VIEW G1");
    checkCopyRoundTrips("DROP CONTINUOUS AGGREGATE IF EXISTS CA1");
    checkCopyRoundTrips("REFRESH CONTINUOUS AGGREGATE CA1");
    checkCopyRoundTrips("ALIGN DATABASE");
    checkCopyRoundTrips("CHECK DATABASE TYPE Customer, Order");
  }

  private void checkCopyRoundTrips(final String sql) {
    final Statement stmt = new com.arcadedb.query.sql.antlr.SQLAntlrParser(null).parse(sql);
    final Statement copy = stmt.copy();
    assertThat(copy).as("copy() of '%s' must not be null", sql).isNotNull();
    assertThat(copy.getClass()).isEqualTo(stmt.getClass());
    assertThat(copy.toString()).as("copy() of '%s' must preserve every field", sql).isEqualTo(stmt.toString());
    // every one of these classes now has a getIdentityElements() (or, for AlignDatabaseStatement, a manual
    // equals()) override, so a copy must also compare content-equal to its source, not just render the same text
    assertThat(copy).as("copy() of '%s' must compare equal to its source", sql).isEqualTo(stmt);
  }
}
