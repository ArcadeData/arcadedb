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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.ResultSet;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * toString(), copy() and equals() of parsed statements (render then re-parse, copy then compare).
 * <p>
 * A container for the regression suites of the feature; each nested class keeps its own fixture.
 */
class StatementRenderingTest {
  /**
   * Regression for #6999: MatchStatement.toString() rendered the positive patterns only, so anything that round-trips a MATCH through...
   * <p>
   * Regression test for issue #6999: {@code MatchStatement.toString()} rendered the positive patterns only, so anything
   * that round-trips a MATCH through its text (a materialized view, a continuous aggregate, the statement cache) silently
   * lost every {@code NOT} pattern and widened its result set.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MatchStatementNotPatternToStringTest extends TestHelper {

    /** The documented shape: a friend-of-a-friend who is not already a direct friend. */
    private static final String QUERY = "MATCH {as: a, type: Person}-Friend->{as: b}-Friend->{as: c}, NOT {as: a}-Friend->{as: c} "
        + "RETURN a.name AS name";

    @Test
    void toStringRendersTheNotPatterns() {
      final SQLAntlrParser parser = new SQLAntlrParser(null);
      final MatchStatement original = (MatchStatement) parser.parse(QUERY);
      assertThat(original.getNotMatchExpressions()).hasSize(1);

      final String rendered = original.toString();
      assertThat(rendered).as("the rendered text must carry the negative pattern").contains(", NOT ");

      final MatchStatement reparsed = (MatchStatement) parser.parse(rendered);
      assertThat(reparsed.getNotMatchExpressions()).as("the NOT pattern must survive a parse/render/parse round trip").hasSize(1);
      assertThat(reparsed).isEqualTo(original);
      assertThat(reparsed.toString()).isEqualTo(rendered);
    }

    /**
     * A negative pattern followed by a positive one: the AST builder indexed the sparse list of NOT tokens by pattern
     * position, so it negated the wrong pattern in this shape, and the text it rendered was a different statement.
     */
    @Test
    void aNotPatternFollowedByAPositiveOneIsAssignedToTheRightPattern() {
      final SQLAntlrParser parser = new SQLAntlrParser(null);
      final MatchStatement original = (MatchStatement) parser.parse(
          "MATCH {as: p, type: Person}, NOT {as: p}-Friend->{as: q}, {as: p}-Knows->{as: k}, NOT {as: p}-Blocks->{as: b} RETURN p");

      assertThat(original.getMatchExpressions()).hasSize(2);
      assertThat(original.getMatchExpressions().get(1).toString()).contains("Knows");
      assertThat(original.getNotMatchExpressions()).hasSize(2);
      assertThat(original.getNotMatchExpressions().get(0).toString()).contains("Friend");
      assertThat(original.getNotMatchExpressions().get(1).toString()).contains("Blocks");

      final String rendered = original.toString();
      final MatchStatement reparsed = (MatchStatement) parser.parse(rendered);
      assertThat(reparsed.getMatchExpressions()).hasSize(2);
      assertThat(reparsed.getNotMatchExpressions()).hasSize(2);
      assertThat(reparsed).isEqualTo(original);
      assertThat(reparsed.toString()).isEqualTo(rendered);
    }

    @Test
    void theReRenderedStatementReturnsTheSameRowsAsTheOriginal() {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("Friend");

      database.transaction(() -> {
        // ALICE -> BOB -> CAROL, WITH NO DIRECT ALICE -> CAROL EDGE
        final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
        final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
        final MutableVertex carol = database.newVertex("Person").set("name", "Carol").save();
        alice.newEdge("Friend", bob).save();
        bob.newEdge("Friend", carol).save();
        // DAVE -> EVE -> FRANK, AND DAVE IS ALREADY A DIRECT FRIEND OF FRANK
        final MutableVertex dave = database.newVertex("Person").set("name", "Dave").save();
        final MutableVertex eve = database.newVertex("Person").set("name", "Eve").save();
        final MutableVertex frank = database.newVertex("Person").set("name", "Frank").save();
        dave.newEdge("Friend", eve).save();
        eve.newEdge("Friend", frank).save();
        dave.newEdge("Friend", frank).save();
      });

      assertThat(names(QUERY)).as("only the chain with no direct edge survives the NOT pattern").containsExactly("Alice");

      final String rendered = new SQLAntlrParser(null).parse(QUERY).toString();
      assertThat(names(rendered)).as("the re-rendered statement must keep the negative filter; without it Dave is back")
          .containsExactly("Alice");
    }

    private List<String> names(final String query) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
      }
      return names;
    }
  }

  /**
   * Regression for #7789: ProfileStatement.toString() rendered the literal text "EXPLAIN " instead of "PROFILE ", so re-parsing a...
   * <p>
   * Regression test for issue #7789: {@code ProfileStatement.toString()} rendered the literal text {@code "EXPLAIN "}
   * instead of {@code "PROFILE "}, so re-parsing a rendered {@code PROFILE <stmt>} produced an {@code ExplainStatement}
   * - a statement with different execution semantics (PROFILE actually runs the plan, EXPLAIN only plans it).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ProfileStatementTest extends AbstractParserTest {

    @Test
    void toStringRendersProfileNotExplain() {
      checkRightSyntax("PROFILE SELECT FROM Person");
    }

    @Test
    void roundTripKeepsTheStatementAProfile() {
      final SimpleNode parsed = checkSyntax("PROFILE SELECT FROM Person", true);
      assertThat(parsed).isInstanceOf(ProfileStatement.class);

      final StringBuilder builder = new StringBuilder();
      parsed.toString(null, builder);
      assertThat(builder.toString()).startsWith("PROFILE ");

      final SimpleNode roundTripped = checkSyntax(builder.toString(), true);
      assertThat(roundTripped).isInstanceOf(ProfileStatement.class);
    }
  }

  /**
   * Regression for #7790: AlterTypeStatement disagreed with itself about which of identifierValue and...
   * <p>
   * Regression test for issue #7790: {@code AlterTypeStatement} disagreed with itself about which of
   * {@code identifierValue} and {@code identifierListValue}/{@code identifierListAddRemove} a given ALTER carries.
   * {@code toString()} rendered {@code SUPERTYPE +X}/{@code BUCKET +x} as the literal text {@code null} (unparseable)
   * and dropped the trailing {@code WITH} clause, {@code copy()} silently dropped {@code identifierValue} (so a copied
   * {@code ALTER TYPE A NAME B} has no new name at all), and {@code getIdentityElements()} omitted both, so two ALTERs
   * that do different things compared {@code equals() == true}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AlterTypeStatementTest extends AbstractParserTest {

    @Test
    void toStringRoundTripsSuperTypeWithSigns() {
      checkRightSyntax("ALTER TYPE Suv SUPERTYPE +Vehicle, +Car");
      checkRightSyntax("ALTER TYPE Suv SUPERTYPE +Vehicle, -Car");
      checkRightSyntax("ALTER TYPE Suv SUPERTYPE Vehicle");
    }

    @Test
    void toStringRoundTripsBucketWithSigns() {
      // No comma between bucket ops per grammar: BUCKET ((PLUS | MINUS) identifier)+
      checkRightSyntax("ALTER TYPE Foo BUCKET +bucket1 -bucket2");
    }

    @Test
    void toStringRoundTripsAliases() {
      checkRightSyntax("ALTER TYPE Foo ALIASES alias1, alias2");
      checkRightSyntax("ALTER TYPE Foo ALIASES NULL");
    }

    @Test
    void toStringRoundTripsNameAndBucketSelectionStrategyWithSettings() {
      checkRightSyntax("ALTER TYPE Foo NAME Bar");
      checkRightSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `round-robin`");
      checkRightSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `round-robin` WITH repartition = true");
    }

    @Test
    void toStringActuallyRendersTheSuperTypeInstead() {
      final AlterTypeStatement stmt = (AlterTypeStatement) checkSyntax("ALTER TYPE Suv SUPERTYPE +Vehicle, +Car", true);
      final StringBuilder builder = new StringBuilder();
      stmt.toString(null, builder);
      assertThat(builder.toString()).doesNotContain("null").contains("+Vehicle").contains("+Car");
    }

    @Test
    void copyPreservesTheNewNameForAlterName() {
      final AlterTypeStatement original = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo NAME Bar", true);
      final AlterTypeStatement copy = (AlterTypeStatement) original.copy();

      assertThat(copy.items).hasSize(1);
      assertThat(copy.items.get(0).identifierValue).isNotNull();
      assertThat(copy.items.get(0).identifierValue.getStringValue()).isEqualTo("Bar");
    }

    @Test
    void copyPreservesTheBucketSelectionStrategyImplementation() {
      final AlterTypeStatement original = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `thread`", true);
      final AlterTypeStatement copy = (AlterTypeStatement) original.copy();

      assertThat(copy.items.get(0).identifierValue.getStringValue())
          .isEqualTo(original.items.get(0).identifierValue.getStringValue());
    }

    @Test
    void copyPreservesTheWithSettings() {
      final AlterTypeStatement original =
          (AlterTypeStatement) checkSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `partitioned('x')` WITH repartition = true", true);
      final AlterTypeStatement copy = (AlterTypeStatement) original.copy();

      assertThat(copy.settings.keySet().stream().map(Identifier::getStringValue).toList()).containsExactly("repartition");
      assertThat(copy).isEqualTo(original);
    }

    @Test
    void equalsDistinguishesDifferentNewNames() {
      final AlterTypeStatement a = (AlterTypeStatement) checkSyntax("ALTER TYPE A NAME B", true);
      final AlterTypeStatement b = (AlterTypeStatement) checkSyntax("ALTER TYPE A NAME C", true);

      assertThat(a).isNotEqualTo(b);
      assertThat(a.hashCode()).isNotEqualTo(b.hashCode());
    }

    @Test
    void equalsDistinguishesDifferentBucketSelectionStrategies() {
      final AlterTypeStatement a = (AlterTypeStatement) checkSyntax("ALTER TYPE A BUCKETSELECTIONSTRATEGY `round-robin`", true);
      final AlterTypeStatement b = (AlterTypeStatement) checkSyntax("ALTER TYPE A BUCKETSELECTIONSTRATEGY `thread`", true);

      assertThat(a).isNotEqualTo(b);
    }

    @Test
    void equalsDistinguishesDifferentWithSettings() {
      final AlterTypeStatement a =
          (AlterTypeStatement) checkSyntax("ALTER TYPE A BUCKETSELECTIONSTRATEGY `thread` WITH repartition = true", true);
      final AlterTypeStatement b =
          (AlterTypeStatement) checkSyntax("ALTER TYPE A BUCKETSELECTIONSTRATEGY `thread` WITH repartition = false", true);

      assertThat(a).isNotEqualTo(b);
    }

    @Test
    void equalsMatchesAnEquivalentStatement() {
      final AlterTypeStatement a = (AlterTypeStatement) checkSyntax("ALTER TYPE A NAME B", true);
      final AlterTypeStatement b = (AlterTypeStatement) checkSyntax("ALTER TYPE A NAME B", true);

      assertThat(a).isEqualTo(b);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }
  }

  /**
   * Regression for #7792: CreateIndexStatement.ifNotExists was read by executeDDL and fed to the index builder, but toString() never...
   * <p>
   * Regression tests for issue #7792: {@code CreateIndexStatement.ifNotExists} was read by {@code executeDDL} and fed
   * to the index builder, but {@code toString()} never emitted the clause and {@code getIdentityElements()} never
   * included the field, so a re-rendered {@code CREATE INDEX IF NOT EXISTS} came back as the strict form (which
   * throws when the index already exists instead of being a no-op) and compared equal to it. {@code copy()} dropped
   * the field too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreateIndexIfNotExistsTest extends AbstractParserTest {

    @Test
    void reRendersIfNotExistsWithoutAName() {
      final Statement result = (Statement) checkRightSyntax("CREATE INDEX IF NOT EXISTS ON Foo (a) UNIQUE");
      // exact match, not just .contains(): also pins down that dropping the name does not leave a double space
      // between "CREATE INDEX" and "IF NOT EXISTS" (review finding on the initial version of this fix)
      assertThat(result.toString()).isEqualTo("CREATE INDEX IF NOT EXISTS ON Foo (a) UNIQUE NULL_STRATEGY SKIP");
    }

    @Test
    void reRendersIfNotExistsWithAName() {
      final Statement result = (Statement) checkRightSyntax("CREATE INDEX idx2 IF NOT EXISTS ON Foo (a) NOTUNIQUE");
      assertThat(result.toString()).isEqualTo("CREATE INDEX idx2 IF NOT EXISTS ON Foo (a) NOTUNIQUE NULL_STRATEGY SKIP");
    }

    @Test
    void ifNotExistsFormNoLongerComparesEqualToThePlainForm() {
      final CreateIndexStatement withIfNotExists = (CreateIndexStatement) new SQLAntlrParser(null)
          .parse("CREATE INDEX IF NOT EXISTS ON Foo (a) UNIQUE");
      final CreateIndexStatement plain = (CreateIndexStatement) new SQLAntlrParser(null)
          .parse("CREATE INDEX ON Foo (a) UNIQUE");

      assertThat(withIfNotExists).isNotEqualTo(plain);
    }

    @Test
    void copyPreservesIfNotExists() {
      final CreateIndexStatement stmt = (CreateIndexStatement) new SQLAntlrParser(null)
          .parse("CREATE INDEX idx2 IF NOT EXISTS ON Foo (a) NOTUNIQUE");
      final CreateIndexStatement copy = stmt.copy();

      assertThat(copy.ifNotExists).isTrue();
      assertThat(copy).isEqualTo(stmt);
      assertThat(copy.toString()).isEqualTo(stmt.toString());
    }
  }

  /**
   * Regression for #7794: CreateTriggerStatement only overrode the no-arg debug toString() and never toString(Map, StringBuilder), so...
   * <p>
   * Regression tests for issue #7794: {@code CreateTriggerStatement} only overrode the no-arg debug {@code toString()}
   * and never {@code toString(Map, StringBuilder)}, so rendering the statement produced a Java debug dump instead of
   * SQL, and nesting it inside anything that renders through the two-argument form (an {@code IF} block, a script)
   * threw {@code UnsupportedOperationException}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreateTriggerToStringTest extends AbstractParserTest {

    @Test
    void reRendersAsSqlNotAsJavaDebugDump() {
      final Statement result = (Statement) checkRightSyntax(
          "CREATE TRIGGER t1 BEFORE CREATE ON TYPE User EXECUTE SQL 'SELECT 1'");
      final String rendered = result.toString();

      assertThat(rendered).doesNotContain("CreateTriggerStatement{");
      assertThat(rendered).isEqualTo("CREATE TRIGGER t1 BEFORE CREATE ON TYPE User EXECUTE SQL 'SELECT 1'");
    }

    @Test
    void reRendersIfNotExistsAndPreservesActionCodeQuoting() {
      final Statement result = (Statement) checkRightSyntax(
          "CREATE TRIGGER IF NOT EXISTS t2 AFTER UPDATE ON TYPE Purchase EXECUTE JAVASCRIPT 'record.total > 0'");
      assertThat(result.toString())
          .isEqualTo("CREATE TRIGGER IF NOT EXISTS t2 AFTER UPDATE ON TYPE Purchase EXECUTE JAVASCRIPT 'record.total > 0'");
    }

    /** Nesting inside an IF block renders through the two-arg toString(); this used to throw UnsupportedOperationException. */
    @Test
    void rendersInsideIfBlockWithoutThrowing() {
      checkRightSyntax("IF (1 = 1) { CREATE TRIGGER t3 BEFORE CREATE ON TYPE User EXECUTE SQL 'SELECT 1'; }");
    }

    @Test
    void copyPreservesEveryField() {
      final CreateTriggerStatement stmt = (CreateTriggerStatement) new SQLAntlrParser(null)
          .parse("CREATE TRIGGER IF NOT EXISTS t4 BEFORE DELETE ON TYPE User EXECUTE JAVA 'com.example.Foo'");
      final CreateTriggerStatement copy = stmt.copy();

      assertThat(copy.toString()).isEqualTo(stmt.toString());
      assertThat(copy.actionCode).isEqualTo(stmt.actionCode);
      assertThat(copy.ifNotExists).isTrue();
      assertThat(copy).isEqualTo(stmt);
    }

    /**
     * Code review follow-up: {@code actionCodeQuoted} is only populated by the parser. A statement built any other
     * way (setting {@code actionCode} directly) must still quote it on render rather than emit the bare word "null".
     */
    @Test
    void quotesActionCodeWhenNotBuiltByTheParser() {
      final CreateTriggerStatement stmt = new CreateTriggerStatement();
      stmt.name = new Identifier("t5");
      stmt.timing = new Identifier("BEFORE");
      stmt.event = new Identifier("CREATE");
      stmt.typeName = new Identifier("User");
      stmt.actionType = new Identifier("SQL");
      stmt.actionCode = "SELECT 1";

      assertThat(stmt.toString()).isEqualTo("CREATE TRIGGER t5 BEFORE CREATE ON TYPE User EXECUTE SQL 'SELECT 1'");
    }

    /**
     * Code review follow-up (second round): the fallback quoting escaped only the quote character and the
     * backslash, but the grammar's STRING_LITERAL rule also forbids a raw CR/LF inside the literal body - they must
     * be written as the {@code \r}/{@code \n} escape sequences. A multi-line actionCode (realistic for a JAVASCRIPT/
     * JAVA trigger body) must still reparse.
     */
    @Test
    void quotesMultilineActionCodeWhenNotBuiltByTheParser() {
      final CreateTriggerStatement stmt = new CreateTriggerStatement();
      stmt.name = new Identifier("t6");
      stmt.timing = new Identifier("BEFORE");
      stmt.event = new Identifier("CREATE");
      stmt.typeName = new Identifier("User");
      stmt.actionType = new Identifier("JAVASCRIPT");
      stmt.actionCode = "var x = 1;\nprint(x);";

      final String rendered = stmt.toString();
      assertThat(rendered).doesNotContain("\n");

      // must reparse without throwing: a raw newline inside the '...' literal is a lexer/syntax error
      new SQLAntlrParser(null).parse(rendered);
    }
  }

  /**
   * Regression for #7800: two re-render defects (an unparseable PERIODIC keyword and an unquoted URL that loses its quotes) plus a...
   * <p>
   * Regression tests for issue #7800: two re-render defects (an unparseable {@code PERIODIC} keyword and an
   * unquoted URL that loses its quotes) plus a batch of DDL statements whose {@code copy()} either threw
   * {@code UnsupportedOperationException} or silently dropped a field.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReRenderAndCopyTest extends AbstractParserTest {

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
     * CodeRabbit follow-up (fourth round) briefly decoded {@code urlString} the same way
     * {@code DefineFunctionStatement.code} does, on the argument that {@code BackupDatabaseStatement} uses
     * {@code Url.getUrlString()} as the actual backup file path. That broke every Windows-style path: decoding turns
     * {@code C:\temp\new.zip} into a path containing a TAB and a LINE FEED, because the lexer's escape set
     * (backslash-n, backslash-t, backslash-r, backslash-u escapes and friends) has nothing to do with filesystem path
     * separators (issue #7894).
     * {@code urlString} must stay the un-decoded, unquoted literal body - only the outer quotes are stripped - while
     * {@code quotedLiteral} carries the raw source text for rendering, so a genuine {@code \n} in a quoted literal is
     * preserved as the two characters backslash-n, not resolved into a line break.
     */
    @Test
    void backupDatabaseUrlIsUnDecodedForExecutionAndRendersVerbatim() {
      final BackupDatabaseStatement stmt = (BackupDatabaseStatement) new com.arcadedb.query.sql.antlr.SQLAntlrParser(null)
          .parse("BACKUP DATABASE 'line1\\nline2'");

      assertThat(stmt.url.getUrlString()).isEqualTo("line1\\nline2");
      assertThat(stmt.toString()).isEqualTo("BACKUP DATABASE 'line1\\nline2'");
    }

    /** Issue #7894: a Windows-style path with a backslash must reach the executor byte-for-byte, not escape-decoded. */
    @Test
    void backupDatabaseWindowsPathUrlIsNotEscapeDecoded() {
      final BackupDatabaseStatement stmt = (BackupDatabaseStatement) new com.arcadedb.query.sql.antlr.SQLAntlrParser(null)
          .parse("BACKUP DATABASE 'C:\\temp\\new.zip'");

      assertThat(stmt.url.getUrlString()).isEqualTo("C:\\temp\\new.zip");
      assertThat(stmt.toString()).isEqualTo("BACKUP DATABASE 'C:\\temp\\new.zip'");
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

  /**
   * Regression for #7912: CreateContinuousAggregateStatement overrode only the no-arg toString(), had no copy() and no identity...
   * <p>
   * Regression tests for issue #7912: {@code CreateContinuousAggregateStatement} overrode only the no-arg
   * {@code toString()}, had no {@code copy()} and no identity elements. Each of those three gaps was closed on a
   * NEIGHBOURING statement by an already-merged fix - {@code CREATE TRIGGER} (#7794), {@code DROP}/{@code REFRESH
   * CONTINUOUS AGGREGATE} (#7800 item 3) - and the CREATE arm was missed only because the corpus behind those
   * reports had no {@code CREATE CONTINUOUS AGGREGATE} in it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreateContinuousAggregateStatementTest extends AbstractParserTest {

    @Test
    void reRendersAsSqlNotAsJavaDebugDump() {
      final Statement result = (Statement) checkRightSyntax(
          "CREATE CONTINUOUS AGGREGATE ca1 AS SELECT count(*) FROM Sensor");

      assertThat(result.toString()).isEqualTo("CREATE CONTINUOUS AGGREGATE ca1 AS SELECT count(*) FROM Sensor");
    }

    @Test
    void reRendersIfNotExists() {
      final Statement result = (Statement) checkRightSyntax(
          "CREATE CONTINUOUS AGGREGATE IF NOT EXISTS ca2 AS SELECT count(*) FROM Sensor");

      assertThat(result.toString()).isEqualTo("CREATE CONTINUOUS AGGREGATE IF NOT EXISTS ca2 AS SELECT count(*) FROM Sensor");
    }

    /**
     * The failure the issue was filed for: an {@code IF} block renders its inner statements through
     * {@code toString(Map, StringBuilder)}, and the inherited one throws.
     */
    @Test
    void rendersInsideIfBlockWithoutThrowing() {
      checkRightSyntax("IF (1 = 1) { CREATE CONTINUOUS AGGREGATE ca3 AS SELECT count(*) FROM Sensor; }");
    }

    @Test
    void rendersInsideAScriptWithoutThrowing() {
      final List<Statement> script = new SQLAntlrParser(null)
          .parseScript("BEGIN;\nCREATE CONTINUOUS AGGREGATE ca4 AS SELECT count(*) FROM Sensor;\nCOMMIT;");

      final StringBuilder builder = new StringBuilder();
      for (final Statement statement : script)
        statement.toString(null, builder.append('\n'));

      assertThat(builder.toString()).contains("CREATE CONTINUOUS AGGREGATE ca4 AS SELECT count(*) FROM Sensor");
    }

    @Test
    void copyPreservesEveryField() {
      final CreateContinuousAggregateStatement stmt = (CreateContinuousAggregateStatement) new SQLAntlrParser(null)
          .parse("CREATE CONTINUOUS AGGREGATE IF NOT EXISTS ca5 AS SELECT count(*) FROM Sensor");
      final CreateContinuousAggregateStatement copy = stmt.copy();

      assertThat(copy.ifNotExists).isTrue();
      assertThat(copy.name.getStringValue()).isEqualTo("ca5");
      assertThat(copy.toString()).isEqualTo(stmt.toString());
      assertThat(copy).isEqualTo(stmt);
      assertThat(copy.hashCode()).isEqualTo(stmt.hashCode());
    }

    /**
     * Two statements parsed from the identical text must compare equal, or the statement cache treats every
     * occurrence as a new entry.
     */
    @Test
    void identicalStatementsCompareEqual() {
      final String sql = "CREATE CONTINUOUS AGGREGATE ca6 AS SELECT count(*) FROM Sensor";
      final Statement a = new SQLAntlrParser(null).parse(sql);
      final Statement b = new SQLAntlrParser(null).parse(sql);

      assertThat(a).isEqualTo(b);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void differentNamesDoNotCompareEqual() {
      final Statement a = new SQLAntlrParser(null).parse("CREATE CONTINUOUS AGGREGATE caA AS SELECT count(*) FROM Sensor");
      final Statement b = new SQLAntlrParser(null).parse("CREATE CONTINUOUS AGGREGATE caB AS SELECT count(*) FROM Sensor");

      assertThat(a).isNotEqualTo(b);
    }

    /**
     * The secondary point of the issue: the no-arg {@code toString()} ignored the params map, so a parameterised
     * sub-select re-rendered its placeholder raw instead of bound.
     */
    @Test
    void bindsParametersOfTheSubSelect() {
      final CreateContinuousAggregateStatement stmt = (CreateContinuousAggregateStatement) new SQLAntlrParser(null)
          .parse("CREATE CONTINUOUS AGGREGATE ca7 AS SELECT count(*) FROM Sensor WHERE value > :threshold");

      final StringBuilder builder = new StringBuilder();
      stmt.toString(Map.of("threshold", 10), builder);

      assertThat(builder.toString()).contains("10").doesNotContain(":threshold");
    }
  }

  /**
   * Regression for #7920: Parser-side regression test for issue #7920:
   * <p>
   * Parser-side regression test for issue #7920: the grammar accepts a comma-separated list of {@code alterTypeItem},
   * but the AST held ONE {@code property}, ONE {@code identifierValue} and one shared identifier list, which the
   * builder overwrote per item. So a multi-item {@code ALTER TYPE} kept only the last item, and because every item
   * appended into the same list the list could be carried from one item's property to another's -
   * {@code ALTER TYPE Foo ALIASES x, y, SUPERTYPE +A} re-rendered as {@code ALTER TYPE Foo supertype +x, +y, +A},
   * three super types out of two aliases and one super type.
   * <p>
   * {@code toString()} also emitted no comma before the {@code CUSTOM} arm, so the one multi-item form that DID
   * execute both items re-rendered as unparseable SQL.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MultiItemAlterTypeTest extends AbstractParserTest {

    @Test
    void everyItemIsKept() {
      final AlterTypeStatement stmt = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo2 NAME Bar2, SUPERTYPE +Base", true);

      assertThat(stmt.items).hasSize(2);
      assertThat(stmt.items.get(0).property).isEqualTo("name");
      assertThat(stmt.items.get(0).identifierValue.getStringValue()).isEqualTo("Bar2");
      assertThat(stmt.items.get(1).property).isEqualTo("supertype");
      assertThat(stmt.items.get(1).identifierListValue).hasSize(1);
      assertThat(stmt.items.get(1).identifierListValue.get(0).getStringValue()).isEqualTo("Base");
    }

    @Test
    void aliasesDoNotLeakIntoTheFollowingSuperType() {
      final AlterTypeStatement stmt = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo ALIASES x, y, SUPERTYPE +A", true);

      assertThat(stmt.items).hasSize(2);
      assertThat(stmt.items.get(0).property).isEqualTo("aliases");
      assertThat(stmt.items.get(0).identifierListValue.stream().map(Identifier::getStringValue).toList())
          .containsExactly("x", "y");
      assertThat(stmt.items.get(1).property).isEqualTo("supertype");
      assertThat(stmt.items.get(1).identifierListValue.stream().map(Identifier::getStringValue).toList())
          .containsExactly("A");

      final StringBuilder builder = new StringBuilder();
      stmt.toString(null, builder);
      assertThat(builder.toString()).isEqualTo("ALTER TYPE Foo aliases x, y, supertype +A");
    }

    @Test
    void multiItemFormsRoundTrip() {
      // checkRightSyntax re-parses what toString() rendered, so each of these is a round trip.
      checkRightSyntax("ALTER TYPE Foo NAME Bar, SUPERTYPE +Base");
      checkRightSyntax("ALTER TYPE Foo NAME Bar, CUSTOM description = 'x'");
      checkRightSyntax("ALTER TYPE Foo ALIASES x, y, SUPERTYPE +A");
      checkRightSyntax("ALTER TYPE Foo SUPERTYPE +A, -B, BUCKET +b1 -b2, CUSTOM k = 1");
    }

    @Test
    void customIsRenderedWithItsSeparatingComma() {
      final AlterTypeStatement stmt =
          (AlterTypeStatement) checkSyntax("ALTER TYPE Foo NAME Bar, CUSTOM description = 'x'", true);

      final StringBuilder builder = new StringBuilder();
      stmt.toString(null, builder);
      // Without the comma this rendered `ALTER TYPE Foo name Bar CUSTOM description='x'`, which does not parse.
      assertThat(builder.toString()).isEqualTo("ALTER TYPE Foo name Bar, CUSTOM description='x'");
    }

    @Test
    void copyCarriesEveryItem() {
      final AlterTypeStatement original =
          (AlterTypeStatement) checkSyntax("ALTER TYPE Foo NAME Bar, SUPERTYPE +Base, CUSTOM k = 'v'", true);
      final AlterTypeStatement copy = (AlterTypeStatement) original.copy();

      assertThat(copy.items).hasSize(3);
      assertThat(copy).isEqualTo(original);
      assertThat(copy.hashCode()).isEqualTo(original.hashCode());
    }

    @Test
    void equalsDistinguishesStatementsThatDifferOnlyInADroppedItem() {
      final AlterTypeStatement a = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo NAME Bar, SUPERTYPE +Base", true);
      final AlterTypeStatement b = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo NAME Baz, SUPERTYPE +Base", true);

      // Both used to collapse onto the same trailing SUPERTYPE item, so they compared equal.
      assertThat(a).isNotEqualTo(b);
    }

    @Test
    void singleItemFormsAreUnaffected() {
      checkRightSyntax("ALTER TYPE Foo NAME Bar");
      checkRightSyntax("ALTER TYPE Suv SUPERTYPE +Vehicle, +Car");
      checkRightSyntax("ALTER TYPE Foo BUCKET +bucket1 -bucket2");
      checkRightSyntax("ALTER TYPE Foo ALIASES alias1, alias2");
      checkRightSyntax("ALTER TYPE Foo ALIASES NULL");
      checkRightSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `round-robin`");
      checkRightSyntax("ALTER TYPE Foo CUSTOM description = 'x'");
      checkRightSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `round-robin` WITH repartition = true");
    }
  }

  /**
   * Regression for #8061: DeleteFunctionStatement.copy() built and returned a DefineFunctionStatement - the opposite statement, one...
   * <p>
   * Regression test for issue #8061: {@code DeleteFunctionStatement.copy()} built and returned a
   * {@code DefineFunctionStatement} - the opposite statement, one that REGISTERS a function rather than deleting one -
   * and {@code equals()} cast its operand to {@code DefineFunctionStatement} right after a {@code getClass()} guard
   * that proved it was actually a {@code DeleteFunctionStatement}, throwing {@code ClassCastException}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DeleteFunctionStatementCopyAndEqualsTest {

    private static DeleteFunctionStatement parse(final String sql) {
      return (DeleteFunctionStatement) new SQLAntlrParser(null).parse(sql);
    }

    @Test
    void copyReturnsADeleteFunctionStatementNotADefineFunctionStatement() {
      final DeleteFunctionStatement original = parse("DELETE FUNCTION lib.fn");

      final Statement copy = original.copy();

      assertThat(copy).isInstanceOf(DeleteFunctionStatement.class);
      assertThat(copy.toString()).isEqualTo("DELETE FUNCTION lib.fn");
    }

    @Test
    void copyRoundTripsThroughAnIfStatementBody() {
      final IfStatement ifStatement = (IfStatement) new SQLAntlrParser(null).parse("IF (1 = 1) { DELETE FUNCTION lib.fn; }");

      final IfStatement copy = ifStatement.copy();

      assertThat(copy.toString()).contains("DELETE FUNCTION lib.fn");
      assertThat(copy.toString()).doesNotContain("DEFINE FUNCTION");
    }

    @Test
    void equalsComparesTwoDeleteFunctionStatementsWithoutThrowing() {
      final DeleteFunctionStatement a = parse("DELETE FUNCTION lib.fn");
      final DeleteFunctionStatement b = parse("DELETE FUNCTION lib.fn");
      final DeleteFunctionStatement c = parse("DELETE FUNCTION lib.other");

      assertThatCode(() -> a.equals(b)).doesNotThrowAnyException();
      assertThat(a).isEqualTo(b);
      assertThat(a).isNotEqualTo(c);
    }
  }

  /**
   * Regression for #9495: the copy() of SelectStatement, CreateVertexStatement, MatchPathItem, FieldMatchPathItem and MathExpression...
   * <p>
   * Regression tests for issue #9495: the {@code copy()} of {@link SelectStatement}, {@link CreateVertexStatement},
   * {@link MatchPathItem}, {@link FieldMatchPathItem} and {@link MathExpression} created the copy with
   * {@code getClass().getConstructor().newInstance()}. A GraalVM native image registers no parser constructor for
   * reflection, so every UPDATE/DELETE (whose plan-cache key copies a synthetic SELECT) failed there with
   * {@code NoSuchMethodException: SelectStatement.<init>()}. The copies now come from an overridable {@code newInstance()}
   * factory, which every subclass must override to keep the runtime type of the copy. That no class of the code base asks for the
   * constructor of its own runtime type is held by {@code ReflectionFreeCopyCensusTest} (census module).
   */
  @Nested
  class ReflectionFreeCopyTest extends TestHelper {

    private static final String     SQL_PACKAGE   = "com/arcadedb/query/sql";
    private static final Class<?>[] FACTORY_BASES = { SelectStatement.class, CreateVertexStatement.class, MatchPathItem.class,
        MathExpression.class };

    /**
     * The factory replaced {@code getClass()}, so a subclass that inherits {@code copy()} and forgets to override
     * {@code newInstance()} silently copies itself into its base class. Every concrete subclass, current and future, is
     * discovered from the compiled classes of the SQL packages and checked.
     */
    @Test
    void copyKeepsTheRuntimeTypeOfEverySubclass() throws Exception {
      int checked = 0;
      for (final String className : sqlClassNames()) {
        final Class<?> clazz = Class.forName(className);
        if (Modifier.isAbstract(clazz.getModifiers()) || !Modifier.isPublic(clazz.getModifiers()) || !isFactoryBased(clazz))
          continue;
        if (!hasPublicNoArgConstructor(clazz)) {
          // such a subclass cannot inherit the factory-based copy() (newInstance() could not create it): it must override copy()
          assertThat(clazz.getMethod("copy").getDeclaringClass()).as("copy() of " + clazz.getSimpleName()).isEqualTo(clazz);
          continue;
        }

        final Object original = clazz.getConstructor().newInstance();
        final Object copy = clazz.getMethod("copy").invoke(original);
        assertThat(copy).as("copy() of " + clazz.getSimpleName()).isNotSameAs(original);
        assertThat(copy.getClass()).as("copy() of " + clazz.getSimpleName()).isEqualTo(clazz);
        checked++;
      }
      // the four bases plus their subclasses: a discovery that finds nothing would pass for the wrong reason
      assertThat(checked).isGreaterThanOrEqualTo(20);
    }

    @Test
    void copiedStatementsPreserveTheirContent() {
      final SelectStatement select = (SelectStatement) parse("SELECT name FROM V WHERE age > 3 LIMIT 5");
      final SelectStatement selectCopy = select.copy();
      assertThat(selectCopy.getClass()).isEqualTo(SelectStatement.class);
      assertThat(selectCopy.toString()).isEqualTo(select.toString());
      assertThat(selectCopy).isEqualTo(select);

      final SelectStatement noTarget = (SelectStatement) parse("SELECT 1 + 2 AS sum");
      assertThat(noTarget.copy().getClass()).isEqualTo(noTarget.getClass());
      assertThat(noTarget.copy().toString()).isEqualTo(noTarget.toString());

      final Statement createVertex = parse("CREATE VERTEX V SET name = 'a'");
      assertThat(createVertex.copy().getClass()).isEqualTo(createVertex.getClass());
      assertThat(createVertex.copy().toString()).isEqualTo(createVertex.toString());

      final MatchStatement match = (MatchStatement) parse(
          "MATCH {type: V, as: a}.out('E'){as: b}.(in('E')){as: c}, {as: a}.field{as: d}, {as: a}-E->{as: e}<-E-{as: f}-E-{as: g} RETURN a, b");
      assertThat(pathItemTypes((MatchStatement) match.copy())).isEqualTo(pathItemTypes(match));
      assertThat(pathItemTypes(match)).contains(MatchPathItem.class, MultiMatchPathItem.class, FieldMatchPathItem.class);
    }

    /** The end-to-end shape of the report: UPDATE, DELETE and UNION ALL all copy a SELECT on their way to a plan. */
    @Test
    void dmlAndUnionExecuteThroughTheCopiedSource() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE TmpProbe");
        database.command("sql", "INSERT INTO TmpProbe SET id = 1, v = 0");
        database.command("sql", "INSERT INTO TmpProbe SET id = 2, v = 0");
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "UPDATE TmpProbe SET v = 1 WHERE id = 1")) {
          assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
        }
        try (final ResultSet rs = database.command("sql", "DELETE FROM TmpProbe WHERE id = 2")) {
          assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
        }
      });

      try (final ResultSet rs = database.query("sql",
          "SELECT unionAll($a, $b) AS u LET $a = (SELECT v FROM TmpProbe WHERE id = 1), $b = (SELECT v FROM TmpProbe WHERE id = 1)")) {
        assertThat(rs.next().<List<?>>getProperty("u")).hasSize(2);
      }
    }

    private Statement parse(final String sql) {
      return ((DatabaseInternal) database).getStatementCache().get(sql);
    }

    private static List<Class<?>> pathItemTypes(final MatchStatement match) {
      final List<Class<?>> types = new ArrayList<>();
      for (final MatchExpression expression : match.getMatchExpressions())
        for (final MatchPathItem item : expression.getItems())
          types.add(item.getClass());
      return types;
    }

    private static boolean isFactoryBased(final Class<?> clazz) {
      for (final Class<?> base : FACTORY_BASES)
        if (base.isAssignableFrom(clazz))
          return true;
      return false;
    }

    private static boolean hasPublicNoArgConstructor(final Class<?> clazz) {
      try {
        clazz.getConstructor();
        return true;
      } catch (final NoSuchMethodException e) {
        return false;
      }
    }

    /** The top-level classes compiled from the SQL packages, read from the class root that holds {@link SelectStatement}. */
    private static List<String> sqlClassNames() throws IOException, URISyntaxException {
      final Path root = Path.of(SelectStatement.class.getProtectionDomain().getCodeSource().getLocation().toURI());
      final Path sql = root.resolve(SQL_PACKAGE);
      assertThat(sql).as("the compiled SQL classes").isDirectory();
      try (final Stream<Path> files = Files.walk(sql)) {
        return files.map(p -> root.relativize(p).toString().replace('\\', '/')).filter(p -> p.endsWith(".class") && !p.contains("$") && !p.endsWith("package-info.class"))
            .map(p -> p.substring(0, p.length() - ".class".length()).replace('/', '.')).sorted().toList();
      }
    }
  }

  /**
   * Regression for #9650: the four Statement subclasses whose toString() or copy() was still lossy, unparseable or throwing after the...
   * <p>
   * Regression tests for issue #9650: the four {@code Statement} subclasses whose {@code toString()} or {@code copy()} was still
   * lossy, unparseable or throwing after the twelve issues filed on that shape one at a time (#7789 to #7801).
   * <p>
   * The wider guard asked for by the issue lives in {@link AbstractParserTest#checkRightSyntax}: every statement any parser test
   * round-trips through it is now also checked for a stable re-render and for a {@code copy()} that is equal to, and renders the
   * same as, its original.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StatementRenderAndCopyTest extends AbstractParserTest {

    /** Row 1: the index type is optional in the grammar (validate() refuses its absence later), so the render must not assume it. */
    @Test
    void createIndexWithoutATypeRendersInsteadOfThrowing() {
      final Statement stmt = parse("CREATE INDEX ON Foo (a)");
      assertThat(stmt.toString()).isEqualTo("CREATE INDEX ON Foo (a) NULL_STRATEGY SKIP");
      assertRoundTrips(stmt);
    }

    @Test
    void explainOfACreateIndexWithoutATypeRenders() {
      final Statement stmt = parse("EXPLAIN CREATE INDEX ON Foo (a)");
      assertThat(stmt.toString()).isEqualTo("EXPLAIN CREATE INDEX ON Foo (a) NULL_STRATEGY SKIP");
    }

    /** Row 2: BreakStatement had no copy() and inherited the throwing base. */
    @Test
    void breakStatementCopies() {
      final Statement stmt = parse("IF (1 = 1) { BREAK; }");
      assertRoundTrips(stmt);

      final BreakStatement breakStatement = new BreakStatement();
      assertThat(breakStatement.copy()).isInstanceOf(BreakStatement.class);
      assertThat(breakStatement.copy().toString()).isEqualTo("BREAK");
    }

    /**
     * Row 3: MatchStatement.copy() ran buildPatterns() on the copy, which aliased every unaliased node and appended an empty filter
     * item that rendered as a trailing ", " - unparseable, and not equal to the original.
     */
    @Test
    void matchStatementCopyIsEqualAndParseable() {
      final MatchStatement stmt = (MatchStatement) parse("MATCH {type: 'V', as: foo} RETURN foo");
      final MatchStatement copy = stmt.copy();

      assertThat(copy.toString()).isEqualTo("MATCH {type: 'V', as: foo} RETURN foo");
      assertThat(copy).isEqualTo(stmt);
      assertThat(copy.hashCode()).isEqualTo(stmt.hashCode());
      assertThat(parse(copy.toString())).isEqualTo(stmt);
    }

    /** An unaliased node must not come back from copy() carrying a generated $ARCADEDB_DEFAULT_ALIAS_n alias. */
    @Test
    void matchStatementCopyDoesNotInventAliases() {
      final Statement stmt = parse("MATCH {type: 'V'}.out('E'){where: (name = 'x')} RETURN $elements");
      final Statement copy = stmt.copy();

      assertThat(copy.toString()).doesNotContain(MatchStatement.DEFAULT_ALIAS_PREFIX);
      assertThat(copy.toString()).isEqualTo(stmt.toString());
      assertThat(copy).isEqualTo(stmt);
    }

    @Test
    void matchSubqueryInsideLetCopies() {
      assertRoundTrips(parse("SELECT FROM V LET $x = (MATCH {type: 'V', as: foo} RETURN foo)"));
    }

    /** Clearing a filter that was never there is a no-op: it used to append an empty item that rendered as a dangling comma. */
    @Test
    void clearingAnAbsentMatchFilterAddsNothing() {
      final MatchStatement stmt = (MatchStatement) parse("MATCH {type: 'V', as: foo} RETURN foo");
      final MatchFilter filter = stmt.getMatchExpressions().getFirst().getOrigin();
      final String before = filter.toString();

      filter.setFilter(null);

      assertThat(filter.toString()).isEqualTo(before);
    }

    /** Row 4: the WITH settings map was a HashMap, so the clause re-rendered in arbitrary order, and differently from its own copy. */
    @Test
    void withSettingsRenderInDeclaredOrder() {
      assertRendersVerbatim("EXPORT DATABASE file://e.jsonl.tgz WITH format = 'jsonl', overwrite = true, includeTypes = Person");
      assertRendersVerbatim("IMPORT DATABASE file://e.jsonl.tgz WITH zeta = 1, commitEvery = 10, alpha = 'x', mid = true");
      assertRendersVerbatim("BACKUP DATABASE file://b.zip WITH maxMBPerSecond = 10, compressionLevel = 5, encryptionAlgorithm = 'AES'");
      assertRendersVerbatim("REBUILD INDEX Foo WITH zeta = 1, batchSize = 1000, alpha = 2");
      assertRendersVerbatim("REBUILD TYPE Foo POLYMORPHIC WITH repartition = true, batchSize = 1000, alpha = 2");
    }

    private void assertRendersVerbatim(final String sql) {
      final Statement stmt = parse(sql);
      assertThat(stmt.toString()).isEqualTo(sql);
      assertRoundTrips(stmt);
    }

    private static void assertRoundTrips(final Statement stmt) {
      final String rendered = stmt.toString();
      final Statement copy = stmt.copy();
      assertThat(copy.toString()).isEqualTo(rendered);
      assertThat(copy).isEqualTo(stmt);
      assertThat(copy.hashCode()).isEqualTo(stmt.hashCode());
      assertThat(parse(rendered).toString()).isEqualTo(rendered);
    }

    private static Statement parse(final String sql) {
      return new SQLAntlrParser(null).parse(sql);
    }
  }
}
