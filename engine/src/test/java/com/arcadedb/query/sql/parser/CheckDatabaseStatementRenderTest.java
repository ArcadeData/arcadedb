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

import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Grammar, rendering and re-parse coverage for the CHECK DATABASE statement.
 * <p>
 * A container for the regression suites of the feature; each nested class keeps its own fixture.
 */
class CheckDatabaseStatementRenderTest {
  /**
   * Regression for #6090: Grammar coverage for the DELETE ORPHANS clause of CHECK DATABASE (issue #6090).
   * <p>
   * Grammar coverage for the {@code DELETE ORPHANS} clause of {@code CHECK DATABASE} (issue #6090).
   * <p>
   * {@code checkRightSyntax} parses, renders the AST back to SQL through {@code toString} and re-parses that, so
   * these also pin the round trip the statement cache depends on.
   */
  @Nested
  class CheckDatabaseDeleteOrphansParser extends AbstractParserTest {

    @Test
    void deleteOrphansClause() {
      checkRightSyntax("CHECK DATABASE FIX DELETE ORPHANS");
      checkRightSyntax("check database fix delete orphans");
      checkRightSyntax("check database fix delete orphans compress");
      checkRightSyntax("check database type Customer fix delete orphans");
      checkRightSyntax("check database record #12:3 fix delete orphans");
      // Accepted by the GRAMMAR; refused at execution because it removes records - see
      // OrphanAndGhostEdgeTest.OrphanEdgeRecordCheck.deleteOrphansWithoutFixIsRefused.
      checkRightSyntax("check database delete orphans");

      checkWrongSyntax("check database fix delete");
      checkWrongSyntax("check database fix orphans");
      checkWrongSyntax("check database delete orphans fix");
    }

    /** The rendered form must carry the clause, or the statement cache would replay a plain FIX. */
    @Test
    void theClauseSurvivesToString() {
      final StringBuilder rendered = new StringBuilder();
      checkRightSyntax("CHECK DATABASE FIX DELETE ORPHANS COMPRESS").toString(null, rendered);
      assertThat(rendered.toString()).isEqualTo("CHECK DATABASE FIX DELETE ORPHANS COMPRESS");
    }

    /**
     * {@code ORPHANS} is a new lexer token, so it is also listed among the keywords usable as an identifier: a
     * schema that already has a type or property by that name must keep parsing.
     */
    @Test
    void orphansIsStillUsableAsAnIdentifier() {
      checkRightSyntax("select from orphans");
      checkRightSyntax("select orphans from Customer");
      checkRightSyntax("create document type orphans");
    }
  }

  /**
   * Regression for #6189: Grammar coverage for the RECLAIM UNREFERENCED FILES clause of CHECK DATABASE (issue #6189).
   * <p>
   * Grammar coverage for the {@code RECLAIM UNREFERENCED FILES} clause of {@code CHECK DATABASE} (issue #6189).
   * <p>
   * {@code checkRightSyntax} parses, renders the AST back to SQL through {@code toString} and re-parses that, so
   * these also pin the round trip the statement cache depends on.
   */
  @Nested
  class CheckDatabaseReclaimUnreferencedFilesParser extends AbstractParserTest {

    @Test
    void reclaimUnreferencedFilesClause() {
      checkRightSyntax("CHECK DATABASE FIX RECLAIM UNREFERENCED FILES");
      checkRightSyntax("check database fix reclaim unreferenced files");
      checkRightSyntax("check database fix reclaim unreferenced files compress");
      checkRightSyntax("check database type Customer fix reclaim unreferenced files");
      checkRightSyntax("check database record #12:3 fix reclaim unreferenced files");
      // Combinable with the other opt-in reclaim clause: two independent removals, one FIX.
      checkRightSyntax("check database fix delete orphans reclaim unreferenced files");
      // Accepted by the GRAMMAR; refused at execution because it removes files - see
      // UnreferencedFilesTest.ReclaimUnreferencedFiles.reclaimWithoutFixIsRefused.
      checkRightSyntax("check database reclaim unreferenced files");

      checkWrongSyntax("check database fix reclaim");
      checkWrongSyntax("check database fix reclaim unreferenced");
      checkWrongSyntax("check database fix unreferenced files");
      checkWrongSyntax("check database fix reclaim unreferenced files fix");
    }

    /** The rendered form must carry the clause, or the statement cache would replay a plain FIX. */
    @Test
    void theClauseSurvivesToString() {
      final StringBuilder rendered = new StringBuilder();
      checkRightSyntax("CHECK DATABASE FIX DELETE ORPHANS RECLAIM UNREFERENCED FILES COMPRESS").toString(null, rendered);
      assertThat(rendered.toString()).isEqualTo("CHECK DATABASE FIX DELETE ORPHANS RECLAIM UNREFERENCED FILES COMPRESS");
    }

    /**
     * {@code RECLAIM}, {@code UNREFERENCED} and {@code FILES} are new lexer tokens, so all three are also listed
     * among the keywords usable as an identifier: a schema that already has a type or property by one of these
     * names must keep parsing.
     */
    @Test
    void newTokensAreStillUsableAsIdentifiers() {
      checkRightSyntax("select from reclaim");
      checkRightSyntax("select reclaim from Customer");
      checkRightSyntax("create document type reclaim");

      checkRightSyntax("select from unreferenced");
      checkRightSyntax("select unreferenced from Customer");
      checkRightSyntax("create document type unreferenced");

      checkRightSyntax("select from files");
      checkRightSyntax("select files from Customer");
      checkRightSyntax("create document type files");
    }
  }

  /**
   * Regression for #7793: the TYPE and BUCKET loops in CheckDatabaseStatement.toString() appended the separator AFTER the element and...
   * <p>
   * Regression tests for issue #7793: the TYPE and BUCKET loops in {@code CheckDatabaseStatement.toString()} appended
   * the separator AFTER the element and only when {@code i > 0}, instead of BEFORE the element when {@code i > 0}
   * (the RECORD loop right below already did it correctly). A two-element list rendered as one run-together
   * identifier plus a trailing comma, which does not parse.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CheckDatabaseTypeBucketSeparator extends AbstractParserTest {

    @Test
    void twoTypesReRenderWithASeparatorAndNoTrailingComma() {
      final Statement result = (Statement) checkRightSyntax("CHECK DATABASE TYPE Customer, Order");
      final String rendered = result.toString();

      assertThat(rendered).matches("CHECK DATABASE TYPE \\w+,\\w+");
      assertThat(rendered).doesNotEndWith(",");
    }

    @Test
    void twoBucketsReRenderWithASeparatorAndNoTrailingComma() {
      final Statement result = (Statement) checkRightSyntax("CHECK DATABASE BUCKET 1, 2");
      final String rendered = result.toString();

      assertThat(rendered).matches("CHECK DATABASE BUCKET \\d+,\\d+");
      assertThat(rendered).doesNotEndWith(",");
    }

    @Test
    void copyPreservesTypesBucketsAndFlags() {
      final CheckDatabaseStatement stmt = (CheckDatabaseStatement) new SQLAntlrParser(null)
          .parse("CHECK DATABASE TYPE Customer, Order BUCKET 1, 2 FIX DEEP COMPRESS");
      final CheckDatabaseStatement copy = stmt.copy();

      // equals() is content-based (Set semantics, so insertion order does not matter here), but iteration order of a
      // freshly-populated HashSet is not guaranteed to match the original's, so the two renders are checked for
      // well-formedness rather than byte-for-byte equality.
      assertThat(copy).isEqualTo(stmt);
      assertThat(copy.types).hasSize(2);
      assertThat(copy.buckets).hasSize(2);
      assertThat(copy.fix).isTrue();
      assertThat(copy.deep).isTrue();
      assertThat(copy.compress).isTrue();
      assertThat(copy.toString()).matches("CHECK DATABASE TYPE \\w+,\\w+ BUCKET \\d+,\\d+ FIX DEEP COMPRESS");
    }
  }

  /**
   * Regression for #7913: Regression for issue #7913:
   * <p>
   * Regression for issue #7913: the TYPE and BUCKET loops in {@code CheckDatabaseStatement.toString()} appended the
   * raw name ({@code Identifier.getStringValue()} / {@code BucketIdentifier.getValue()}) instead of letting the
   * element render itself, as the RECORD loop three lines below already did. A back-tick quoted name lost its
   * quotes, so the render no longer reparsed - and the statement cache re-parses the rendered text, so a statement
   * whose render does not reparse cannot round-trip. #7793 fixed the separator on these same two loops; the element
   * rendering next to it was left untouched.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CheckDatabaseQuotedNameRender extends AbstractParserTest {

    @Test
    void aQuotedTypeNameWithASpaceKeepsItsBackTicks() {
      final Statement parsed = (Statement) checkRightSyntax("CHECK DATABASE TYPE `My Type`");

      assertThat(parsed.toString()).isEqualTo("CHECK DATABASE TYPE `My Type`");
      // And the render reparses, which is the point: the statement cache re-parses its own output.
      assertThat(checkRightSyntax(parsed.toString()).toString()).isEqualTo(parsed.toString());
    }

    @Test
    void aQuotedBucketNameWithASpaceKeepsItsBackTicks() {
      final Statement parsed = (Statement) checkRightSyntax("CHECK DATABASE BUCKET `my bucket`");

      assertThat(parsed.toString()).isEqualTo("CHECK DATABASE BUCKET `my bucket`");
      assertThat(checkRightSyntax(parsed.toString()).toString()).isEqualTo(parsed.toString());
    }

    /**
     * A quoted RESERVED WORD: this one reparsed even before the fix, but as a different statement - the quotes are
     * what say "a type called Order", not the ORDER keyword - so the round-trip silently stopped comparing equal.
     */
    @Test
    void aQuotedReservedWordKeepsItsBackTicksAndReparsesEqual() {
      final CheckDatabaseStatement parsed = (CheckDatabaseStatement) new SQLAntlrParser(null)
          .parse("CHECK DATABASE TYPE `Order`");

      assertThat(parsed.toString()).isEqualTo("CHECK DATABASE TYPE `Order`");

      final CheckDatabaseStatement reparsed = (CheckDatabaseStatement) new SQLAntlrParser(null).parse(parsed.toString());
      assertThat(reparsed).isEqualTo(parsed);
    }

    /**
     * Mixed list: the quoted element keeps its quotes and the plain one does not grow any, with #7793's separator
     * still in place.
     */
    @Test
    void aMixedTypeListRendersEachElementInItsOwnForm() {
      final CheckDatabaseStatement parsed = (CheckDatabaseStatement) new SQLAntlrParser(null)
          .parse("CHECK DATABASE TYPE `My Type`, Customer");
      final String rendered = parsed.toString();

      assertThat(rendered).startsWith("CHECK DATABASE TYPE ");
      assertThat(rendered).contains("`My Type`").contains("Customer");
      assertThat(rendered).doesNotContain("``");
      assertThat(((CheckDatabaseStatement) new SQLAntlrParser(null).parse(rendered))).isEqualTo(parsed);
    }

    /**
     * A numeric bucket reference has no name to quote and must keep rendering as the bare number: the else arm now
     * splits on {@code bucketId} instead of falling through {@code getValue()}.
     */
    @Test
    void aNumericBucketStillRendersAsTheBareNumber() {
      final Statement parsed = (Statement) checkRightSyntax("CHECK DATABASE BUCKET 1, 2");

      assertThat(parsed.toString()).matches("CHECK DATABASE BUCKET \\d+,\\d+");
    }

    /**
     * {@code CREATE TYPE ... BUCKET} renders through the same {@code BucketIdentifier.toString()}, so it gets the
     * fix too.
     */
    @Test
    void createTypeRendersAQuotedBucketNameWithItsBackTicks() {
      final Statement parsed = (Statement) checkRightSyntax("CREATE DOCUMENT TYPE Foo BUCKET `my bucket`");

      assertThat(parsed.toString()).contains("`my bucket`");
      assertThat(checkRightSyntax(parsed.toString()).toString()).isEqualTo(parsed.toString());
    }
  }

  /**
   * Regression for #7952: Grammar coverage for the DELETE INVALID RECORDS clause of CHECK DATABASE (issue #7952).
   * <p>
   * Grammar coverage for the {@code DELETE INVALID RECORDS} clause of {@code CHECK DATABASE} (issue #7952).
   * <p>
   * {@code checkRightSyntax} parses, renders the AST back to SQL through {@code toString} and re-parses that, so
   * these also pin the round trip the statement cache depends on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CheckDatabaseDeleteInvalidRecordsParser extends AbstractParserTest {

    @Test
    void deleteInvalidRecordsClause() {
      checkRightSyntax("CHECK DATABASE FIX DELETE INVALID RECORDS");
      checkRightSyntax("check database fix delete invalid records");
      checkRightSyntax("check database fix delete invalid records compress");
      checkRightSyntax("check database type Customer fix delete invalid records");
      checkRightSyntax("check database record #12:3 fix delete invalid records");
      // Combinable with the other opt-in removals: three independent repairs, one FIX.
      checkRightSyntax("check database fix delete orphans delete invalid records reclaim unreferenced files");
      // Accepted by the GRAMMAR; refused at execution because it removes records - see
      // ConstraintViolationScanTest.deleteInvalidRecordsWithoutFixIsRefused.
      checkRightSyntax("check database delete invalid records");

      checkWrongSyntax("check database fix delete invalid");
      checkWrongSyntax("check database fix invalid records");
      checkWrongSyntax("check database fix delete records");
      checkWrongSyntax("check database fix delete invalid records fix");
    }

    /** The rendered form must carry the clause, or the statement cache would replay a plain FIX. */
    @Test
    void theClauseSurvivesToString() {
      final StringBuilder rendered = new StringBuilder();
      checkRightSyntax("CHECK DATABASE FIX DELETE ORPHANS DELETE INVALID RECORDS RECLAIM UNREFERENCED FILES COMPRESS")
          .toString(null, rendered);
      assertThat(rendered.toString())
          .isEqualTo("CHECK DATABASE FIX DELETE ORPHANS DELETE INVALID RECORDS RECLAIM UNREFERENCED FILES COMPRESS");
    }

    /**
     * {@code INVALID} and {@code RECORDS} are new lexer tokens, so both are also listed among the keywords usable as
     * an identifier: a schema that already has a type or property by either name must keep parsing. {@code RECORDS}
     * matters in particular, because the singular {@code RECORD} has been usable as one all along and a plural that
     * suddenly was not would be a silent regression for anyone who pluralised their type names.
     */
    @Test
    void newTokensAreStillUsableAsIdentifiers() {
      checkRightSyntax("select from invalid");
      checkRightSyntax("select invalid from Customer");
      checkRightSyntax("create document type invalid");

      checkRightSyntax("select from records");
      checkRightSyntax("select records from Customer");
      checkRightSyntax("create document type records");
      checkRightSyntax("select from Customer where records = 3");
    }
  }
}
