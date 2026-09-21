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
 * Grammar coverage for the {@code DELETE INVALID RECORDS} clause of {@code CHECK DATABASE} (issue #7952).
 * <p>
 * {@code checkRightSyntax} parses, renders the AST back to SQL through {@code toString} and re-parses that, so
 * these also pin the round trip the statement cache depends on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7952CheckDatabaseDeleteInvalidRecordsParserTest extends AbstractParserTest {

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
    // Issue7952ConstraintViolationScanTest.deleteInvalidRecordsWithoutFixIsRefused.
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
