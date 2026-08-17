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
 * Grammar coverage for the {@code RECLAIM UNREFERENCED FILES} clause of {@code CHECK DATABASE} (issue #6189).
 * <p>
 * {@code checkRightSyntax} parses, renders the AST back to SQL through {@code toString} and re-parses that, so
 * these also pin the round trip the statement cache depends on.
 */
class Issue6189CheckDatabaseReclaimUnreferencedFilesParserTest extends AbstractParserTest {

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
    // Issue6189ReclaimUnreferencedFilesTest.reclaimWithoutFixIsRefused.
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
