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
 * Grammar coverage for the {@code DELETE ORPHANS} clause of {@code CHECK DATABASE} (issue #6090).
 * <p>
 * {@code checkRightSyntax} parses, renders the AST back to SQL through {@code toString} and re-parses that, so
 * these also pin the round trip the statement cache depends on.
 */
class Issue6090CheckDatabaseDeleteOrphansParserTest extends AbstractParserTest {

  @Test
  void deleteOrphansClause() {
    checkRightSyntax("CHECK DATABASE FIX DELETE ORPHANS");
    checkRightSyntax("check database fix delete orphans");
    checkRightSyntax("check database fix delete orphans compress");
    checkRightSyntax("check database type Customer fix delete orphans");
    checkRightSyntax("check database record #12:3 fix delete orphans");
    // Accepted by the GRAMMAR; refused at execution because it removes records - see
    // Issue6090OrphanEdgeRecordCheckTest.deleteOrphansWithoutFixIsRefused.
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
