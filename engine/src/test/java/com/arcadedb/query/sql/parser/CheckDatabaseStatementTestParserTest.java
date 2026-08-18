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

class CheckDatabaseStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("CHECK DATABASE");
    checkRightSyntax("check database");
    checkRightSyntax("check database type Customer");
    checkRightSyntax("check database bucket 3");
    checkRightSyntax("check database bucket 3 FIX");
    checkRightSyntax("check database bucket Customer");
    checkRightSyntax("check database bucket Customer fix");

    checkWrongSyntax("check database file:///foo/bar/ foo bar");
  }

  /** #5680: the RECORD scope, the cheap repair path for a single vertex whose edge chain is broken. */
  @Test
  void recordScope() {
    checkRightSyntax("check database record #12:3");
    checkRightSyntax("CHECK DATABASE RECORD #12:3 FIX");
    checkRightSyntax("check database record #12:3, #12:9 fix");
    checkRightSyntax("check database record #12:3 fix compress");
    // Accepted by the GRAMMAR; rejected at execution, since RECORD plus TYPE/BUCKET has no sensible meaning
    // (see CheckDatabaseRecordScopeTest.checkDatabaseRecordRejectsBeingCombinedWithTypeOrBucket).
    checkRightSyntax("check database type Customer record #12:3 fix");

    checkWrongSyntax("check database record");
    checkWrongSyntax("check database record Customer");
  }

  /**
   * #6360: {@code DEEP}, the tier that decodes the data instead of reconciling what describes it. It composes with
   * every other clause and is independent of {@code FIX} - nothing it finds is repairable, since a block whose
   * declared statistics disagree with its own values was written that way.
   */
  @Test
  void deepTier() {
    checkRightSyntax("CHECK DATABASE DEEP");
    checkRightSyntax("check database deep");
    checkRightSyntax("check database type Metrics deep");
    checkRightSyntax("check database fix deep");
    checkRightSyntax("check database type Metrics fix deep compress");

    // The keyword stays usable as an identifier, so a schema that already has a type called "deep" keeps parsing.
    checkRightSyntax("check database type deep");
    checkRightSyntax("select from deep");

    // DEEP comes after FIX, as the grammar orders every other optional clause of this statement.
    checkWrongSyntax("check database deep fix");

    // #6189's clause landed next to this one, so the order the two sit in is worth pinning rather than
    // rediscovering: RECLAIM UNREFERENCED FILES first, DEEP after it, COMPRESS last.
    checkRightSyntax("check database fix reclaim unreferenced files deep");
    checkRightSyntax("check database type Metrics fix delete orphans reclaim unreferenced files deep compress");
    checkWrongSyntax("check database fix deep reclaim unreferenced files");
  }
}
