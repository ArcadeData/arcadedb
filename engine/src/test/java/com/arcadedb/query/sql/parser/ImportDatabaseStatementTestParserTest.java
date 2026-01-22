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

class ImportDatabaseStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("IMPORT DATABASE http://www.foo.bar");
    checkRightSyntax("import database http://www.foo.bar");
    checkRightSyntax("IMPORT DATABASE https://www.foo.bar");
    checkRightSyntax("IMPORT DATABASE file:///foo/bar/");
    checkRightSyntax("IMPORT DATABASE http://www.foo.bar WITH forceDatabaseCreate = true");
    checkRightSyntax("IMPORT DATABASE http://www.foo.bar WITH forceDatabaseCreate = true, commitEvery = 10000");

    checkWrongSyntax("import database file:///foo/bar/ foo bar");
    checkWrongSyntax("import database http://www.foo.bar asdf ");
    checkWrongSyntax("IMPORT DATABASE https://www.foo.bar asd ");
  }

  /**
   * Regression test for GitHub issue #1552.
   * IMPORT DATABASE should allow optional URL when vertices/edges files are specified.
   */
  @Test
  void testRegression_Issue1552_OptionalUrl() {
    // URL should be optional when using vertices/edges settings
    checkRightSyntax("IMPORT DATABASE WITH vertices=\"file://vertices.csv\"");
    checkRightSyntax("IMPORT DATABASE WITH vertices=\"file://vertices.csv\", verticesFileType=csv, typeIdProperty=Id");
    checkRightSyntax(
        "IMPORT DATABASE WITH vertices=\"file://vertices.csv\", verticesFileType=csv, typeIdProperty=Id, " +
            "edges=\"file://edges.csv\", edgesFileType=csv, edgeFromField=\"From\", edgeToField=\"To\"");
  }
}
