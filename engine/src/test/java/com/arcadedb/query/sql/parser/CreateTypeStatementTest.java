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

public class CreateTypeStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE DOCUMENT TYPE Foo");
    checkRightSyntax("create document type Foo");
    checkRightSyntax("create document type Foo extends bar, baz bucket 12, 13, 14 ");
    checkRightSyntax("CREATE DOCUMENT TYPE Foo EXTENDS bar, baz BUCKET 12, 13, 14");
    checkRightSyntax("CREATE DOCUMENT TYPE Foo EXTENDS bar, baz BUCKETS 5");

    checkWrongSyntax("CREATE DOCUMENT TYPE Foo EXTENDS ");
    checkWrongSyntax("CREATE DOCUMENT TYPE Foo BUCKET ");
    checkWrongSyntax("CREATE DOCUMENT TYPE Foo BUCKETS ");
    checkWrongSyntax("CREATE DOCUMENT TYPE Foo BUCKETS 1,2 ");
  }

  @Test
  public void testIfNotExists() {
    checkRightSyntax("CREATE DOCUMENT TYPE Foo if not exists");
    checkRightSyntax("CREATE DOCUMENT TYPE Foo IF NOT EXISTS");
    checkRightSyntax("CREATE DOCUMENT TYPE Foo if not exists extends V");

    checkWrongSyntax("CREATE DOCUMENT TYPE Foo if");
    checkWrongSyntax("CREATE DOCUMENT TYPE Foo if not");
  }
}
