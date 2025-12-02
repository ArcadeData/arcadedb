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

class CreatePropertyStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("CREATE PROPERTY Foo.bar STRING");
    checkRightSyntax("create property Foo.bar STRING");

    checkRightSyntax("CREATE PROPERTY Foo.bar LINK");

    checkRightSyntax("CREATE PROPERTY `Foo bar`.`bar baz` LINK");

    checkRightSyntax("CREATE PROPERTY Foo.bar Integer (MANDATORY, READONLY, NOTNULL, MAX 5, MIN 3, DEFAULT 7)");

    checkRightSyntax("CREATE PROPERTY Foo.bar LINK (MANDATORY, READONLY, NOTNULL, MAX 5, MIN 3, DEFAULT 7)");

    checkRightSyntax("CREATE PROPERTY Foo.bar LINK (MANDATORY true, READONLY false, NOTNULL true, MAX 5, MIN 3, DEFAULT 7)");
  }

  @Test
  void ifNotExists() {
    checkRightSyntax("CREATE PROPERTY Foo.bar if not exists STRING");
    checkWrongSyntax("CREATE PROPERTY Foo.bar if exists STRING");
    checkWrongSyntax("CREATE PROPERTY Foo.bar if not exists");
  }
}
