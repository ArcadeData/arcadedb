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

class DefineFunctionStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("DEFINE FUNCTION test.print \"print('\\nTest!')\"");
    checkRightSyntax("DEFINE FUNCTION test.dummy \"return a + b;\" PARAMETERS [a,b]");
    checkRightSyntax("DEFINE FUNCTION users.allUsersButAdmin \"SELECT FROM ouser WHERE name <> 'admin'\" LANGUAGE SQL");
    checkRightSyntax("define function users.allUsersButAdmin \"SELECT FROM ouser WHERE name <> 'admin'\" parameters [a,b] language SQL");

    checkWrongSyntax("DEFINE FUNCTION test \"print('\\nTest!')\"");
  }

  @Test
  void deleteFunction() {
    checkRightSyntax("DELETE FUNCTION test.print");
    checkRightSyntax("DELETE FUNCTION math.sum");
    checkRightSyntax("delete function users.allUsersButAdmin");

    checkWrongSyntax("DELETE FUNCTION test");  // Missing function name (only library)
    checkWrongSyntax("DELETE FUNCTION");       // Missing both library and function name
  }
}
