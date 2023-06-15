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

public class DropTypeStatementTestParserTest extends AbstractParserTest {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP TYPE Foo");
    checkRightSyntax("drop type Foo");
    checkRightSyntax("drop type Foo UNSAFE");
    checkRightSyntax("drop type Foo unsafe");
    checkRightSyntax("DROP TYPE `Foo bar`");
    checkRightSyntax("drop type ?");

    checkWrongSyntax("drop type Foo UNSAFE foo");
    checkWrongSyntax("drop type Foo bar");
  }

  @Test
  public void testIfExists() {
    checkRightSyntax("DROP TYPE Foo if exists");
    checkRightSyntax("DROP TYPE Foo IF EXISTS");
    checkRightSyntax("DROP TYPE if if exists");
    checkRightSyntax("DROP TYPE if if exists unsafe");
    checkRightSyntax("DROP TYPE ? IF EXISTS");

    checkWrongSyntax("drop type Foo if");
    checkWrongSyntax("drop type Foo if exists lkj");
    checkWrongSyntax("drop type Foo if lkj");
  }
}
