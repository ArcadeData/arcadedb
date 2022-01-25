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

public class TruncateTypeStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("TRUNCATE TYPE Foo");
    checkRightSyntax("truncate type Foo");
    checkRightSyntax("TRUNCATE TYPE Foo polymorphic");
    checkRightSyntax("truncate type Foo POLYMORPHIC");
    checkRightSyntax("TRUNCATE TYPE Foo unsafe");
    checkRightSyntax("truncate type Foo UNSAFE");
    checkRightSyntax("TRUNCATE TYPE Foo polymorphic unsafe");
    checkRightSyntax("truncate type Foo POLYMORPHIC UNSAFE");
    checkWrongSyntax("TRUNCATE TYPE Foo polymorphic unsafe FOO");
    checkRightSyntax("truncate type `Foo bar` ");
    checkWrongSyntax("truncate type Foo bar ");
    checkWrongSyntax("truncate clazz Foo ");
  }
}
