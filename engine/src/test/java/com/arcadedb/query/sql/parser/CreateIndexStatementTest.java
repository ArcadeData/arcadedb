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

public class CreateIndexStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar, baz) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar, @rid) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar by key, baz by value) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar on Foo (bar) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar, baz) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar by key, baz by value) UNIQUE");
    checkRightSyntax("create index OUser.name UNIQUE ENGINE LSM");
    checkRightSyntax("create index OUser.name UNIQUE engine LSM");
    checkRightSyntax("CREATE INDEX Foo.bar IF NOT EXISTS on Foo (bar) UNIQUE");

    checkWrongSyntax("CREATE INDEX Foo");
    checkWrongSyntax("CREATE INDEX Foo.bar on Foo (bar) wUNIQUE");
    checkWrongSyntax("CREATE INDEX Foo.bar IF EXISTS on Foo (bar) UNIQUE");
  }
}
