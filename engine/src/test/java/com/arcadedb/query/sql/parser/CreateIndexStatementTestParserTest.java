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

class CreateIndexStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("create index `OUser.name` UNIQUE ENGINE LSM");
    checkRightSyntax("create index `OUser.name` UNIQUE engine LSM");
    checkRightSyntax("create index `OUser.name` IF NOT EXISTS UNIQUE engine LSM");
    checkRightSyntax("create index `OUser.name` UNIQUE ENGINE LSM METADATA {\"test\": 3}");

    checkRightSyntax("CREATE INDEX on Foo (bar, baz) UNIQUE");
    checkRightSyntax("CREATE INDEX on Foo (bar, @rid) UNIQUE");
    checkRightSyntax("CREATE INDEX on Foo (bar by key, baz by value) UNIQUE");
    checkRightSyntax("CREATE INDEX on Foo (bar) UNIQUE");
    checkRightSyntax("CREATE INDEX on Foo (bar, baz) UNIQUE");
    checkRightSyntax("CREATE INDEX on Foo (bar, baz) UNIQUE");
    checkRightSyntax("CREATE INDEX on Foo (bar by key, baz by value) UNIQUE");
    checkRightSyntax("CREATE INDEX on Foo (tags by item) NOTUNIQUE");
    checkRightSyntax("CREATE INDEX on Foo (identifiers by item) NOTUNIQUE");
    checkRightSyntax("CREATE INDEX on Foo (items by item, name by key) UNIQUE");
    checkRightSyntax("CREATE INDEX IF NOT EXISTS on Foo (bar) UNIQUE");
    checkRightSyntax("CREATE INDEX IF NOT EXISTS on Foo (bar) UNIQUE NULL_STRATEGY SKIP");
    checkRightSyntax("CREATE INDEX IF NOT EXISTS on Foo (bar) UNIQUE ENGINE LSM");
    checkRightSyntax("CREATE INDEX IF NOT EXISTS on Foo (bar) UNIQUE METADATA {\"test\": 3}");

    checkWrongSyntax("CREATE INDEX `OUser.name` on Foo (bar, baz) UNIQUE");
    checkWrongSyntax("CREATE INDEX Foo");
    checkWrongSyntax("CREATE INDEX on Foo (bar) wUNIQUE");
    checkWrongSyntax("CREATE INDEX IF EXISTS on Foo (bar) UNIQUE");
    checkWrongSyntax("CREATE INDEX on Foo (bar) wUNIQUE");
  }
}
