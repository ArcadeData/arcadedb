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

class CreateEdgeStatementTest extends AbstractParserTest {

  @Test
  void simpleCreate() {
    checkRightSyntax("create edge Foo from (Select from a) to (Select from b)");
  }

  @Test
  void createFromRid() {
    checkRightSyntax("create edge Foo from #11:0 to #11:1");
  }

  @Test
  void createFromRidArray() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0]");
  }

  @Test
  void createFromRidSet() {
    checkRightSyntax("create edge Foo from #11:0 to #11:1 set foo='bar', bar=2");
  }

  @Test
  void createFromRidArraySet() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] set foo='bar', bar=2");
  }

  @Test
  void batch() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] set foo='bar', bar=2");
  }

  @Test
  void inputVariables() {
    checkRightSyntax("create edge Foo from ? to ?");
    checkRightSyntax("create edge Foo from :a to :b");
    checkRightSyntax("create edge Foo from [:a, :b] to [:b, :c]");
  }

  @Test
  void subStatements() {
    checkRightSyntax("create edge Foo from (select from Foo) to (select from bar)");
    checkRightSyntax("create edge Foo from (traverse out() from #12:0) to (select from bar)");
    checkRightSyntax("create edge Foo from (MATCH {type:Person, as:A} return $elements) to (select from bar)");
  }
}
