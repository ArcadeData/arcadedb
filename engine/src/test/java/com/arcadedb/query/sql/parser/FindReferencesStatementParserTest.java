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

class FindReferencesStatementParserTest extends AbstractParserTest {

  @Test
  void rightSyntax() {
    checkRightSyntax("FIND REFERENCES #12:0");
    checkRightSyntax("find references #12:0");
    checkRightSyntax("FIND REFERENCES #12:0 [Person]");
    checkRightSyntax("FIND REFERENCES #12:0 [Person, Animal]");
    checkRightSyntax("FIND REFERENCES #12:0 [Person, bucket:animal]");
    checkRightSyntax("FIND REFERENCES (select from foo where name = ?)");
    checkRightSyntax("FIND REFERENCES (select from foo where name = ?) [Person, bucket:animal]");
  }

  @Test
  void wrongSyntax() {
    checkWrongSyntax("FIND REFERENCES");
    checkWrongSyntax("FIND REFERENCES #12:0 #12:1");
    checkWrongSyntax("FIND REFERENCES #12:0, #12:1");
    checkWrongSyntax("FIND REFERENCES [#12:0, #12:1]");
    checkWrongSyntax("FIND REFERENCES foo");
  }
}
