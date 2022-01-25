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

public class CommitStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("COMMIT");
    checkRightSyntax("commit");

    checkRightSyntax("COMMIT RETRY 10");
    checkRightSyntax("commit retry 10");

    //checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';}");
//    checkRightSyntax(
//        "commit retry 10 ELSE {INSERT INTO A SET name = 'Foo'; INSERT INTO A SET name = 'Bar';}");
//
//    checkRightSyntax("commit retry 10 ELSE CONTINUE");
//    checkRightSyntax("commit retry 10 ELSE FAIL");
//    checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} AND CONTINUE");
//    checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} AND FAIL");
//
//    checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} and continue");
//    checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} and fail");

    checkWrongSyntax("COMMIT RETRY 10.1");
    checkWrongSyntax("COMMIT RETRY 10,1");
    checkWrongSyntax("COMMIT RETRY foo");
    checkWrongSyntax("COMMIT RETRY");
    checkWrongSyntax("COMMIT 10.1");
    checkWrongSyntax("commit retry 10 ELSE {}");
    checkWrongSyntax("commit retry 10 ELSE");
    checkWrongSyntax("commit ELSE {INSERT INTO A SET name = 'Foo';}");
    checkWrongSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} AND ");
    checkWrongSyntax("commit retry 10 ELSE AND CONTINUE");
    checkWrongSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} CONTINUE");
    checkWrongSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} FAIL");
  }
}
