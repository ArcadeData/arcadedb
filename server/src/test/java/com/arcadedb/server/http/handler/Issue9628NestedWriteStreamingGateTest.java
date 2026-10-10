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
package com.arcadedb.server.http.handler;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9628: the NDJSON streaming gate admits only read-only statements, and read a write nested in a LET, a
 * projection or a FROM target as read-only because the outer statement was classified alone.
 */
class Issue9628NestedWriteStreamingGateTest extends TestHelper {

  @Test
  void refusesANestedWrite() {
    for (final String command : new String[] { //
        "SELECT FROM V LET $y = (INSERT INTO V SET a = 2)", //
        "SELECT (DELETE FROM V) as p FROM V", //
        "SELECT FROM (UPDATE V SET a = 6)", //
        "LET $z = (CREATE DOCUMENT TYPE Sneak)" })
      assertThatThrownBy(() -> AbstractQueryHandler.requireStreamableStatement(database, "sql", command))
          .as(command)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("read-only statement");
  }

  @Test
  void refusesABlockAroundBackup() {
    assertThatThrownBy(() -> AbstractQueryHandler.requireStreamableStatement(database, "sqlscript", "IF (1=1) { BACKUP DATABASE; }"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("read-only statement");
  }

  @Test
  void stillAllowsANestedRead() {
    assertThatCode(() -> AbstractQueryHandler.requireStreamableStatement(database, "sql",
        "SELECT $y FROM V LET $y = (SELECT count(*) FROM V)")).doesNotThrowAnyException();
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
  }
}
