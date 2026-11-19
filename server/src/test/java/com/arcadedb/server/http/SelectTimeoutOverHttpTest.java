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
package com.arcadedb.server.http;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11804: the HTTP command endpoint appends a LIMIT to a SELECT that states none, and appended after a TIMEOUT clause it
 * made a valid statement a syntax error. A statement with a LIMIT of its own and a {@code TIMEOUT ... EXCEPTION} got a second one.
 */
class SelectTimeoutOverHttpTest extends BaseGraphServerTest {

  @Test
  void aSelectEndingWithATimeoutClauseRuns() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE Timed");
      executeCommand(serverIndex, "sql", "INSERT INTO Timed CONTENT [{\"n\": 1}, {\"n\": 2}, {\"n\": 3}]");

      final Map<String, Integer> expectedRows = Map.ofEntries(//
          Map.entry("SELECT n FROM Timed TIMEOUT 5000", 3), //
          Map.entry("SELECT n FROM Timed TIMEOUT 5000 EXCEPTION", 3), //
          Map.entry("SELECT n FROM Timed ORDER BY n timeout 5000 return", 3), //
          Map.entry("SELECT n FROM (SELECT n FROM Timed) TIMEOUT 5000", 3), //
          Map.entry("SELECT n FROM Timed LIMIT 2 TIMEOUT 5000 EXCEPTION", 2), //
          Map.entry("SELECT n FROM Timed SKIP 1 LIMIT 1 TIMEOUT 5000 RETURN", 1), //
          Map.entry("SELECT n FROM Timed TIMEOUT /* budget */ 5000", 3), //
          Map.entry("SELECT n FROM Timed -- note\nTIMEOUT 5000", 3), //
          Map.entry("SELECT n FROM Timed TIMEOUT 5000L", 3), //
          Map.entry("SELECT n FROM Timed LIMIT 2 TIMEOUT 0x1388 RETURN", 2), //
          // A keyword written right after the number is a token of its own to the lexer
          Map.entry("SELECT n FROM Timed TIMEOUT 5000RETURN", 3), //
          Map.entry("SELECT n FROM Timed LIMIT 2 TIMEOUT 5000EXCEPTION", 2));
      for (final Map.Entry<String, Integer> statement : expectedRows.entrySet()) {
        final JSONObject response = executeCommand(serverIndex, "sql", statement.getKey());
        assertThat(response).as("refused over HTTP: %s", statement.getKey()).isNotNull();
        assertThat(response.getJSONObject("result").getJSONArray("records").length()).as(statement.getKey())
            .isEqualTo(statement.getValue());
      }
    });
  }
}
