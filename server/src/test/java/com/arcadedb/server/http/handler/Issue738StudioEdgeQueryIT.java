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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for ops issue #738 (Locstat / Johan Hoogenboezem).
 * <p>
 * A query that returns a single EDGE over the Studio serializer must surface only that edge in the
 * graph, not every sibling edge that happens to run between the same two endpoints. The two Account
 * vertices below are connected by many TRANSFER edges; a query pinned to one transaction returns a
 * single record but used to render all of them in the Graph view, because the "filter out not
 * connected edges" pass added every edge between two included vertices - and an edge row adds both of
 * its endpoints to that set.
 */
class Issue738StudioEdgeQueryIT extends BaseGraphServerTest {

  @Test
  void selectSingleEdgeDoesNotSurfaceSiblingEdges() throws Exception {
    // Schema modelled on the client's: Account vertices, TRANSFER edges keyed by (transactionId, date).
    executeCommand(0, "sql", "CREATE VERTEX TYPE Account");
    executeCommand(0, "sql", "CREATE PROPERTY Account.accountNumber STRING");
    executeCommand(0, "sql", "CREATE EDGE TYPE TRANSFER");
    executeCommand(0, "sql", "CREATE PROPERTY TRANSFER.transactionId STRING");
    executeCommand(0, "sql", "CREATE PROPERTY TRANSFER.date DATE");
    executeCommand(0, "sql", "CREATE INDEX TRANSFER_PK ON TRANSFER (transactionId, date) UNIQUE");

    executeCommand(0, "sql", "INSERT INTO Account SET accountNumber = 'from'");
    executeCommand(0, "sql", "INSERT INTO Account SET accountNumber = 'to'");

    // 11 TRANSFER edges between the SAME two accounts, one per day, distinct transactionIds.
    final int edgeCount = 11;
    for (int i = 0; i < edgeCount; i++) {
      final String txId = "745298" + (10 + i);
      final String date = String.format("2026-02-%02d", 1 + i);
      executeCommand(0, "sql", "CREATE EDGE TRANSFER "
          + "FROM (SELECT FROM Account WHERE accountNumber = 'from') "
          + "TO (SELECT FROM Account WHERE accountNumber = 'to') "
          + "SET transactionId = '" + txId + "', date = date('" + date + "', 'yyyy-MM-dd')");
    }

    // Client's query: pinned to one (transactionId, date) via the unique index.
    final JSONObject response = executeCommand(0, "sql",
        "SELECT FROM TRANSFER WHERE transactionId = '74529810' AND date = date('2026-02-01', 'yyyy-MM-dd')");
    assertThat(response).isNotNull();
    final JSONObject result = response.getJSONObject("result");

    assertThat(result.getJSONArray("records").length()).as("Exactly one TRANSFER matches the predicate").isEqualTo(1);
    assertThat(result.getJSONArray("edges").length())
        .as("Only the queried edge must be in the graph, not the %d sibling TRANSFER edges", edgeCount).isEqualTo(1);
    // Both endpoints of the single edge are still expected in the graph.
    assertThat(result.getJSONArray("vertices").length()).as("The two endpoints of the queried edge").isEqualTo(2);
  }
}
