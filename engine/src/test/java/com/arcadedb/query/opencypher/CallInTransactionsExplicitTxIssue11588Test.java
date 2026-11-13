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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11588: CALL { ... } IN TRANSACTIONS inside a transaction the client opened (HTTP /begin, Bolt BEGIN) committed
 * its batches on their own, so the client's ROLLBACK did not undo them. The statement is now refused there, as Neo4j
 * does, and keeps running in batches in an implicit transaction.
 */
class CallInTransactionsExplicitTxIssue11588Test extends TestHelper {

  private static final String STATEMENT = "UNWIND range(1, 25) AS i CALL { WITH i CREATE (:X {i: i}) } IN TRANSACTIONS OF 10 ROWS";

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createVertexType("X"));
  }

  @Test
  void refusedInClientManagedTransactionAndRollbackLeavesNothing() {
    database.begin();
    // What PostBeginHandler and the Bolt BEGIN do for the transaction the client owns
    ((DatabaseInternal) database).getTransaction().setClientManaged(true);
    try {
      database.command("opencypher", "CREATE (:X {i: 0})");
      assertThatThrownBy(() -> database.command("opencypher", STATEMENT))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("implicit transaction");
    } finally {
      database.rollback();
    }

    assertThat(countX()).isZero();
  }

  @Test
  void runsInBatchesWithoutTransaction() {
    database.command("opencypher", STATEMENT);
    assertThat(countX()).isEqualTo(25);
  }

  @Test
  void runsInServerOpenedTransaction() {
    // The HTTP auto-commit wrapper runs every command inside database.transaction(): not client managed
    database.transaction(() -> database.command("opencypher", STATEMENT));
    assertThat(countX()).isEqualTo(25);
  }

  private long countX() {
    return database.query("opencypher", "MATCH (x:X) RETURN count(x) AS c").next().<Number>getProperty("c").longValue();
  }
}
