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
 * Issue #11588: CALL { ... } IN TRANSACTIONS inside a transaction its caller owns (HTTP /begin, Bolt BEGIN, an embedded
 * begin() or transaction()) committed its batches on their own, so the caller's ROLLBACK did not undo them. The statement
 * is now refused there, as Neo4j does, and keeps running in batches in an implicit transaction: none at all, or the one a
 * server opens around one request.
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
  void refusedInEmbeddedTransactionAndNothingCommitted() {
    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("opencypher", "CREATE (:X {i: 0})");
      database.command("opencypher", STATEMENT);
    })).hasStackTraceContaining("can only be executed in an implicit transaction");

    assertThat(countX()).isZero();
  }

  @Test
  void refusedInEmbeddedBeginAndRollbackLeavesNothing() {
    database.begin();
    try {
      assertThatThrownBy(() -> database.command("opencypher", STATEMENT))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("implicit transaction");
    } finally {
      database.rollback();
    }

    assertThat(countX()).isZero();
  }

  @Test
  void runsInServerOpenedTransaction() {
    // What the HTTP auto-commit wrapper does around every command it runs
    database.transaction(() -> {
      ((DatabaseInternal) database).getTransaction().setImplicit(true);
      database.command("opencypher", STATEMENT);
    });
    assertThat(countX()).isEqualTo(25);
  }

  @Test
  void refusedOnceTheClientDeclaresTheImplicitTransactionItsOwn() {
    // A Postgres BEGIN sent inside the implicit block of a pipeline joins that block and declares it the client's
    database.begin();
    ((DatabaseInternal) database).getTransaction().setImplicit(true);
    ((DatabaseInternal) database).getTransaction().setClientManaged(true);
    try {
      assertThatThrownBy(() -> database.command("opencypher", STATEMENT))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("implicit transaction");
    } finally {
      database.rollback();
    }

    assertThat(countX()).isZero();
  }

  private long countX() {
    return database.query("opencypher", "MATCH (x:X) RETURN count(x) AS c").next().<Number>getProperty("c").longValue();
  }
}
