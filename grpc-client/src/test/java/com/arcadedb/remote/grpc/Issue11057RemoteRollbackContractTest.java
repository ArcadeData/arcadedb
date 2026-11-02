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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11057: {@code rollback()} with no active transaction is a no-op on the embedded engine ("ALREADY ROLLED BACK") and threw
 * "Transaction not begun" on both remote drivers. {@code commit()} clears the session, so the standard defensive shape - a
 * {@code finally} that rolls back whatever the block left open - threw on the SUCCESS path, after the data was durable, and the
 * caller reported a committed unit of work as failed. The same transaction state machine is now driven through the three
 * implementations of {@link BasicDatabase}, the states that are not errors included.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11057RemoteRollbackContractTest extends BaseGrpcClientServerTest {
  private static final String TYPE = "Doc11057";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;
  private RemoteDatabase     http;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeEach
  void open() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    http = new RemoteDatabase("localhost", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    http.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
  }

  @AfterEach
  void close() {
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
    if (http != null)
      http.close();
  }

  /** The reported shape: begin, work, commit, and the rollback in a {@code finally} that runs after the commit too. */
  @ParameterizedTest
  @ValueSource(strings = { "embedded", "http", "grpc" })
  void aRollbackInFinallyAfterACommitIsANoOp(final String implementation) {
    final BasicDatabase db = database(implementation);
    final long before = count();

    db.begin();
    try {
      db.newDocument(TYPE).set("by", implementation).save();
      db.commit();
    } finally {
      db.rollback();
    }

    assertThat(db.isTransactionActive()).isFalse();
    assertThat(count()).as("the unit of work committed on " + implementation).isEqualTo(before + 1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "embedded", "http", "grpc" })
  void aSecondRollbackAndARollbackWithoutBeginAreNoOps(final String implementation) {
    final BasicDatabase db = database(implementation);
    final long before = count();

    db.rollback();

    db.begin();
    db.newDocument(TYPE).set("by", implementation).save();
    db.rollback();
    db.rollback();

    assertThat(db.isTransactionActive()).isFalse();
    assertThat(count()).as("nothing was committed on " + implementation).isEqualTo(before);
  }

  /** The counterweight: an open transaction is still rolled back, and a commit without one still refuses. */
  @ParameterizedTest
  @ValueSource(strings = { "embedded", "http", "grpc" })
  void anOpenTransactionIsStillRolledBackAndACommitWithoutOneStillRefuses(final String implementation) {
    final BasicDatabase db = database(implementation);
    final long before = count();

    db.begin();
    db.newDocument(TYPE).set("by", implementation).save();
    assertThat(db.isTransactionActive()).isTrue();
    db.rollback();

    assertThat(db.isTransactionActive()).isFalse();
    assertThat(count()).isEqualTo(before);
    assertThatThrownBy(db::commit).isInstanceOf(TransactionException.class);
  }

  /**
   * The half the drivers cannot meet, now stated on {@code BasicDatabase.begin()}: a server session holds one transaction, so a
   * remote {@code begin()} inside a transaction refuses rather than nesting, and leaves the open transaction usable.
   */
  @ParameterizedTest
  @ValueSource(strings = { "http", "grpc" })
  void aRemoteBeginInsideATransactionRefusesAndKeepsTheOpenOne(final String implementation) {
    final BasicDatabase db = database(implementation);
    final long before = count();

    db.begin();
    try {
      assertThatThrownBy(db::begin).isInstanceOf(TransactionException.class).hasMessageContaining("already begun");
      assertThat(db.isTransactionActive()).isTrue();
      db.newDocument(TYPE).set("by", implementation).save();
      db.commit();
    } finally {
      db.rollback();
    }
    assertThat(count()).isEqualTo(before + 1);
  }

  private long count() {
    return embedded().countType(TYPE, false);
  }

  private BasicDatabase database(final String implementation) {
    return switch (implementation) {
      case "embedded" -> embedded();
      case "http" -> http;
      case "grpc" -> grpc;
      default -> throw new IllegalArgumentException(implementation);
    };
  }

  private Database embedded() {
    return getServerDatabase(0, getDatabaseName());
  }
}
