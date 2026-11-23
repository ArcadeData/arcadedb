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
import com.arcadedb.exception.CommandSemanticException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12202: over gRPC, a SQL {@code BEGIN}, {@code COMMIT} or {@code ROLLBACK} sent as a command of its own outside a transaction
 * was answered OK and did nothing: the call ran in its own transaction, so a later {@code ROLLBACK} undid nothing. It is refused
 * now (INVALID_ARGUMENT, rebuilt as a {@link CommandSemanticException}); inside a transaction of the client, and as a
 * {@code sqlscript}, it runs as before.
 */
public class Issue12202GrpcStandaloneTransactionControlIT extends BaseGrpcClientServerTest {

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void open() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "CREATE DOCUMENT TYPE D12202");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aTransactionControlOutsideATransactionIsRefused() {
    for (final String statement : new String[] { "begin", "commit", "ROLLBACK" })
      assertThatThrownBy(() -> grpc.command("sql", statement)).as(statement).isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("RemoteGrpcDatabase.begin()");

    grpc.command("sql", "INSERT INTO D12202 SET k = 'kept'");
    assertThat(count()).isEqualTo(1L);
  }

  /** The openCypher spelling has the same gap (issue #12232). */
  @Test
  void anOpenCypherTransactionControlOutsideATransactionIsRefused() {
    for (final String statement : new String[] { "START TRANSACTION", "COMMIT", "ROLLBACK" })
      assertThatThrownBy(() -> grpc.command("opencypher", statement)).as(statement).isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("RemoteGrpcDatabase.begin()");
  }

  @Test
  void theTransactionApiAndAScriptStillWork() {
    grpc.begin();
    grpc.command("sql", "INSERT INTO D12202 SET k = 'rolledBack'");
    grpc.rollback();
    assertThat(count()).isZero();

    grpc.command("sqlscript", "BEGIN; INSERT INTO D12202 SET k = 'scriptRolledBack'; ROLLBACK;");
    assertThat(count()).isZero();
    grpc.command("sqlscript", "BEGIN; INSERT INTO D12202 SET k = 'scriptCommitted'; COMMIT;");
    assertThat(count()).isEqualTo(1L);
  }

  private long count() {
    return grpc.query("sql", "SELECT count(@rid) AS c FROM D12202").next().<Number>getProperty("c").longValue();
  }
}
