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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11049, end to end: a database a {@code close database} command closed reached the gRPC client as a
 * {@link NeedRetryException}, so {@code transaction()} spent its whole retry budget, with the back-off between attempts,
 * against a database that stays closed until an operator opens it. The HTTP client rebuilt the type and failed once. Run in
 * production mode, which conceals the free-form text, so what the client acts on has to be the structured answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11049GrpcClosedDatabaseTest extends BaseGrpcClientServerTest {
  private static final String NAME = "Issue11049Held";

  private ArcadeDBServer   server;
  private RemoteGrpcServer admin;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_MODE, "production");
  }

  @BeforeEach
  void createDatabase() {
    server = getServer(0);
    final ServerDatabase created = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    created.getSchema().createDocumentType("T");
    admin = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterEach
  void dropDatabase() {
    if (admin != null)
      admin.close();
    server.unmarkClosedByCommand(NAME);
    if (server.existsDatabase(NAME))
      server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
    FileUtils.deleteRecursively(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
  }

  @Test
  void aDatabaseClosedByCommandFailsOnceWithItsReason() {
    try (final RemoteGrpcDatabase database = new RemoteGrpcDatabase(admin, "localhost", getServerGrpcPort(), getServerHttpPort(0),
        NAME, "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      admin.closeDatabase(NAME);

      assertThatThrownBy(() -> database.query("sql", "select from T").close())
          .isExactlyInstanceOf(DatabaseNotAvailableException.class)
          .satisfies(e -> assertThat(((DatabaseNotAvailableException) e).getReason()).isEqualTo(
              DatabaseNotAvailableException.Reason.CLOSED));

      assertThatThrownBy(() -> database.command("sql", "insert into T set k = 1").close())
          .isExactlyInstanceOf(DatabaseNotAvailableException.class);

      // the probe of the report: the error callback runs once per attempt, and there is only one
      final AtomicInteger attempts = new AtomicInteger();
      final AtomicInteger executions = new AtomicInteger();
      assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, true, 5, null, e -> attempts.incrementAndGet()))
          .isInstanceOf(TransactionException.class)
          .isNotInstanceOf(NeedRetryException.class)
          .satisfies(e -> assertThat(e.getCause()).isInstanceOfSatisfying(DatabaseNotAvailableException.class,
              c -> assertThat(c.getReason()).isEqualTo(DatabaseNotAvailableException.Reason.CLOSED)));
      assertThat(attempts.get()).as("a database closed on purpose is not retried").isEqualTo(1);
      assertThat(executions.get()).isZero();

      admin.openDatabase(NAME);
      try (final ResultSet rs = database.query("sql", "select count(*) as c from T")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
      }
    }
  }
}
