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
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4260: {@link RemoteGrpcDatabase} fails with
 * "Record not found" when reloading a record that was created and saved inside
 * an open transaction but not yet committed. The same flow works fine with the
 * HTTP/JSON {@link RemoteDatabase} client.
 *
 * <p>Root cause: gRPC's {@code executeQuery} server handler used to run on the
 * calling gRPC worker thread even when the request carried an externally-managed
 * transaction id. ArcadeDB transactions are thread-bound: changes pending in a
 * transaction (such as a vertex saved but not yet committed) are visible only to
 * the thread that owns the transaction. The query therefore missed the in-flight
 * record. The fix dispatches {@code executeQuery} to the transaction's dedicated
 * executor thread, matching what {@code executeCommand}, {@code createRecord},
 * {@code updateRecord} and {@code deleteRecord} already do.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue4260ReloadInsideTransactionIT extends BaseGraphServerTest {

  private static final int GRPC_PORT   = 50051;
  private static final int HTTP_PORT   = 2480;
  private static final String VERTEX_TYPE = "Issue4260Vertex";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

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
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, HTTP_PORT, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);

    grpc.command("sql", "CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.svex IF NOT EXISTS STRING");
    grpc.command("sql", "DELETE FROM `" + VERTEX_TYPE + "`");
  }

  @AfterEach
  void closeClient() {
    if (grpc != null) {
      try {
        grpc.rollback();
      } catch (final Throwable ignore) {
        // ignore
      }
      grpc.close();
    }
    if (grpcServer != null) {
      grpcServer.close();
    }
  }

  @Test
  void reloadOfUncommittedVertexInsideTransactionMustSucceed() {
    grpc.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
      v.set("svex", "svt1");
      v.save();
      assertThat(v.getIdentity()).as("save() should assign a RID").isNotNull();

      // The bug: prior to the fix the gRPC server ran executeQuery on the worker thread
      // (not the transaction's owner thread) so the in-flight vertex was not visible and
      // reload() threw RemoteException("Record <rid> not found").
      v.reload();

      assertThat(v.<String>get("svex")).isEqualTo("svt1");

      // The same record must also be reachable via a plain query inside the same tx.
      try (final ResultSet rs = grpc.query("sql", "SELECT FROM " + v.getIdentity())) {
        assertThat(rs.hasNext()).isTrue();
        final Result r = rs.next();
        assertThat(r.<String>getProperty("svex")).isEqualTo("svt1");
      }

      grpc.commit();
    } catch (final RuntimeException re) {
      try {
        grpc.rollback();
      } catch (final Throwable ignore) {
        // ignore
      }
      throw re;
    }

    // After commit the record must still be there.
    try (final ResultSet rs = grpc.query("sql", "SELECT FROM `" + VERTEX_TYPE + "` WHERE svex = 'svt1'")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }
}
