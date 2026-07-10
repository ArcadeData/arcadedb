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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5042: when the server-side idle reaper has already rolled back and discarded a
 * transaction, the {@link RemoteGrpcDatabase} client must surface the failure instead of silently treating a
 * {@code committed=false}/{@code rolledBack=false} response as success.
 *
 * <p>Also verifies the streaming analogue of the #4260 fix: a streamed query issued inside an
 * externally-managed transaction sees the transaction's own uncommitted writes.
 */
@Tag("slow")
public class Issue5042CommitRollbackThrowIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT   = 50051;
  private static final int    HTTP_PORT   = 2480;
  private static final String VERTEX_TYPE = "Issue5042Vertex";

  // Short reaper windows so the abandoned transaction is reclaimed quickly and deterministically.
  private static final String MAX_IDLE_MS      = "1000";
  private static final String REAPER_PERIOD_MS = "250";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
    // ContextConfiguration falls back to system properties for these plugin-specific keys.
    System.setProperty("arcadedb.grpc.tx.maxIdleMs", MAX_IDLE_MS);
    System.setProperty("arcadedb.grpc.tx.reaperPeriodMs", REAPER_PERIOD_MS);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    System.clearProperty("arcadedb.grpc.tx.maxIdleMs");
    System.clearProperty("arcadedb.grpc.tx.reaperPeriodMs");
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
    if (grpcServer != null)
      grpcServer.close();
  }

  @Test
  void commitThrowsWhenTransactionWasReaped() throws InterruptedException {
    grpc.begin();

    // Simulate a long client pause (GC pause, network stall, debugger): the idle reaper rolls the
    // transaction back and discards it while the client is not looking.
    Thread.sleep(Long.parseLong(MAX_IDLE_MS) + 1500L);

    // The server reports success=true, committed=false for the now-unknown transaction. The client must
    // NOT treat that as a durable commit - it must throw.
    assertThatThrownBy(() -> grpc.commit()).isInstanceOf(TransactionException.class);
  }

  @Test
  void rollbackThrowsWhenTransactionWasReaped() throws InterruptedException {
    grpc.begin();

    Thread.sleep(Long.parseLong(MAX_IDLE_MS) + 1500L);

    // The server reports success=true, rolledBack=false for the now-unknown transaction. The client must
    // surface that instead of silently reporting a clean rollback.
    assertThatThrownBy(() -> grpc.rollback()).isInstanceOf(TransactionException.class);
  }

  @Test
  void streamedQueryInsideTransactionSeesUncommittedWrite() {
    grpc.begin();
    try {
      final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
      v.set("svex", "streamed");
      v.save();
      assertThat(v.getIdentity()).isNotNull();

      // A streamed read bound to the same open transaction must see the uncommitted vertex.
      int seen = 0;
      try (final ResultSet rs = grpc.queryStream("sql",
          "SELECT FROM `" + VERTEX_TYPE + "` WHERE svex = 'streamed'", (Map<String, Object>) null, 10)) {
        while (rs.hasNext()) {
          rs.next();
          seen++;
        }
      }
      assertThat(seen).as("streamed query inside the transaction must see its uncommitted write").isEqualTo(1);

      grpc.commit();
    } catch (final RuntimeException re) {
      try {
        grpc.rollback();
      } catch (final Throwable ignore) {
        // ignore
      }
      throw re;
    }
  }
}
