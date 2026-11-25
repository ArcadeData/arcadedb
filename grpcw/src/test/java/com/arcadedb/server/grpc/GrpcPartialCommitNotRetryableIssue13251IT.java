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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13251 over gRPC: an auto-commit command whose statement committed part of its work ({@code UPDATE ... BATCH n}) and then
 * failed on a conflict is not replayed by the server, but it was answered {@code ABORTED}, which every gRPC client reads as "run
 * the transaction again", and the rerun applied the committed batches once more. It is answered with a status no client retries.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class GrpcPartialCommitNotRetryableIssue13251IT extends BaseGrpcServerTest {
  private static final String ROWS    = "Issue13251GrpcRow";
  private static final String TRIGGER = "Issue13251GrpcTrigger";
  private static final int    SIZE    = 25;

  private final AtomicInteger                                   conflicts = new AtomicInteger();
  private       BeforeRecordUpdateListener                      conflict;
  private       ManagedChannel                                  channel;
  private       ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setUp() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE " + ROWS);
    database.command("sql", "CREATE PROPERTY " + ROWS + ".seq INTEGER");
    database.command("sql", "CREATE VERTEX TYPE " + TRIGGER);
    database.transaction(() -> {
      for (int i = 0; i < SIZE; i++)
        database.newVertex(ROWS).set("seq", 0).save();
      database.newVertex(TRIGGER).set("seq", 0).save();
    });

    // The statement after the batches fails the way a genuine MVCC conflict on it would
    conflict = record -> {
      if (record instanceof Vertex vertex && vertex.getTypeName().equals(TRIGGER)) {
        conflicts.incrementAndGet();
        throw new ConcurrentModificationException("simulated conflict after the batches committed (issue #13251)");
      }
      return true;
    };
    database.getEvents().registerListener(conflict);

    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(
        ClientInterceptors.intercept(channel, new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName())));
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    getServerDatabase(0, getDatabaseName()).getEvents().unregisterListener(conflict);
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aBatchedUpdateFollowedByAConflictIsNotAnsweredAborted() {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setLanguage("sqlscript")
        .setCommand("UPDATE %s SET seq = seq + 1 BATCH 10; UPDATE %s SET seq = seq + 1;".formatted(ROWS, TRIGGER))
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build()).build();

    assertThatThrownBy(() -> stub.executeCommand(request)).isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
      assertThat(e.getStatus().getCode()).as("ABORTED tells a gRPC client to run it again").isNotEqualTo(Status.Code.ABORTED)
          .isNotEqualTo(Status.Code.UNAVAILABLE);
      assertThat(e.getStatus().getDescription()).contains("committed part of its work");
    });

    assertThat(conflicts.get()).as("the command ran once").isEqualTo(1);
    assertThat(countRowsWithSeq(1)).as("the 20 rows the batches committed are incremented exactly once").isEqualTo(20);
    assertThat(countRowsWithSeq(0)).as("the last 5 were rolled back").isEqualTo(5);
  }

  private long countRowsWithSeq(final int seq) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql",
        "SELECT count(*) AS c FROM " + ROWS + " WHERE seq = ?", seq)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
