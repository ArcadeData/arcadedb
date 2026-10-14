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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.query.sql.executor.ResultSet;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9974: the counts an insert summary replies are the records the engine wrote. The insert path counted {@code inserted} after
 * a void {@code save()} and the upsert by key counted {@code updated} the same way, so a write a before-event listener vetoed was
 * reported as applied. A vetoed row is now counted as ignored: refused on purpose, neither applied nor failed. A vetoed update of a
 * matching record must not fall back to inserting a second one for the same key either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9974GrpcInsertVetoCountTest extends BaseGrpcServerTest {
  private static final String TYPE = "VetoedRows";

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions,
          final Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
          @Override
          public void start(final Listener<RespT> responseListener, final Metadata headers) {
            headers.put(USER_HEADER, "root");
            headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
            headers.put(DATABASE_HEADER, getDatabaseName());
            super.start(responseListener, headers);
          }
        };
      }
    }));

    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".pid STRING");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (pid) UNIQUE");
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aVetoedInsertIsCountedAsIgnored() {
    getServerDatabase(0, getDatabaseName()).getSchema().getType(TYPE).getEvents()
        .registerListener((BeforeRecordCreateListener) record -> !"refused".equals(record.asDocument().getString("pid")));

    final InsertSummary summary = stub.bulkInsert(request(InsertOptions.ConflictMode.CONFLICT_ERROR, row("k1", "a"), row("refused", "b"),
        row("k2", "c")));

    assertThat(summary.getReceived()).isEqualTo(3);
    assertThat(summary.getInserted()).isEqualTo(2);
    assertThat(summary.getIgnored()).isEqualTo(1);
    assertThat(summary.getFailed()).isZero();
    assertThat(countRows()).isEqualTo(2L);
  }

  @Test
  void aVetoedUpsertIsCountedAsIgnoredAndInsertsNothing() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      database.newDocument(TYPE).set("pid", "k1").set("v", "old").save();
      database.newDocument(TYPE).set("pid", "k2").set("v", "old").save();
    });
    database.getSchema().getType(TYPE).getEvents()
        .registerListener((BeforeRecordUpdateListener) record -> !"k2".equals(record.asDocument().getString("pid")));

    final InsertSummary summary = stub.bulkInsert(request(InsertOptions.ConflictMode.CONFLICT_UPDATE, row("k1", "new"), row("k2", "new")));

    assertThat(summary.getUpdated()).isEqualTo(1);
    assertThat(summary.getIgnored()).isEqualTo(1);
    assertThat(summary.getInserted()).isZero();
    assertThat(summary.getFailed()).isZero();
    assertThat(countRows()).isEqualTo(2L);
  }

  private BulkInsertRequest request(final InsertOptions.ConflictMode mode, final GrpcRecord... rows) {
    return BulkInsertRequest.newBuilder().setOptions(InsertOptions.newBuilder().setDatabase(getDatabaseName())
            .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
            .setTargetClass(TYPE).setConflictMode(mode).addAllKeyColumns(List.of("pid")).addAllUpdateColumnsOnConflict(List.of("v"))
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH).build())
        .addAllRows(List.of(rows)).build();
  }

  private static GrpcRecord row(final String pid, final String v) {
    return GrpcRecord.newBuilder().setType(TYPE).putProperties("pid", GrpcValue.newBuilder().setStringValue(pid).build())
        .putProperties("v", GrpcValue.newBuilder().setStringValue(v).build()).build();
  }

  private long countRows() {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT count() AS c FROM " + TYPE)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
