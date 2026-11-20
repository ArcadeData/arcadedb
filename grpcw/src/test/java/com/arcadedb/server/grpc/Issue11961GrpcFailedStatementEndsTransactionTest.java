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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11961: a statement that fails inside a client-managed gRPC transaction ends the transaction, as over HTTP (#9006),
 * Postgres and Bolt. The transaction used to stay open and a later {@code CommitTransaction} answered {@code committed=true},
 * publishing what the statements before the failure wrote: the same client code committed a partial unit of work over gRPC
 * only. Now the transaction is rolled back, a later statement is refused with {@code FAILED_PRECONDITION}, the commit answers
 * {@code committed=false} and a rollback ends it.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public class Issue11961GrpcFailedStatementEndsTransactionTest extends BaseGrpcServerTest {
  private static final String TYPE = "Tx11961";

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
    final Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
    headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), getDatabaseName());
    stub = ArcadeDbServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

    final DocumentType type = getServerDatabase(0, getDatabaseName()).getSchema().createDocumentType(TYPE);
    type.createProperty("tag", Type.STRING);
    type.createProperty("v", Type.INTEGER);
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /** The steps of the issue: insert, read it back, a failing insert, commit. Nothing of the transaction is published. */
  @Test
  void aFailedCommandRollsBackTheTransactionAndItsCommitPublishesNothing() {
    final TransactionContext tx = begin();

    command("INSERT INTO " + TYPE + " SET tag = 't', v = 1", tx);
    assertThat(stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setQuery("SELECT FROM " + TYPE + " WHERE tag = 't'").setTransaction(tx).build()).getResults(0).getRecordsCount()).isEqualTo(1);

    // the failing call keeps the status of its own failure
    assertThatThrownBy(() -> command("INSERT INTO " + TYPE + " SET tag = 'bad', v = 'notanint'", tx))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

    final CommitTransactionResponse commit = commit(tx);
    assertThat(commit.getCommitted()).isFalse();
    assertThat(commit.getMessage()).contains("rolled back after a failed statement");

    assertThat(count()).isZero();
  }

  /** After the failure the transaction runs nothing more: a statement is refused, and the rollback ends it cleanly. */
  @Test
  void aStatementAfterTheFailureIsRefusedAndTheRollbackEndsTheTransaction() {
    final TransactionContext tx = begin();

    command("INSERT INTO " + TYPE + " SET tag = 't', v = 1", tx);
    assertThatThrownBy(() -> command("INSERT INTO " + TYPE + " SET tag = 'bad', v = 'notanint'", tx))
        .isInstanceOf(StatusRuntimeException.class);

    // run outside the rolled back transaction, the write would land in a transaction of its own that nothing ends
    assertThatThrownBy(() -> command("INSERT INTO " + TYPE + " SET tag = 'after', v = 2", tx))
        .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
          assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
          assertThat(e.getStatus().getDescription()).contains("rolled back after a failed statement");
        });
    assertThatThrownBy(() -> stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setType(TYPE).setRecord(row("after", GrpcValue.newBuilder().setInt32Value(3).build()))
        .setTransaction(tx).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));

    // a client's rollback in its error handling succeeds, and ends the transaction
    assertThat(stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(tx)
        .build()).getRolledBack()).isTrue();
    assertThat(commit(tx).getCommitted()).isFalse();

    assertThat(count()).isZero();
  }

  /** The record calls end the transaction the same way: a failing CreateRecord takes the create before it with it. */
  @Test
  void aFailingCreateRecordEndsTheTransaction() {
    final TransactionContext tx = begin();

    assertThat(stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType(TYPE).setRecord(row("t", GrpcValue.newBuilder().setInt32Value(1).build())).setTransaction(tx).build()).getRid())
        .startsWith("#");

    assertThatThrownBy(() -> stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setType(TYPE).setRecord(row("t", GrpcValue.newBuilder().setStringValue("notanint").build()))
        .setTransaction(tx).build())).isInstanceOf(StatusRuntimeException.class);

    assertThat(commit(tx).getCommitted()).isFalse();
    assertThat(count()).isZero();
  }

  /**
   * A record that does not exist is the answer to a call addressing it by RID, which wrote nothing: the transaction goes on, as
   * the HTTP client's lookup (a query matching nothing) leaves its session open.
   */
  @Test
  void aLookupOfAMissingRecordLeavesTheTransactionOpen() {
    final TransactionContext tx = begin();

    final String rid = stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType(TYPE).setRecord(row("t", GrpcValue.newBuilder().setInt32Value(1).build())).setTransaction(tx).build()).getRid();
    final String missing = rid.substring(0, rid.indexOf(':') + 1) + "999999";

    assertThatThrownBy(() -> stub.lookupByRid(LookupByRidRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setRid(missing).setTransaction(tx).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    assertThatThrownBy(() -> stub.deleteRecord(DeleteRecordRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setRid(missing).setTransaction(tx).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));

    assertThat(commit(tx).getCommitted()).isTrue();
    assertThat(count()).isEqualTo(1L);
  }

  // --- helpers ---

  private TransactionContext begin() {
    final String txId = stub.beginTransaction(BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).build()).getTransactionId();
    return TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();
  }

  private ExecuteCommandResponse command(final String sql, final TransactionContext tx) {
    return stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setLanguage("sql").setCommand(sql).setTransaction(tx).build());
  }

  private CommitTransactionResponse commit(final TransactionContext tx) {
    return stub.commitTransaction(CommitTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(tx).build());
  }

  private static GrpcRecord row(final String tag, final GrpcValue v) {
    return GrpcRecord.newBuilder().setType(TYPE).putProperties("tag", GrpcValue.newBuilder().setStringValue(tag).build())
        .putProperties("v", v).build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private long count() {
    final Database database = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
