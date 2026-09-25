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
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordCreateListener;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8363, the gRPC half: on an HA node, {@code RaftReplicatedDatabase} refuses a CLIENT request on a database
 * whose directory is being replaced from the leader's snapshot, and it tells a client from the engine by
 * {@link ProtocolContext}. {@code LookupByRid}, {@code CreateRecord} and the work of a client-managed transaction
 * never set that tag - query metrics were its only reader and none of them runs a query - so on the node being
 * replaced they read as engine work and were served from the copy the cluster was discarding.
 * <p>
 * Observed where the database sees the request: record listeners fire on the thread that does the read or the
 * write, so the tag they see is the tag the gate sees.
 */
class Issue8363GrpcRequestsAreTaggedAsClientsIT extends BaseGrpcServerTest {

  private final List<String> tagsSeenOnRead   = new CopyOnWriteArrayList<>();
  private final List<String> tagsSeenOnCreate = new CopyOnWriteArrayList<>();

  private final AfterRecordReadListener    readListener   = record -> {
    tagsSeenOnRead.add(ProtocolContext.get());
    return record;
  };
  private final BeforeRecordCreateListener createListener = record -> {
    tagsSeenOnCreate.add(ProtocolContext.get());
    return true;
  };

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setUpClientAndListeners() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel,
        new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName())));
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getEvents().registerListener(readListener);
    database.getEvents().registerListener(createListener);
  }

  @AfterEach
  void tearDownClientAndListeners() throws InterruptedException {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getEvents().unregisterListener(readListener);
    database.getEvents().unregisterListener(createListener);
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private GrpcRecord vertex(final String name) {
    return GrpcRecord.newBuilder().setType(VERTEX1_TYPE_NAME)
        .putProperties("name", GrpcValue.newBuilder().setStringValue(name).build()).build();
  }

  @Test
  void createRecordAndLookupByRidOutsideATransactionRunAsAClient() {
    final String rid = stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setType(VERTEX1_TYPE_NAME).setRecord(vertex("auto")).build()).getRid();

    stub.lookupByRid(LookupByRidRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setRid(rid).build());

    assertThat(tagsSeenOnCreate).as("CreateRecord").isNotEmpty().containsOnly("grpc");
    assertThat(tagsSeenOnRead).as("LookupByRid").isNotEmpty().containsOnly("grpc");
  }

  @Test
  void theWorkOfAClientManagedTransactionRunsAsAClient() {
    final String txId = stub.beginTransaction(BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).build()).getTransactionId();
    final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId)
        .setDatabase(getDatabaseName()).build();

    final String rid = stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setType(VERTEX1_TYPE_NAME).setRecord(vertex("in-tx")).setTransaction(tx)
        .build()).getRid();
    stub.commitTransaction(CommitTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials())
        .build());

    final String txId2 = stub.beginTransaction(BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).build()).getTransactionId();
    final TransactionContext tx2 = TransactionContext.newBuilder().setTransactionId(txId2)
        .setDatabase(getDatabaseName()).build();
    stub.lookupByRid(LookupByRidRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setRid(rid).setTransaction(tx2).build());
    stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setTransaction(tx2).setCredentials(credentials())
        .build());

    assertThat(tagsSeenOnCreate).as("CreateRecord inside a transaction").isNotEmpty().containsOnly("grpc");
    assertThat(tagsSeenOnRead).as("LookupByRid inside a transaction").isNotEmpty().containsOnly("grpc");
  }
}
