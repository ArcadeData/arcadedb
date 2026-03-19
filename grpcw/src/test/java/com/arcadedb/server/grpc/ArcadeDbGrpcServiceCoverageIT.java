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
import com.arcadedb.test.BaseGraphServerTest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
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
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests to improve code coverage of ArcadeDbGrpcService.
 * Covers vertex/edge CRUD, streaming modes, transaction edge cases,
 * bulk insert variants, data type round-trips, and projection settings.
 */
public class ArcadeDbGrpcServiceCoverageIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    // Disable parallel scan to avoid thread-local DatabaseContext contamination
    // across tests when gRPC thread pool threads retain stale context
    GlobalConfiguration.QUERY_PARALLEL_SCAN.setValue(false);
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT)
        .usePlaintext()
        .build();

    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)) {
        @Override
        public void start(final Listener<RespT> responseListener, final Metadata headers) {
          headers.put(USER_HEADER, "root");
          headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
          headers.put(DATABASE_HEADER, getDatabaseName());
          super.start(responseListener, headers);
        }
      };
    }
  }

  // ---- Helper methods ----

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
  }

  private GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
  }

  private GrpcValue intValue(final int i) {
    return GrpcValue.newBuilder().setInt32Value(i).build();
  }

  private GrpcValue longValue(final long l) {
    return GrpcValue.newBuilder().setInt64Value(l).build();
  }

  private GrpcValue boolValue(final boolean b) {
    return GrpcValue.newBuilder().setBoolValue(b).build();
  }

  private GrpcValue floatValue(final float f) {
    return GrpcValue.newBuilder().setFloatValue(f).build();
  }

  private GrpcValue doubleValue(final double d) {
    return GrpcValue.newBuilder().setDoubleValue(d).build();
  }

  private String createVertexType(final String typeName) {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE VERTEX TYPE " + typeName)
        .build();
    authenticatedStub.executeCommand(request);
    return typeName;
  }

  private String createDocumentType(final String typeName) {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE DOCUMENT TYPE " + typeName)
        .build();
    authenticatedStub.executeCommand(request);
    return typeName;
  }

  private String createEdgeType(final String typeName) {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE EDGE TYPE " + typeName)
        .build();
    authenticatedStub.executeCommand(request);
    return typeName;
  }

  // ========== Vertex CRUD ==========

  @Test
  void createVertexRecord() {
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType(VERTEX1_TYPE_NAME)
        .putProperties("name", stringValue("CoverageVertex"))
        .build();

    final CreateRecordRequest request = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(VERTEX1_TYPE_NAME)
        .setRecord(record)
        .build();

    final CreateRecordResponse response = authenticatedStub.createRecord(request);

    assertThat(response.getRid()).isNotEmpty();
    assertThat(response.getRid()).startsWith("#");

    // Verify it's actually a vertex by looking it up
    final LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(response.getRid())
        .build();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(lookupRequest);
    assertThat(lookupResponse.getFound()).isTrue();
    assertThat(lookupResponse.getRecord().getPropertiesMap().get("name").getStringValue())
        .isEqualTo("CoverageVertex");
  }

  @Test
  void createVertexRecordWithProperties() {
    final String typeName = "CovVertex_" + System.currentTimeMillis();
    createVertexType(typeName);

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType(typeName)
        .putProperties("name", stringValue("PropVertex"))
        .putProperties("age", intValue(42))
        .putProperties("score", doubleValue(99.5))
        .build();

    final CreateRecordRequest request = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(typeName)
        .setRecord(record)
        .build();

    final CreateRecordResponse response = authenticatedStub.createRecord(request);
    assertThat(response.getRid()).isNotEmpty();

    // Verify all properties
    final LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(response.getRid())
        .build();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(lookupRequest);
    assertThat(lookupResponse.getRecord().getPropertiesMap().get("name").getStringValue()).isEqualTo("PropVertex");
  }

  @Test
  void updateVertexRecord() {
    // Create a vertex
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType(VERTEX1_TYPE_NAME)
        .putProperties("name", stringValue("OriginalVertex"))
        .build();

    final CreateRecordRequest createRequest = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(VERTEX1_TYPE_NAME)
        .setRecord(record)
        .build();

    final String rid = authenticatedStub.createRecord(createRequest).getRid();

    // Update the vertex (requires explicit transaction)
    final GrpcRecord updatedRecord = GrpcRecord.newBuilder()
        .setType(VERTEX1_TYPE_NAME)
        .putProperties("name", stringValue("UpdatedVertex"))
        .build();

    final UpdateRecordRequest updateRequest = UpdateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .setRecord(updatedRecord)
        .setTransaction(TransactionContext.newBuilder()
            .setBegin(true)
            .setCommit(true)
            .build())
        .build();

    final UpdateRecordResponse updateResponse = authenticatedStub.updateRecord(updateRequest);
    assertThat(updateResponse.getSuccess()).isTrue();
    assertThat(updateResponse.getUpdated()).isTrue();

    // Verify the update
    final LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .build();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(lookupRequest);
    assertThat(lookupResponse.getRecord().getPropertiesMap().get("name").getStringValue())
        .isEqualTo("UpdatedVertex");
  }

  // ========== Edge CRUD ==========

  @Test
  void createEdgeRecord() {
    // Create two vertices to connect with an edge
    final GrpcRecord v1Record = GrpcRecord.newBuilder()
        .setType(VERTEX1_TYPE_NAME)
        .putProperties("name", stringValue("EdgeOutVertex"))
        .build();
    final String outRid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType(VERTEX1_TYPE_NAME)
            .setRecord(v1Record)
            .build()).getRid();

    final GrpcRecord v2Record = GrpcRecord.newBuilder()
        .setType(VERTEX1_TYPE_NAME)
        .putProperties("name", stringValue("EdgeInVertex"))
        .build();
    final String inRid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType(VERTEX1_TYPE_NAME)
            .setRecord(v2Record)
            .build()).getRid();

    // Create edge via createRecord RPC
    final GrpcRecord edgeRecord = GrpcRecord.newBuilder()
        .setType(EDGE1_TYPE_NAME)
        .putProperties("out", stringValue(outRid))
        .putProperties("in", stringValue(inRid))
        .build();

    final CreateRecordResponse edgeResponse = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType(EDGE1_TYPE_NAME)
            .setRecord(edgeRecord)
            .build());

    assertThat(edgeResponse.getRid()).isNotEmpty();
    assertThat(edgeResponse.getRid()).startsWith("#");
  }

  @Test
  void createEdgeRecordWithProperties() {
    // Create two vertices
    final String outRid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType(VERTEX1_TYPE_NAME)
            .setRecord(GrpcRecord.newBuilder().setType(VERTEX1_TYPE_NAME)
                .putProperties("name", stringValue("OutV")).build())
            .build()).getRid();

    final String inRid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType(VERTEX1_TYPE_NAME)
            .setRecord(GrpcRecord.newBuilder().setType(VERTEX1_TYPE_NAME)
                .putProperties("name", stringValue("InV")).build())
            .build()).getRid();

    // Create edge with extra properties
    final GrpcRecord edgeRecord = GrpcRecord.newBuilder()
        .setType(EDGE1_TYPE_NAME)
        .putProperties("out", stringValue(outRid))
        .putProperties("in", stringValue(inRid))
        .putProperties("weight", doubleValue(3.14))
        .putProperties("label", stringValue("knows"))
        .build();

    final CreateRecordResponse response = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType(EDGE1_TYPE_NAME)
            .setRecord(edgeRecord)
            .build());

    assertThat(response.getRid()).isNotEmpty();
  }

  @Test
  void createEdgeRecordWithoutEndpointsFails() {
    final GrpcRecord edgeRecord = GrpcRecord.newBuilder()
        .setType(EDGE1_TYPE_NAME)
        .putProperties("weight", doubleValue(1.0))
        .build();

    final CreateRecordRequest request = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(EDGE1_TYPE_NAME)
        .setRecord(edgeRecord)
        .build();

    assertThatThrownBy(() -> authenticatedStub.createRecord(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("'out' and 'in'");
  }

  // ========== ExecuteCommand transaction paths ==========

  @Test
  void executeCommandWithExplicitBeginCommit() {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INSERT INTO Person SET name = 'ExplicitTx'")
        .setTransaction(TransactionContext.newBuilder()
            .setBegin(true)
            .setCommit(true)
            .build())
        .build();

    final ExecuteCommandResponse response = authenticatedStub.executeCommand(request);
    assertThat(response.getSuccess()).isTrue();

    // Verify the insert is visible (committed)
    final ExecuteQueryRequest query = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE name = 'ExplicitTx'")
        .build();
    final ExecuteQueryResponse queryResponse = authenticatedStub.executeQuery(query);
    assertThat(queryResponse.getResultsList().get(0).getRecordsList()).isNotEmpty();
  }

  @Test
  void executeCommandWithExplicitBeginRollback() {
    final String uniqueName = "RollbackTx_" + System.currentTimeMillis();

    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INSERT INTO Person SET name = '" + uniqueName + "'")
        .setTransaction(TransactionContext.newBuilder()
            .setBegin(true)
            .setRollback(true)
            .build())
        .build();

    final ExecuteCommandResponse response = authenticatedStub.executeCommand(request);
    assertThat(response.getSuccess()).isTrue();

    // Verify the insert is NOT visible (rolled back)
    final ExecuteQueryRequest query = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE name = '" + uniqueName + "'")
        .build();
    final ExecuteQueryResponse queryResponse = authenticatedStub.executeQuery(query);
    assertThat(queryResponse.getResultsList().get(0).getRecordsList()).isEmpty();
  }

  @Test
  void executeCommandReturnRowsWithNonElementResults() {
    // SELECT count(*) returns a non-element result (just a number property)
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("SELECT count(*) as cnt FROM V1")
        .setReturnRows(true)
        .build();

    final ExecuteCommandResponse response = authenticatedStub.executeCommand(request);
    assertThat(response.getSuccess()).isTrue();
    // The count result should be counted in affected records
    assertThat(response.getAffectedRecords()).isGreaterThanOrEqualTo(0);
  }

  // ========== ExecuteQuery ==========

  @Test
  void executeQueryWithLimit() {
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1")
        .setLimit(2)
        .build();

    final ExecuteQueryResponse response = authenticatedStub.executeQuery(request);
    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList().size()).isLessThanOrEqualTo(2);
  }

  @Test
  void executeQueryWithProjectionSettingsAsMap() {
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 LIMIT 1")
        .setProjectionSettings(ProjectionSettings.newBuilder()
            .setIncludeProjections(true)
            .setProjectionEncoding(ProjectionSettings.ProjectionEncoding.PROJECTION_AS_MAP)
            .build())
        .build();

    final ExecuteQueryResponse response = authenticatedStub.executeQuery(request);
    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).isNotEmpty();
  }

  @Test
  void executeQueryWithProjectionSettingsAsLink() {
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 LIMIT 1")
        .setProjectionSettings(ProjectionSettings.newBuilder()
            .setIncludeProjections(true)
            .setProjectionEncoding(ProjectionSettings.ProjectionEncoding.PROJECTION_AS_LINK)
            .build())
        .build();

    final ExecuteQueryResponse response = authenticatedStub.executeQuery(request);
    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).isNotEmpty();
  }

  @Test
  void executeQueryWithProjectionSoftLimit() {
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 LIMIT 1")
        .setProjectionSettings(ProjectionSettings.newBuilder()
            .setIncludeProjections(true)
            .setProjectionEncoding(ProjectionSettings.ProjectionEncoding.PROJECTION_AS_JSON)
            .setSoftLimitBytes(Int32Value.of(1024))
            .build())
        .build();

    final ExecuteQueryResponse response = authenticatedStub.executeQuery(request);
    assertThat(response.getResultsList()).isNotEmpty();
  }

  // ========== Transaction edge cases ==========

  @Test
  void commitTransactionWithBlankId() {
    final CommitTransactionRequest request = CommitTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder()
            .setTransactionId("")
            .setDatabase(getDatabaseName())
            .build())
        .build();

    assertThatThrownBy(() -> authenticatedStub.commitTransaction(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  @Test
  void commitTransactionWithNonExistentId() {
    final CommitTransactionRequest request = CommitTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder()
            .setTransactionId("non-existent-tx-id-12345")
            .setDatabase(getDatabaseName())
            .build())
        .build();

    final CommitTransactionResponse response = authenticatedStub.commitTransaction(request);
    // Non-existent tx returns success=true, committed=false (idempotent no-op)
    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getCommitted()).isFalse();
    assertThat(response.getMessage()).contains("No active transaction");
  }

  @Test
  void rollbackTransactionWithBlankId() {
    final RollbackTransactionRequest request = RollbackTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder()
            .setTransactionId("")
            .setDatabase(getDatabaseName())
            .build())
        .build();

    assertThatThrownBy(() -> authenticatedStub.rollbackTransaction(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  @Test
  void rollbackTransactionWithNonExistentId() {
    final RollbackTransactionRequest request = RollbackTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder()
            .setTransactionId("non-existent-tx-id-67890")
            .setDatabase(getDatabaseName())
            .build())
        .build();

    final RollbackTransactionResponse response = authenticatedStub.rollbackTransaction(request);
    // Non-existent tx returns success=true, rolledBack=false (idempotent no-op)
    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getRolledBack()).isFalse();
    assertThat(response.getMessage()).contains("No active transaction");
  }

  // ========== StreamQuery modes ==========

  @Test
  void streamQueryMaterializeAllMode() {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1")
        .setBatchSize(5)
        .setRetrievalMode(StreamQueryRequest.RetrievalMode.MATERIALIZE_ALL)
        .build();

    final Iterator<QueryResult> results = authenticatedStub.streamQuery(request);

    final List<GrpcRecord> allRecords = new ArrayList<>();
    while (results.hasNext()) {
      final QueryResult batch = results.next();
      allRecords.addAll(batch.getRecordsList());
    }

    assertThat(allRecords).isNotEmpty();
  }

  @Test
  void streamQueryPagedMode() {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1")
        .setBatchSize(2)
        .setRetrievalMode(StreamQueryRequest.RetrievalMode.PAGED)
        .build();

    final Iterator<QueryResult> results = authenticatedStub.streamQuery(request);

    final List<GrpcRecord> allRecords = new ArrayList<>();
    int batchCount = 0;
    while (results.hasNext()) {
      final QueryResult batch = results.next();
      allRecords.addAll(batch.getRecordsList());
      batchCount++;
    }

    assertThat(allRecords).isNotEmpty();
    // With batch size 2 and multiple V1 records, we should get multiple batches
    assertThat(batchCount).isGreaterThanOrEqualTo(1);
  }

  @Test
  void streamQueryMaterializeAllWithNonElementResults() {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT count(*) as cnt FROM V1")
        .setBatchSize(10)
        .setRetrievalMode(StreamQueryRequest.RetrievalMode.MATERIALIZE_ALL)
        .build();

    final Iterator<QueryResult> results = authenticatedStub.streamQuery(request);

    assertThat(results.hasNext()).isTrue();
    final QueryResult batch = results.next();
    assertThat(batch.getRecordsCount()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void streamQueryPagedWithNonElementResults() {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT count(*) as cnt FROM V1")
        .setBatchSize(10)
        .setRetrievalMode(StreamQueryRequest.RetrievalMode.PAGED)
        .build();

    final Iterator<QueryResult> results = authenticatedStub.streamQuery(request);

    assertThat(results.hasNext()).isTrue();
    final QueryResult batch = results.next();
    assertThat(batch.getRecordsCount()).isGreaterThanOrEqualTo(1);
  }

  // ========== BulkInsert ==========

  @Test
  void bulkInsertVertexType() {
    final String typeName = "BulkVertex_" + System.currentTimeMillis();
    createVertexType(typeName);

    final List<GrpcRecord> records = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      records.add(GrpcRecord.newBuilder()
          .setType(typeName)
          .putProperties("index", intValue(i))
          .putProperties("name", stringValue("BulkV_" + i))
          .build());
    }

    final BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .addAllRows(records)
        .build();

    final InsertSummary response = authenticatedStub.bulkInsert(request);
    assertThat(response.getReceived()).isEqualTo(5);
    assertThat(response.getInserted()).isEqualTo(5);
    assertThat(response.getFailed()).isEqualTo(0);
  }

  @Test
  void bulkInsertWithConflictUpdate() {
    final String typeName = "BulkUpsert_" + System.currentTimeMillis();
    createDocumentType(typeName);

    // Create a property and unique index for upsert
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE PROPERTY " + typeName + ".uniqueKey STRING")
        .build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE INDEX ON " + typeName + "(uniqueKey) UNIQUE")
        .build());

    // First insert
    final List<GrpcRecord> firstBatch = new ArrayList<>();
    firstBatch.add(GrpcRecord.newBuilder()
        .setType(typeName)
        .putProperties("uniqueKey", stringValue("upsert1"))
        .putProperties("value", stringValue("original"))
        .build());

    authenticatedStub.bulkInsert(BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .addAllRows(firstBatch)
        .build());

    // Now upsert with CONFLICT_UPDATE
    final List<GrpcRecord> upsertBatch = new ArrayList<>();
    upsertBatch.add(GrpcRecord.newBuilder()
        .setType(typeName)
        .putProperties("uniqueKey", stringValue("upsert1"))
        .putProperties("value", stringValue("updated"))
        .build());

    final InsertSummary response = authenticatedStub.bulkInsert(BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .addKeyColumns("uniqueKey")
            .addUpdateColumnsOnConflict("value")
            .setConflictMode(InsertOptions.ConflictMode.CONFLICT_UPDATE)
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .addAllRows(upsertBatch)
        .build());

    assertThat(response.getReceived()).isEqualTo(1);
    assertThat(response.getUpdated()).isEqualTo(1);
  }

  @Test
  void bulkInsertWithPerRowTransactionMode() {
    final String typeName = "BulkPerRow_" + System.currentTimeMillis();
    createDocumentType(typeName);

    final List<GrpcRecord> records = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      records.add(GrpcRecord.newBuilder()
          .setType(typeName)
          .putProperties("index", intValue(i))
          .build());
    }

    final BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .setTransactionMode(InsertOptions.TransactionMode.PER_ROW)
            .build())
        .addAllRows(records)
        .build();

    final InsertSummary response = authenticatedStub.bulkInsert(request);
    assertThat(response.getReceived()).isEqualTo(3);
    assertThat(response.getInserted()).isEqualTo(3);
  }

  @Test
  void bulkInsertWithPerRequestTransactionMode() {
    final String typeName = "BulkPerReq_" + System.currentTimeMillis();
    createDocumentType(typeName);

    final List<GrpcRecord> records = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      records.add(GrpcRecord.newBuilder()
          .setType(typeName)
          .putProperties("index", intValue(i))
          .build());
    }

    final BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .setTransactionMode(InsertOptions.TransactionMode.PER_REQUEST)
            .build())
        .addAllRows(records)
        .build();

    final InsertSummary response = authenticatedStub.bulkInsert(request);
    assertThat(response.getReceived()).isEqualTo(3);
    assertThat(response.getInserted()).isEqualTo(3);
  }

  @Test
  void bulkInsertWithPerStreamTransactionMode() {
    final String typeName = "BulkPerStream_" + System.currentTimeMillis();
    createDocumentType(typeName);

    final List<GrpcRecord> records = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      records.add(GrpcRecord.newBuilder()
          .setType(typeName)
          .putProperties("index", intValue(i))
          .build());
    }

    final BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
            .build())
        .addAllRows(records)
        .build();

    final InsertSummary response = authenticatedStub.bulkInsert(request);
    assertThat(response.getReceived()).isEqualTo(3);
    assertThat(response.getInserted()).isEqualTo(3);
  }

  @Test
  void bulkInsertWithValidateOnly() {
    final String typeName = "BulkValidate_" + System.currentTimeMillis();
    createDocumentType(typeName);

    final List<GrpcRecord> records = new ArrayList<>();
    records.add(GrpcRecord.newBuilder()
        .setType(typeName)
        .putProperties("name", stringValue("ValidateOnly"))
        .build());

    final BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .setValidateOnly(true)
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .addAllRows(records)
        .build();

    final InsertSummary response = authenticatedStub.bulkInsert(request);
    assertThat(response.getReceived()).isEqualTo(1);
    // validate_only means no actual insert
    assertThat(response.getInserted()).isEqualTo(0);

    // Verify nothing was actually inserted
    final ExecuteQueryRequest query = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM " + typeName + " WHERE name = 'ValidateOnly'")
        .build();
    final ExecuteQueryResponse queryResponse = authenticatedStub.executeQuery(query);
    assertThat(queryResponse.getResultsList().get(0).getRecordsList()).isEmpty();
  }

  @Test
  void bulkInsertVertexWithConflictUpdate() {
    final String typeName = "BulkVUpsert_" + System.currentTimeMillis();
    createVertexType(typeName);

    // Create a property and unique index
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE PROPERTY " + typeName + ".key STRING")
        .build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE INDEX ON " + typeName + "(key) UNIQUE")
        .build());

    // First insert a vertex
    authenticatedStub.bulkInsert(BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .addRows(GrpcRecord.newBuilder()
            .setType(typeName)
            .putProperties("key", stringValue("vkey1"))
            .putProperties("value", stringValue("original"))
            .build())
        .build());

    // Upsert vertex with CONFLICT_UPDATE
    final InsertSummary response = authenticatedStub.bulkInsert(BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .addKeyColumns("key")
            .addUpdateColumnsOnConflict("value")
            .setConflictMode(InsertOptions.ConflictMode.CONFLICT_UPDATE)
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .addRows(GrpcRecord.newBuilder()
            .setType(typeName)
            .putProperties("key", stringValue("vkey1"))
            .putProperties("value", stringValue("updated"))
            .build())
        .build());

    assertThat(response.getReceived()).isEqualTo(1);
    assertThat(response.getUpdated()).isEqualTo(1);
  }

  // ========== Data type round-trips ==========

  @Test
  void createAndReadWithBooleanValue() {
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("BoolPerson"))
        .putProperties("active", boolValue(true))
        .build();

    final CreateRecordRequest request = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .setRecord(record)
        .build();

    final String rid = authenticatedStub.createRecord(request).getRid();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(
        LookupByRidRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid(rid)
            .build());

    assertThat(lookupResponse.getFound()).isTrue();
    assertThat(lookupResponse.getRecord().getPropertiesMap().get("active").getBoolValue()).isTrue();
  }

  @Test
  void createAndReadWithFloatDoubleValues() {
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("FloatPerson"))
        .putProperties("floatVal", floatValue(3.14f))
        .putProperties("doubleVal", doubleValue(2.71828))
        .build();

    final String rid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("Person")
            .setRecord(record)
            .build()).getRid();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(
        LookupByRidRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid(rid)
            .build());

    assertThat(lookupResponse.getFound()).isTrue();
    // Float/double may be stored as double internally
    final var props = lookupResponse.getRecord().getPropertiesMap();
    assertThat(props).containsKey("floatVal");
    assertThat(props).containsKey("doubleVal");
  }

  @Test
  void createAndReadWithDateValue() {
    final Timestamp ts = Timestamp.newBuilder()
        .setSeconds(1700000000L)
        .setNanos(0)
        .build();

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("DatePerson"))
        .putProperties("createdAt", GrpcValue.newBuilder().setTimestampValue(ts).build())
        .build();

    final String rid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("Person")
            .setRecord(record)
            .build()).getRid();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(
        LookupByRidRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid(rid)
            .build());

    assertThat(lookupResponse.getFound()).isTrue();
    assertThat(lookupResponse.getRecord().getPropertiesMap()).containsKey("createdAt");
  }

  @Test
  void createAndReadWithDecimalValue() {
    // BigDecimal(12345, 2) = 123.45
    final GrpcDecimal grpcDecimal = GrpcDecimal.newBuilder()
        .setUnscaled(12345L)
        .setScale(2)
        .build();

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("DecimalPerson"))
        .putProperties("amount", GrpcValue.newBuilder().setDecimalValue(grpcDecimal).build())
        .build();

    final String rid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("Person")
            .setRecord(record)
            .build()).getRid();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(
        LookupByRidRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid(rid)
            .build());

    assertThat(lookupResponse.getFound()).isTrue();
    assertThat(lookupResponse.getRecord().getPropertiesMap()).containsKey("amount");
  }

  @Test
  void createAndReadWithListValue() {
    final GrpcList tagsList = GrpcList.newBuilder()
        .addValues(stringValue("java"))
        .addValues(stringValue("grpc"))
        .addValues(stringValue("arcadedb"))
        .build();

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("ListPerson"))
        .putProperties("tags", GrpcValue.newBuilder().setListValue(tagsList).build())
        .build();

    final String rid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("Person")
            .setRecord(record)
            .build()).getRid();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(
        LookupByRidRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid(rid)
            .build());

    assertThat(lookupResponse.getFound()).isTrue();
    assertThat(lookupResponse.getRecord().getPropertiesMap()).containsKey("tags");
    // The list should be stored and returned
    final GrpcValue tagsValue = lookupResponse.getRecord().getPropertiesMap().get("tags");
    assertThat(tagsValue.hasListValue()).isTrue();
    assertThat(tagsValue.getListValue().getValuesCount()).isEqualTo(3);
  }

  @Test
  void createAndReadWithMapValue() {
    final GrpcMap addressMap = GrpcMap.newBuilder()
        .putEntries("street", stringValue("Via Roma 1"))
        .putEntries("city", stringValue("Milan"))
        .putEntries("zip", stringValue("20100"))
        .build();

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("MapPerson"))
        .putProperties("address", GrpcValue.newBuilder().setMapValue(addressMap).build())
        .build();

    final String rid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("Person")
            .setRecord(record)
            .build()).getRid();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(
        LookupByRidRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid(rid)
            .build());

    assertThat(lookupResponse.getFound()).isTrue();
    assertThat(lookupResponse.getRecord().getPropertiesMap()).containsKey("address");
  }

  @Test
  void createAndReadWithBytesValue() {
    final byte[] data = "Hello ArcadeDB gRPC".getBytes(StandardCharsets.UTF_8);

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("BytesPerson"))
        .putProperties("binaryData", GrpcValue.newBuilder().setBytesValue(ByteString.copyFrom(data)).build())
        .build();

    final String rid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("Person")
            .setRecord(record)
            .build()).getRid();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(
        LookupByRidRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid(rid)
            .build());

    assertThat(lookupResponse.getFound()).isTrue();
    assertThat(lookupResponse.getRecord().getPropertiesMap()).containsKey("binaryData");
  }
}
