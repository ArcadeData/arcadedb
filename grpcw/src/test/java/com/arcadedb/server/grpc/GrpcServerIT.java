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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.BaseGraphServerTest;
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

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GrpcServerIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> AUTHORIZATION_HEADER =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub blockingStub;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  public void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT)
        .usePlaintext()
        .build();
    blockingStub = ArcadeDbServiceGrpc.newBlockingStub(channel);

    // Create an authenticated channel using a client interceptor
    Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
  }

  /**
   * Client interceptor that adds authentication headers to every request
   */
  private class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          headers.put(USER_HEADER, "root");
          headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
          headers.put(DATABASE_HEADER, getDatabaseName());
          super.start(responseListener, headers);
        }
      };
    }
  }

  /**
   * Client interceptor that uses Bearer token authentication
   */
  private class TokenAuthClientInterceptor implements ClientInterceptor {
    private final String token;

    TokenAuthClientInterceptor(final String token) {
      this.token = token;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          headers.put(AUTHORIZATION_HEADER, "Bearer " + token);
          headers.put(DATABASE_HEADER, getDatabaseName());
          super.start(responseListener, headers);
        }
      };
    }
  }

  /**
   * Helper to login via HTTP and get a token
   */
  private String loginAndGetToken(final String username, final String password) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI("http://localhost:2480/api/v1/login").toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8)));
    conn.setDoOutput(true);

    final int responseCode = conn.getResponseCode();
    if (responseCode != 200)
      throw new RuntimeException("Login failed with status: " + responseCode);

    final String response = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    final JSONObject jsonResponse = new JSONObject(response);
    return jsonResponse.getString("token");
  }

  /**
   * Helper to logout via HTTP to invalidate a token
   */
  private void logout(final String token) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI("http://localhost:2480/api/v1/logout").toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization", "Bearer " + token);
    conn.getResponseCode(); // Execute request
  }

  @AfterEach
  public void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

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

  @Test
  void executeQuerySelectsExistingData() {
    ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 WHERE id = 0")
        .build();

    ExecuteQueryResponse response = authenticatedStub.executeQuery(request);

    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).isNotEmpty();

    GrpcRecord record = response.getResultsList().get(0).getRecordsList().get(0);
    assertThat(record.getPropertiesMap()).containsKey("name");
    assertThat(record.getPropertiesMap().get("name").getStringValue()).isEqualTo("V1");
  }

  @Test
  void executeQueryWithParametersWorks() {
    ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 WHERE id = :id")
        .putParameters("id", longValue(0))
        .build();

    ExecuteQueryResponse response = authenticatedStub.executeQuery(request);

    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).isNotEmpty();
  }

  @Test
  void executeQueryReturnsEmptyForNoMatches() {
    ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 WHERE id = 99999")
        .build();

    ExecuteQueryResponse response = authenticatedStub.executeQuery(request);

    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).isEmpty();
  }

  @Test
  void executeQueryWithoutCredentialsFails() {
    ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setQuery("SELECT FROM V1")
        .build();

    // Using blockingStub without authentication headers
    assertThatThrownBy(() -> blockingStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }

  @Test
  void executeCommandInsertDocument() {
    ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INSERT INTO Person SET name = 'John Doe', age = 30")
        .setReturnRows(true)
        .build();

    ExecuteCommandResponse response = authenticatedStub.executeCommand(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getAffectedRecords()).isEqualTo(1);
  }

  @Test
  void executeCommandWithParameters() {
    ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INSERT INTO Person SET name = :name, age = :age")
        .putParameters("name", stringValue("Jane Doe"))
        .putParameters("age", intValue(25))
        .setReturnRows(true)
        .build();

    ExecuteCommandResponse response = authenticatedStub.executeCommand(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getAffectedRecords()).isEqualTo(1);
  }

  @Test
  void executeCommandDdlCreateType() {
    String typeName = "GrpcTestType_" + System.currentTimeMillis();

    ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE DOCUMENT TYPE " + typeName)
        .build();

    ExecuteCommandResponse response = authenticatedStub.executeCommand(request);

    assertThat(response.getSuccess()).isTrue();

    // Verify type was created by querying schema
    ExecuteQueryRequest queryRequest = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM schema:types WHERE name = '" + typeName + "'")
        .build();

    ExecuteQueryResponse queryResponse = authenticatedStub.executeQuery(queryRequest);
    assertThat(queryResponse.getResultsList().get(0).getRecordsList()).isNotEmpty();
  }

  @Test
  void executeCommandInvalidSqlReturnsError() {
    ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INVALID SQL SYNTAX HERE")
        .build();

    ExecuteCommandResponse response = authenticatedStub.executeCommand(request);

    assertThat(response.getSuccess()).isFalse();
    assertThat(response.getMessage()).isNotEmpty();
  }

  // CRUD operation tests

  @Test
  void createRecordAndLookupByRid() {
    // Create a record
    GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("Test Person"))
        .putProperties("age", intValue(40))
        .build();

    CreateRecordRequest createRequest = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .setRecord(record)
        .build();

    CreateRecordResponse createResponse = authenticatedStub.createRecord(createRequest);

    assertThat(createResponse.getRid()).isNotEmpty();
    assertThat(createResponse.getRid()).startsWith("#");

    // Lookup by RID
    LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(createResponse.getRid())
        .build();

    LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(lookupRequest);

    assertThat(lookupResponse.getFound()).isTrue();
    assertThat(lookupResponse.getRecord().getPropertiesMap().get("name").getStringValue())
        .isEqualTo("Test Person");
  }

  @Test
  void lookupByRidNotFoundThrowsException() {
    // First create a record to get a valid bucket ID
    GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("Temp Record"))
        .build();

    CreateRecordRequest createRequest = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .setRecord(record)
        .build();

    String rid = authenticatedStub.createRecord(createRequest).getRid();
    // Extract bucket ID from the created RID (e.g., #26:0 -> 26)
    String bucketId = rid.substring(1, rid.indexOf(':'));

    // Use the same bucket with a very high position that doesn't exist
    LookupByRidRequest request = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid("#" + bucketId + ":999999")
        .build();

    // The service throws an exception when a record is not found
    assertThatThrownBy(() -> authenticatedStub.lookupByRid(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("not found");
  }

  @Test
  void updateRecordModifiesData() {
    // Create a record first
    GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("Original Name"))
        .putProperties("age", intValue(20))
        .build();

    CreateRecordRequest createRequest = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .setRecord(record)
        .build();

    String rid = authenticatedStub.createRecord(createRequest).getRid();

    // Update the record
    GrpcRecord updatedRecord = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("Updated Name"))
        .putProperties("age", intValue(21))
        .build();

    UpdateRecordRequest updateRequest = UpdateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .setRecord(updatedRecord)
        .build();

    UpdateRecordResponse updateResponse = authenticatedStub.updateRecord(updateRequest);

    assertThat(updateResponse.getSuccess()).isTrue();
    assertThat(updateResponse.getUpdated()).isTrue();

    // Verify the update
    LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .build();

    LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(lookupRequest);

    assertThat(lookupResponse.getRecord().getPropertiesMap().get("name").getStringValue())
        .isEqualTo("Updated Name");
  }

  @Test
  void deleteRecordRemovesData() {
    // Create a record first
    GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("To Be Deleted"))
        .build();

    CreateRecordRequest createRequest = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .setRecord(record)
        .build();

    String rid = authenticatedStub.createRecord(createRequest).getRid();

    // Delete the record
    DeleteRecordRequest deleteRequest = DeleteRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .build();

    DeleteRecordResponse deleteResponse = authenticatedStub.deleteRecord(deleteRequest);

    assertThat(deleteResponse.getSuccess()).isTrue();
    assertThat(deleteResponse.getDeleted()).isTrue();

    // Verify it's gone - lookup of deleted record throws an exception
    LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .build();

    assertThatThrownBy(() -> authenticatedStub.lookupByRid(lookupRequest))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("not found");
  }

  @Test
  void deleteNonExistentRecordThrowsException() {
    // First create a record to get a valid bucket ID
    GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("Temp For Bucket"))
        .build();

    CreateRecordRequest createRequest = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .setRecord(record)
        .build();

    String rid = authenticatedStub.createRecord(createRequest).getRid();
    // Extract bucket ID from the created RID (e.g., #26:0 -> 26)
    String bucketId = rid.substring(1, rid.indexOf(':'));

    // Use the same bucket with a very high position that doesn't exist
    DeleteRecordRequest request = DeleteRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid("#" + bucketId + ":999999")
        .build();

    // The service throws an exception when trying to delete a non-existent record
    assertThatThrownBy(() -> authenticatedStub.deleteRecord(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("not found");
  }

  // Transaction tests

  @Test
  void beginAndCommitTransaction() {
    // Begin transaction
    BeginTransactionRequest beginRequest = BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .build();

    BeginTransactionResponse beginResponse = authenticatedStub.beginTransaction(beginRequest);

    assertThat(beginResponse.getTransactionId()).isNotEmpty();

    String txId = beginResponse.getTransactionId();

    // Insert within transaction
    ExecuteCommandRequest insertRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INSERT INTO Person SET name = 'Transaction Test'")
        .setTransaction(TransactionContext.newBuilder()
            .setTransactionId(txId)
            .build())
        .build();

    authenticatedStub.executeCommand(insertRequest);

    // Commit transaction
    CommitTransactionRequest commitRequest = CommitTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder()
            .setTransactionId(txId)
            .setDatabase(getDatabaseName())
            .build())
        .build();

    CommitTransactionResponse commitResponse = authenticatedStub.commitTransaction(commitRequest);

    assertThat(commitResponse.getSuccess()).isTrue();
    assertThat(commitResponse.getCommitted()).isTrue();

    // Verify data is visible after commit
    ExecuteQueryRequest queryRequest = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE name = 'Transaction Test'")
        .build();

    ExecuteQueryResponse queryResponse = authenticatedStub.executeQuery(queryRequest);

    assertThat(queryResponse.getResultsList().get(0).getRecordsList()).isNotEmpty();
  }

  @Test
  void beginAndRollbackTransaction() {
    // Begin transaction
    BeginTransactionRequest beginRequest = BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .build();

    BeginTransactionResponse beginResponse = authenticatedStub.beginTransaction(beginRequest);

    String txId = beginResponse.getTransactionId();

    // Insert within transaction
    ExecuteCommandRequest insertRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INSERT INTO Person SET name = 'Rollback Test " + txId + "'")
        .setTransaction(TransactionContext.newBuilder()
            .setTransactionId(txId)
            .build())
        .build();

    authenticatedStub.executeCommand(insertRequest);

    // Rollback transaction
    RollbackTransactionRequest rollbackRequest = RollbackTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder()
            .setTransactionId(txId)
            .setDatabase(getDatabaseName())
            .build())
        .build();

    RollbackTransactionResponse rollbackResponse = authenticatedStub.rollbackTransaction(rollbackRequest);

    assertThat(rollbackResponse.getSuccess()).isTrue();
    assertThat(rollbackResponse.getRolledBack()).isTrue();

    // Verify data is NOT visible after rollback
    ExecuteQueryRequest queryRequest = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE name = 'Rollback Test " + txId + "'")
        .build();

    ExecuteQueryResponse queryResponse = authenticatedStub.executeQuery(queryRequest);

    assertThat(queryResponse.getResultsList().get(0).getRecordsList()).isEmpty();
  }

  // Streaming query tests

  @Test
  void streamQueryReturnsResults() {
    StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1")
        .setBatchSize(10)
        .build();

    Iterator<QueryResult> results = authenticatedStub.streamQuery(request);

    List<GrpcRecord> allRecords = new ArrayList<>();
    while (results.hasNext()) {
      QueryResult batch = results.next();
      allRecords.addAll(batch.getRecordsList());
    }

    assertThat(allRecords).isNotEmpty();
  }

  @Test
  void streamQueryWithSmallBatchSize() {
    // First insert multiple records
    for (int i = 0; i < 5; i++) {
      ExecuteCommandRequest insertRequest = ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setCommand("INSERT INTO Person SET name = 'Stream Test " + i + "', batchTest = true")
          .build();
      authenticatedStub.executeCommand(insertRequest);
    }

    StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE batchTest = true")
        .setBatchSize(2)
        .build();

    Iterator<QueryResult> results = authenticatedStub.streamQuery(request);

    int batchCount = 0;
    int totalRecords = 0;
    while (results.hasNext()) {
      QueryResult batch = results.next();
      batchCount++;
      totalRecords += batch.getRecordsList().size();
    }

    assertThat(totalRecords).isGreaterThanOrEqualTo(5);
    // With batch size 2 and 5+ records, we should have at least 3 batches
    assertThat(batchCount).isGreaterThanOrEqualTo(3);
  }

  // Bulk insert tests

  @Test
  void bulkInsertMultipleRecords() {
    String typeName = "BulkTestType_" + System.currentTimeMillis();

    // Create the type first
    ExecuteCommandRequest createTypeRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE DOCUMENT TYPE " + typeName)
        .build();
    authenticatedStub.executeCommand(createTypeRequest);

    // Prepare records
    List<GrpcRecord> records = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      records.add(GrpcRecord.newBuilder()
          .setType(typeName)
          .putProperties("index", intValue(i))
          .putProperties("name", stringValue("Bulk Record " + i))
          .build());
    }

    BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .addAllRows(records)
        .build();

    InsertSummary response = authenticatedStub.bulkInsert(request);

    assertThat(response.getReceived()).isEqualTo(10);
    assertThat(response.getInserted()).isEqualTo(10);
    assertThat(response.getFailed()).isEqualTo(0);
  }

  @Test
  void bulkInsertWithConflictIgnore() {
    String typeName = "BulkConflictType_" + System.currentTimeMillis();

    // Create the type with unique index
    ExecuteCommandRequest createTypeRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE DOCUMENT TYPE " + typeName)
        .build();
    authenticatedStub.executeCommand(createTypeRequest);

    ExecuteCommandRequest createIndexRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE PROPERTY " + typeName + ".uniqueKey STRING")
        .build();
    authenticatedStub.executeCommand(createIndexRequest);

    ExecuteCommandRequest createUniqueIndexRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE INDEX ON " + typeName + "(uniqueKey) UNIQUE")
        .build();
    authenticatedStub.executeCommand(createUniqueIndexRequest);

    // Insert records with duplicates
    List<GrpcRecord> records = new ArrayList<>();
    records.add(GrpcRecord.newBuilder()
        .setType(typeName)
        .putProperties("uniqueKey", stringValue("key1"))
        .build());
    records.add(GrpcRecord.newBuilder()
        .setType(typeName)
        .putProperties("uniqueKey", stringValue("key1")) // duplicate
        .build());
    records.add(GrpcRecord.newBuilder()
        .setType(typeName)
        .putProperties("uniqueKey", stringValue("key2"))
        .build());

    BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass(typeName)
            .addKeyColumns("uniqueKey")
            .setConflictMode(InsertOptions.ConflictMode.CONFLICT_IGNORE)
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .addAllRows(records)
        .build();

    InsertSummary response = authenticatedStub.bulkInsert(request);

    assertThat(response.getReceived()).isEqualTo(3);
    assertThat(response.getInserted()).isEqualTo(2); // key1 and key2
    assertThat(response.getIgnored()).isEqualTo(1);  // duplicate key1
  }

  // Token authentication tests

  @Test
  void authenticateWithHttpTokenOverGrpc() throws Exception {
    // 1. Login via HTTP to get token
    final String token = loginAndGetToken("root", DEFAULT_PASSWORD_FOR_TESTS);
    assertThat(token).isNotNull();
    assertThat(token).startsWith("AU-");

    // 2. Create gRPC stub with token auth
    final Channel tokenChannel = ClientInterceptors.intercept(channel, new TokenAuthClientInterceptor(token));
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub tokenStub = ArcadeDbServiceGrpc.newBlockingStub(tokenChannel);

    // 3. Execute a query using token auth
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setQuery("SELECT 1 as test")
        .build();

    final ExecuteQueryResponse response = tokenStub.executeQuery(request);
    assertThat(response.getResultsList()).isNotEmpty();
  }

  @Test
  void tokenRejectedAfterLogout() throws Exception {
    // 1. Login and get token
    final String token = loginAndGetToken("root", DEFAULT_PASSWORD_FOR_TESTS);

    // 2. Logout to invalidate token
    logout(token);

    // 3. Try to use invalidated token
    final Channel tokenChannel = ClientInterceptors.intercept(channel, new TokenAuthClientInterceptor(token));
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub tokenStub = ArcadeDbServiceGrpc.newBlockingStub(tokenChannel);

    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setQuery("SELECT 1 as test")
        .build();

    // Should fail with UNAUTHENTICATED
    assertThatThrownBy(() -> tokenStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }

  @Test
  void invalidTokenRejected() {
    // Use a fake token that doesn't exist
    final Channel tokenChannel = ClientInterceptors.intercept(channel, new TokenAuthClientInterceptor("AU-fake-token-12345"));
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub tokenStub = ArcadeDbServiceGrpc.newBlockingStub(tokenChannel);

    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setQuery("SELECT 1 as test")
        .build();

    // Should fail with UNAUTHENTICATED
    assertThatThrownBy(() -> tokenStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }
}
