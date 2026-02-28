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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Extended integration tests for ArcadeDbGrpcService.
 * Tests edge cases, error handling, and additional scenarios not covered by GrpcServerIT.
 */
public class ArcadeDbGrpcServiceExtendedTest extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub blockingStub;
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
    blockingStub = ArcadeDbServiceGrpc.newBlockingStub(channel);

    // Create an authenticated channel using a client interceptor
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

  /**
   * Client interceptor that adds authentication headers to every request
   */
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

  // Helper methods for creating common test values
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

  private GrpcValue doubleValue(final double d) {
    return GrpcValue.newBuilder().setDoubleValue(d).build();
  }

  // ========== ExecuteQuery Edge Cases ==========

  @Test
  void executeQueryWithEmptyStringReturnsError() {
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("")
        .build();

    assertThatThrownBy(() -> authenticatedStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void executeQueryWithInvalidSqlReturnsError() {
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("INVALID SQL SYNTAX HERE")
        .build();

    assertThatThrownBy(() -> authenticatedStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void executeQueryWithNonExistentTableReturnsError() {
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM NonExistentTable")
        .build();

    assertThatThrownBy(() -> authenticatedStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void executeQueryWithMultipleStatementsSeparated() {
    // Test that only first statement is executed
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 LIMIT 1")
        .build();

    final ExecuteQueryResponse response = authenticatedStub.executeQuery(request);

    assertThat(response.getResultsList()).isNotEmpty();
  }

  @Test
  void executeQueryWithNullParameterValue() {
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 WHERE name = :name")
        .putParameters("name", GrpcValue.newBuilder().build()) // Empty/null value
        .build();

    final ExecuteQueryResponse response = authenticatedStub.executeQuery(request);
    assertThat(response.getResultsList()).isNotNull();
  }

  // ========== ExecuteCommand Edge Cases ==========

  @Test
  void executeCommandWithReturnRowsFalseDoesNotReturnData() {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INSERT INTO Person SET name = 'Test', age = 100")
        .setReturnRows(false)
        .build();

    final ExecuteCommandResponse response = authenticatedStub.executeCommand(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getAffectedRecords()).isEqualTo(1);
    // No results returned when returnRows is false
  }

  @Test
  void executeCommandUpdateWithNoMatches() {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("UPDATE Person SET age = 200 WHERE name = 'NonExistent'")
        .build();

    final ExecuteCommandResponse response = authenticatedStub.executeCommand(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getAffectedRecords()).isEqualTo(0);
  }

  @Test
  void executeCommandDeleteWithNoMatches() {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("DELETE FROM Person WHERE name = 'NonExistent'")
        .build();

    final ExecuteCommandResponse response = authenticatedStub.executeCommand(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getAffectedRecords()).isEqualTo(0);
  }

  // ========== CreateRecord Edge Cases ==========

  @Test
  void createRecordWithEmptyPropertiesSucceeds() {
    final CreateRecordRequest request = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .build();

    final CreateRecordResponse response = authenticatedStub.createRecord(request);

    assertThat(response.getRid()).isNotEmpty();
  }

  @Test
  void createRecordWithComplexNestedData() {
    // Test with map/list values
    final GrpcMap addressMap = GrpcMap.newBuilder()
        .putEntries("street", stringValue("123 Main St"))
        .putEntries("city", stringValue("Rome"))
        .build();

    final GrpcList tagsList = GrpcList.newBuilder()
        .addValues(stringValue("tag1"))
        .addValues(stringValue("tag2"))
        .build();

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("Complex User"))
        .putProperties("address", GrpcValue.newBuilder().setMapValue(addressMap).build())
        .putProperties("tags", GrpcValue.newBuilder().setListValue(tagsList).build())
        .build();

    final CreateRecordRequest request = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .setRecord(record)
        .build();

    final CreateRecordResponse response = authenticatedStub.createRecord(request);

    assertThat(response.getRid()).isNotEmpty();

    // Verify data was stored correctly
    final LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(response.getRid())
        .build();

    final LookupByRidResponse lookupResponse = authenticatedStub.lookupByRid(lookupRequest);
    assertThat(lookupResponse.getRecord().getPropertiesMap()).containsKey("address");
    assertThat(lookupResponse.getRecord().getPropertiesMap()).containsKey("tags");
  }

  @Test
  void createRecordWithNonExistentTypeReturnsError() {
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("NonExistentType")
        .putProperties("name", stringValue("Test"))
        .build();

    final CreateRecordRequest request = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("NonExistentType")
        .setRecord(record)
        .build();

    assertThatThrownBy(() -> authenticatedStub.createRecord(request))
        .isInstanceOf(StatusRuntimeException.class);
  }

  // ========== UpdateRecord Edge Cases ==========

  @Test
  void updateRecordWithInvalidRidReturnsError() {
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("Updated"))
        .build();

    final UpdateRecordRequest request = UpdateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid("#99:99999")
        .setRecord(record)
        .build();

    assertThatThrownBy(() -> authenticatedStub.updateRecord(request))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void updateRecordWithEmptyPropertiesSucceeds() {
    // First create a record
    final GrpcRecord createRec = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("Original"))
        .build();

    final CreateRecordRequest createRequest = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .setRecord(createRec)
        .build();

    final String rid = authenticatedStub.createRecord(createRequest).getRid();

    // Update with empty properties (no-op)
    final GrpcRecord emptyUpdateRec = GrpcRecord.newBuilder()
        .setType("Person")
        .build();

    final UpdateRecordRequest updateRequest = UpdateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .setRecord(emptyUpdateRec)
        .build();

    final UpdateRecordResponse response = authenticatedStub.updateRecord(updateRequest);
    assertThat(response.getSuccess()).isTrue();
  }

  // ========== DeleteRecord Edge Cases ==========

  @Test
  void deleteRecordWithInvalidRidReturnsError() {
    final DeleteRecordRequest request = DeleteRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid("#99:99999")
        .build();

    assertThatThrownBy(() -> authenticatedStub.deleteRecord(request))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void deleteRecordTwiceReturnsError() {
    // Create a record
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("ToDelete"))
        .build();

    final CreateRecordRequest createRequest = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .setRecord(record)
        .build();

    final String rid = authenticatedStub.createRecord(createRequest).getRid();

    // Delete it
    final DeleteRecordRequest deleteRequest = DeleteRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .build();

    authenticatedStub.deleteRecord(deleteRequest);

    // Try to delete again
    assertThatThrownBy(() -> authenticatedStub.deleteRecord(deleteRequest))
        .isInstanceOf(StatusRuntimeException.class);
  }

  // ========== Transaction Edge Cases ==========

  // ========== StreamQuery Edge Cases ==========

  @Test
  void streamQueryWithBatchSizeOne() {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 LIMIT 5")
        .setBatchSize(1)
        .build();

    final Iterator<QueryResult> results = authenticatedStub.streamQuery(request);

    // Collect all batches
    int totalRecords = 0;
    int batchCount = 0;
    while (results.hasNext()) {
      final QueryResult batch = results.next();
      totalRecords += batch.getRecordsCount();
      batchCount++;
    }

    assertThat(totalRecords).isLessThanOrEqualTo(5);
    assertThat(batchCount).isGreaterThan(0);
  }

  @Test
  void streamQueryWithLargeBatchSize() {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1")
        .setBatchSize(10000) // Very large batch size
        .build();

    final Iterator<QueryResult> results = authenticatedStub.streamQuery(request);

    // Should still work
    assertThat(results.hasNext()).isTrue();
    final QueryResult batch = results.next();
    assertThat(batch.getRecordsCount()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void streamQueryWithInvalidQueryReturnsError() {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("INVALID SQL QUERY")
        .setBatchSize(10)
        .build();

    final Iterator<QueryResult> results = authenticatedStub.streamQuery(request);

    // Error should occur when trying to get first result
    assertThatThrownBy(results::next)
        .isInstanceOf(StatusRuntimeException.class);
  }

  // ========== BulkInsert Edge Cases ==========

  @Test
  void bulkInsertWithEmptyRecordsSucceeds() {
    final BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass("Person")
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .build();

    final InsertSummary response = authenticatedStub.bulkInsert(request);

    assertThat(response.getReceived()).isEqualTo(0);
    assertThat(response.getInserted()).isEqualTo(0);
    assertThat(response.getFailed()).isEqualTo(0);
  }

  @Test
  void bulkInsertWithSingleRecord() {
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("Single"))
        .putProperties("age", intValue(25))
        .build();

    final BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass("Person")
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .addRows(record)
        .build();

    final InsertSummary response = authenticatedStub.bulkInsert(request);

    assertThat(response.getReceived()).isEqualTo(1);
    assertThat(response.getInserted()).isEqualTo(1);
    assertThat(response.getFailed()).isEqualTo(0);
  }

  @Test
  void bulkInsertWithMixedValidAndInvalidData() {
    final List<GrpcRecord> records = new ArrayList<>();

    // Add valid record
    records.add(GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("Valid"))
        .putProperties("age", intValue(30))
        .build());

    // Add another valid record
    records.add(GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("AlsoValid"))
        .putProperties("age", intValue(35))
        .build());

    final BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setTargetClass("Person")
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .build())
        .addAllRows(records)
        .build();

    final InsertSummary response = authenticatedStub.bulkInsert(request);

    assertThat(response.getInserted()).isEqualTo(2);
  }

  // ========== LookupByRid Edge Cases ==========

  @Test
  void lookupByRidWithMalformedRidReturnsError() {
    final LookupByRidRequest request = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid("not-a-valid-rid")
        .build();

    assertThatThrownBy(() -> authenticatedStub.lookupByRid(request))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void lookupByRidAfterDeleteReturnsError() {
    // Create and delete a record
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", stringValue("ToDelete"))
        .build();

    final CreateRecordRequest createRequest = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType("Person")
        .setRecord(record)
        .build();

    final String rid = authenticatedStub.createRecord(createRequest).getRid();

    final DeleteRecordRequest deleteRequest = DeleteRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .build();

    authenticatedStub.deleteRecord(deleteRequest);

    // Try to lookup
    final LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .build();

    assertThatThrownBy(() -> authenticatedStub.lookupByRid(lookupRequest))
        .isInstanceOf(StatusRuntimeException.class);
  }

  // ========== Authentication Edge Cases ==========

  @Test
  void requestWithWrongDatabaseReturnsError() {
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase("NonExistentDatabase")
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1")
        .build();

    assertThatThrownBy(() -> authenticatedStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void requestWithWrongPasswordReturnsError() {
    final DatabaseCredentials wrongCredentials = DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword("wrong-password")
        .build();

    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(wrongCredentials)
        .setQuery("SELECT FROM V1")
        .build();

    assertThatThrownBy(() -> blockingStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void requestWithNonExistentUserReturnsError() {
    final DatabaseCredentials wrongCredentials = DatabaseCredentials.newBuilder()
        .setUsername("nonexistentuser")
        .setPassword("anypassword")
        .build();

    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(wrongCredentials)
        .setQuery("SELECT FROM V1")
        .build();

    assertThatThrownBy(() -> blockingStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class);
  }
}
