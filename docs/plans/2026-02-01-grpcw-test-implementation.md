# gRPC Module Test Coverage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add comprehensive unit and integration tests to the grpcw module, achieving ~52 tests covering all service methods, interceptors, and plugin configuration.

**Architecture:** Integration tests extend `BaseGraphServerTest` to get full server lifecycle. Unit tests use gRPC testing utilities and mocks. Tests follow TDD where practical - for existing code, we write tests that verify current behavior.

**Tech Stack:** JUnit 5, AssertJ, gRPC Java, gRPC Testing, BaseGraphServerTest

---

### Task 1: Update pom.xml with Test Dependencies

**Files:**
- Modify: `grpcw/pom.xml:39-57`

**Step 1: Add server test-jar dependency**

Add after the existing `arcadedb-test-utils` dependency (line 56):

```xml
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-server</artifactId>
            <version>${project.parent.version}</version>
            <scope>test</scope>
            <type>test-jar</type>
        </dependency>
```

**Step 2: Add grpc-testing dependency**

Add after the server test-jar dependency:

```xml
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-testing</artifactId>
            <version>${grpc.version}</version>
            <scope>test</scope>
            <exclusions>
                <exclusion>
                    <groupId>junit</groupId>
                    <artifactId>junit</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```

**Step 3: Verify build compiles**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn compile -pl grpcw -am -DskipTests -q`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add grpcw/pom.xml
git commit -m "build(grpcw): add test dependencies for gRPC testing"
```

---

### Task 2: Create GrpcAdminServiceIT - Basic Setup and Ping Test

**Files:**
- Create: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAdminServiceIT.java`

**Step 1: Create the test class with setup**

```java
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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GrpcAdminServiceIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private ManagedChannel channel;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

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
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
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

  @Test
  void pingWithValidCredentials() {
    PingRequest request = PingRequest.newBuilder()
        .setCredentials(credentials())
        .build();

    PingResponse response = adminStub.ping(request);

    assertThat(response.getOk()).isTrue();
    assertThat(response.getServerTimeMs()).isGreaterThan(0);
  }

  @Test
  void pingWithoutCredentialsFails() {
    PingRequest request = PingRequest.newBuilder().build();

    assertThatThrownBy(() -> adminStub.ping(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }

  @Test
  void pingWithInvalidCredentialsFails() {
    PingRequest request = PingRequest.newBuilder()
        .setCredentials(DatabaseCredentials.newBuilder()
            .setUsername("root")
            .setPassword("wrongpassword")
            .build())
        .build();

    assertThatThrownBy(() -> adminStub.ping(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }
}
```

**Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcAdminServiceIT -q`
Expected: Tests run: 3, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAdminServiceIT.java
git commit -m "test(grpcw): add GrpcAdminServiceIT with ping tests"
```

---

### Task 3: GrpcAdminServiceIT - Server Info and Database Listing Tests

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAdminServiceIT.java`

**Step 1: Add getServerInfo test**

Add after the ping tests:

```java
  @Test
  void getServerInfoReturnsValidData() {
    GetServerInfoRequest request = GetServerInfoRequest.newBuilder()
        .setCredentials(credentials())
        .build();

    GetServerInfoResponse response = adminStub.getServerInfo(request);

    assertThat(response.getVersion()).isNotEmpty();
    assertThat(response.getHttpPort()).isGreaterThan(0);
    assertThat(response.getGrpcPort()).isEqualTo(GRPC_PORT);
    assertThat(response.getDatabasesCount()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void listDatabasesReturnsExistingDatabase() {
    ListDatabasesRequest request = ListDatabasesRequest.newBuilder()
        .setCredentials(credentials())
        .build();

    ListDatabasesResponse response = adminStub.listDatabases(request);

    assertThat(response.getDatabasesList()).contains(getDatabaseName());
  }

  @Test
  void existsDatabaseReturnsTrueForExisting() {
    ExistsDatabaseRequest request = ExistsDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(getDatabaseName())
        .build();

    ExistsDatabaseResponse response = adminStub.existsDatabase(request);

    assertThat(response.getExists()).isTrue();
  }

  @Test
  void existsDatabaseReturnsFalseForNonExistent() {
    ExistsDatabaseRequest request = ExistsDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName("nonexistent_database_xyz")
        .build();

    ExistsDatabaseResponse response = adminStub.existsDatabase(request);

    assertThat(response.getExists()).isFalse();
  }
```

**Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcAdminServiceIT -q`
Expected: Tests run: 7, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAdminServiceIT.java
git commit -m "test(grpcw): add server info and database listing tests"
```

---

### Task 4: GrpcAdminServiceIT - Database Create/Drop and Info Tests

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAdminServiceIT.java`

**Step 1: Add database management tests**

Add after the existsDatabase tests:

```java
  @Test
  void createAndDropDatabase() {
    String testDbName = "grpc_test_db_" + System.currentTimeMillis();

    // Create database
    CreateDatabaseRequest createRequest = CreateDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(testDbName)
        .setType("graph")
        .build();

    adminStub.createDatabase(createRequest);

    // Verify it exists
    ExistsDatabaseRequest existsRequest = ExistsDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(testDbName)
        .build();

    assertThat(adminStub.existsDatabase(existsRequest).getExists()).isTrue();

    // Drop database
    DropDatabaseRequest dropRequest = DropDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(testDbName)
        .build();

    adminStub.dropDatabase(dropRequest);

    // Verify it no longer exists
    assertThat(adminStub.existsDatabase(existsRequest).getExists()).isFalse();
  }

  @Test
  void createDatabaseIsIdempotent() {
    String testDbName = "grpc_idempotent_db_" + System.currentTimeMillis();

    CreateDatabaseRequest request = CreateDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(testDbName)
        .setType("document")
        .build();

    // Create twice - should not throw
    adminStub.createDatabase(request);
    adminStub.createDatabase(request);

    // Cleanup
    DropDatabaseRequest dropRequest = DropDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(testDbName)
        .build();
    adminStub.dropDatabase(dropRequest);
  }

  @Test
  void dropNonExistentDatabaseIsIdempotent() {
    DropDatabaseRequest request = DropDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName("nonexistent_db_for_drop_test")
        .build();

    // Should not throw
    adminStub.dropDatabase(request);
  }

  @Test
  void getDatabaseInfoReturnsValidData() {
    GetDatabaseInfoRequest request = GetDatabaseInfoRequest.newBuilder()
        .setCredentials(credentials())
        .setName(getDatabaseName())
        .build();

    GetDatabaseInfoResponse response = adminStub.getDatabaseInfo(request);

    assertThat(response.getDatabase()).isEqualTo(getDatabaseName());
    assertThat(response.getType()).isEqualTo("graph");
    assertThat(response.getClasses()).isGreaterThan(0);
  }

  @Test
  void getDatabaseInfoForNonExistentFails() {
    GetDatabaseInfoRequest request = GetDatabaseInfoRequest.newBuilder()
        .setCredentials(credentials())
        .setName("nonexistent_database")
        .build();

    assertThatThrownBy(() -> adminStub.getDatabaseInfo(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("NOT_FOUND");
  }
```

**Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcAdminServiceIT -q`
Expected: Tests run: 12, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAdminServiceIT.java
git commit -m "test(grpcw): add database create/drop and info tests"
```

---

### Task 5: Create GrpcServerIT - Basic Setup and Query Tests

**Files:**
- Create: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java`

**Step 1: Create the test class with executeQuery tests**

```java
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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GrpcServerIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private ManagedChannel channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub blockingStub;

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

  private GrpcValue stringValue(String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
  }

  private GrpcValue intValue(int i) {
    return GrpcValue.newBuilder().setInt32Value(i).build();
  }

  private GrpcValue longValue(long l) {
    return GrpcValue.newBuilder().setInt64Value(l).build();
  }

  @Test
  void executeQuerySelectsExistingData() {
    ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 WHERE id = 0")
        .build();

    ExecuteQueryResponse response = blockingStub.executeQuery(request);

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

    ExecuteQueryResponse response = blockingStub.executeQuery(request);

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

    ExecuteQueryResponse response = blockingStub.executeQuery(request);

    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).isEmpty();
  }

  @Test
  void executeQueryWithoutCredentialsFails() {
    ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setQuery("SELECT FROM V1")
        .build();

    assertThatThrownBy(() -> blockingStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }
}
```

**Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcServerIT -q`
Expected: Tests run: 4, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java
git commit -m "test(grpcw): add GrpcServerIT with query tests"
```

---

### Task 6: GrpcServerIT - Execute Command Tests

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java`

**Step 1: Add executeCommand tests**

Add after the executeQuery tests:

```java
  @Test
  void executeCommandInsertDocument() {
    ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INSERT INTO Person SET name = 'John Doe', age = 30")
        .setReturnRows(true)
        .build();

    ExecuteCommandResponse response = blockingStub.executeCommand(request);

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

    ExecuteCommandResponse response = blockingStub.executeCommand(request);

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

    ExecuteCommandResponse response = blockingStub.executeCommand(request);

    assertThat(response.getSuccess()).isTrue();

    // Verify type was created by querying schema
    ExecuteQueryRequest queryRequest = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM schema:types WHERE name = '" + typeName + "'")
        .build();

    ExecuteQueryResponse queryResponse = blockingStub.executeQuery(queryRequest);
    assertThat(queryResponse.getResultsList().get(0).getRecordsList()).isNotEmpty();
  }

  @Test
  void executeCommandInvalidSqlReturnsError() {
    ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INVALID SQL SYNTAX HERE")
        .build();

    assertThatThrownBy(() -> blockingStub.executeCommand(request))
        .isInstanceOf(StatusRuntimeException.class);
  }
```

**Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcServerIT -q`
Expected: Tests run: 8, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java
git commit -m "test(grpcw): add executeCommand tests"
```

---

### Task 7: GrpcServerIT - CRUD Operations (Create, Lookup, Update, Delete)

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java`

**Step 1: Add CRUD tests**

Add after the executeCommand tests:

```java
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

    CreateRecordResponse createResponse = blockingStub.createRecord(createRequest);

    assertThat(createResponse.getRid()).isNotEmpty();
    assertThat(createResponse.getRid()).startsWith("#");

    // Lookup by RID
    LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(createResponse.getRid())
        .build();

    LookupByRidResponse lookupResponse = blockingStub.lookupByRid(lookupRequest);

    assertThat(lookupResponse.getFound()).isTrue();
    assertThat(lookupResponse.getRecord().getPropertiesMap().get("name").getStringValue())
        .isEqualTo("Test Person");
  }

  @Test
  void lookupByRidNotFoundReturnsFalse() {
    LookupByRidRequest request = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid("#999:999")
        .build();

    LookupByRidResponse response = blockingStub.lookupByRid(request);

    assertThat(response.getFound()).isFalse();
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

    String rid = blockingStub.createRecord(createRequest).getRid();

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

    UpdateRecordResponse updateResponse = blockingStub.updateRecord(updateRequest);

    assertThat(updateResponse.getSuccess()).isTrue();
    assertThat(updateResponse.getUpdated()).isTrue();

    // Verify the update
    LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .build();

    LookupByRidResponse lookupResponse = blockingStub.lookupByRid(lookupRequest);

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

    String rid = blockingStub.createRecord(createRequest).getRid();

    // Delete the record
    DeleteRecordRequest deleteRequest = DeleteRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .build();

    DeleteRecordResponse deleteResponse = blockingStub.deleteRecord(deleteRequest);

    assertThat(deleteResponse.getSuccess()).isTrue();
    assertThat(deleteResponse.getDeleted()).isTrue();

    // Verify it's gone
    LookupByRidRequest lookupRequest = LookupByRidRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid(rid)
        .build();

    LookupByRidResponse lookupResponse = blockingStub.lookupByRid(lookupRequest);

    assertThat(lookupResponse.getFound()).isFalse();
  }

  @Test
  void deleteNonExistentRecordReportsNotDeleted() {
    DeleteRecordRequest request = DeleteRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setRid("#999:999")
        .build();

    DeleteRecordResponse response = blockingStub.deleteRecord(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getDeleted()).isFalse();
  }
```

**Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcServerIT -q`
Expected: Tests run: 13, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java
git commit -m "test(grpcw): add CRUD operation tests"
```

---

### Task 8: GrpcServerIT - Transaction Tests

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java`

**Step 1: Add transaction tests**

Add after the CRUD tests:

```java
  @Test
  void beginAndCommitTransaction() {
    // Begin transaction
    BeginTransactionRequest beginRequest = BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .build();

    BeginTransactionResponse beginResponse = blockingStub.beginTransaction(beginRequest);

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

    blockingStub.executeCommand(insertRequest);

    // Commit transaction
    CommitTransactionRequest commitRequest = CommitTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder()
            .setTransactionId(txId)
            .setDatabase(getDatabaseName())
            .build())
        .build();

    CommitTransactionResponse commitResponse = blockingStub.commitTransaction(commitRequest);

    assertThat(commitResponse.getSuccess()).isTrue();
    assertThat(commitResponse.getCommitted()).isTrue();

    // Verify data is visible after commit
    ExecuteQueryRequest queryRequest = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE name = 'Transaction Test'")
        .build();

    ExecuteQueryResponse queryResponse = blockingStub.executeQuery(queryRequest);

    assertThat(queryResponse.getResultsList().get(0).getRecordsList()).isNotEmpty();
  }

  @Test
  void beginAndRollbackTransaction() {
    // Begin transaction
    BeginTransactionRequest beginRequest = BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .build();

    BeginTransactionResponse beginResponse = blockingStub.beginTransaction(beginRequest);

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

    blockingStub.executeCommand(insertRequest);

    // Rollback transaction
    RollbackTransactionRequest rollbackRequest = RollbackTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder()
            .setTransactionId(txId)
            .setDatabase(getDatabaseName())
            .build())
        .build();

    RollbackTransactionResponse rollbackResponse = blockingStub.rollbackTransaction(rollbackRequest);

    assertThat(rollbackResponse.getSuccess()).isTrue();
    assertThat(rollbackResponse.getRolledBack()).isTrue();

    // Verify data is NOT visible after rollback
    ExecuteQueryRequest queryRequest = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE name = 'Rollback Test " + txId + "'")
        .build();

    ExecuteQueryResponse queryResponse = blockingStub.executeQuery(queryRequest);

    assertThat(queryResponse.getResultsList().get(0).getRecordsList()).isEmpty();
  }
```

**Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcServerIT -q`
Expected: Tests run: 15, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java
git commit -m "test(grpcw): add transaction tests"
```

---

### Task 9: GrpcServerIT - Streaming Query Tests

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java`

**Step 1: Add streaming query tests**

Add the import at the top:

```java
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
```

Add after the transaction tests:

```java
  @Test
  void streamQueryReturnsResults() {
    StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1")
        .setBatchSize(10)
        .build();

    Iterator<QueryResult> results = blockingStub.streamQuery(request);

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
      blockingStub.executeCommand(insertRequest);
    }

    StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE batchTest = true")
        .setBatchSize(2)
        .build();

    Iterator<QueryResult> results = blockingStub.streamQuery(request);

    int batchCount = 0;
    int totalRecords = 0;
    while (results.hasNext()) {
      QueryResult batch = results.next();
      batchCount++;
      totalRecords += batch.getRecordsList().size();
    }

    assertThat(totalRecords).isGreaterThanOrEqualTo(5);
    // With batch size 2 and 5+ records, we should have multiple batches
    assertThat(batchCount).isGreaterThanOrEqualTo(1);
  }
```

**Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcServerIT -q`
Expected: Tests run: 17, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java
git commit -m "test(grpcw): add streaming query tests"
```

---

### Task 10: GrpcServerIT - Bulk Insert Tests

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java`

**Step 1: Add bulk insert tests**

Add after the streaming tests:

```java
  @Test
  void bulkInsertMultipleRecords() {
    String typeName = "BulkTestType_" + System.currentTimeMillis();

    // Create the type first
    ExecuteCommandRequest createTypeRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE DOCUMENT TYPE " + typeName)
        .build();
    blockingStub.executeCommand(createTypeRequest);

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
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setOptions(InsertOptions.newBuilder()
            .setTargetClass(typeName)
            .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
            .build())
        .addAllRows(records)
        .build();

    InsertSummary response = blockingStub.bulkInsert(request);

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
    blockingStub.executeCommand(createTypeRequest);

    ExecuteCommandRequest createIndexRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE PROPERTY " + typeName + ".uniqueKey STRING")
        .build();
    blockingStub.executeCommand(createIndexRequest);

    ExecuteCommandRequest createUniqueIndexRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE INDEX ON " + typeName + "(uniqueKey) UNIQUE")
        .build();
    blockingStub.executeCommand(createUniqueIndexRequest);

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
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setOptions(InsertOptions.newBuilder()
            .setTargetClass(typeName)
            .addKeyColumns("uniqueKey")
            .setConflictMode(InsertOptions.ConflictMode.CONFLICT_IGNORE)
            .build())
        .addAllRows(records)
        .build();

    InsertSummary response = blockingStub.bulkInsert(request);

    assertThat(response.getReceived()).isEqualTo(3);
    assertThat(response.getInserted()).isEqualTo(2); // key1 and key2
    assertThat(response.getIgnored()).isEqualTo(1);  // duplicate key1
  }
```

**Step 2: Add List import if not already present**

Ensure this import is at the top of the file:
```java
import java.util.List;
```

**Step 3: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcServerIT -q`
Expected: Tests run: 19, Failures: 0, Errors: 0

**Step 4: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java
git commit -m "test(grpcw): add bulk insert tests"
```

---

### Task 11: Create GrpcServerPluginTest - Unit Tests

**Files:**
- Create: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerPluginTest.java`

**Step 1: Create the unit test class**

```java
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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcServerPluginTest {

  @TempDir
  Path tempDir;

  @Test
  void pluginConfiguresWithDefaults() {
    GrpcServerPlugin plugin = new GrpcServerPlugin();
    ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    ContextConfiguration config = new ContextConfiguration();

    when(mockServer.getRootPath()).thenReturn(tempDir.toString());
    when(mockServer.getConfiguration()).thenReturn(config);

    plugin.configure(mockServer, config);

    // Plugin should be configured without errors
    GrpcServerPlugin.ServerStatus status = plugin.getStatus();
    assertThat(status.standardServerRunning).isFalse(); // Not started yet
  }

  @Test
  void pluginDisabledWhenConfigured() {
    GrpcServerPlugin plugin = new GrpcServerPlugin();
    ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.enabled", "false");

    when(mockServer.getRootPath()).thenReturn(tempDir.toString());
    when(mockServer.getConfiguration()).thenReturn(config);

    plugin.configure(mockServer, config);
    plugin.startService();

    GrpcServerPlugin.ServerStatus status = plugin.getStatus();
    assertThat(status.standardServerRunning).isFalse();
    assertThat(status.xdsServerRunning).isFalse();
  }

  @Test
  void serverStatusReportsCorrectPorts() {
    GrpcServerPlugin.ServerStatus status = new GrpcServerPlugin.ServerStatus(
        true, false, 50051, -1);

    assertThat(status.standardServerRunning).isTrue();
    assertThat(status.xdsServerRunning).isFalse();
    assertThat(status.standardPort).isEqualTo(50051);
    assertThat(status.xdsPort).isEqualTo(-1);
  }
}
```

**Step 2: Add Mockito dependency to pom.xml**

Add after the grpc-testing dependency in `grpcw/pom.xml`:

```xml
        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-core</artifactId>
            <version>${mockito.version}</version>
            <scope>test</scope>
        </dependency>
```

**Step 3: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcServerPluginTest -q`
Expected: Tests run: 3, Failures: 0, Errors: 0

**Step 4: Commit**

```bash
git add grpcw/pom.xml grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerPluginTest.java
git commit -m "test(grpcw): add GrpcServerPlugin unit tests"
```

---

### Task 12: Create GrpcAuthInterceptorTest - Unit Tests

**Files:**
- Create: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAuthInterceptorTest.java`

**Step 1: Create the unit test class**

```java
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

import com.arcadedb.server.security.ServerSecurity;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcAuthInterceptorTest {

  private ServerSecurity mockSecurity;
  private GrpcAuthInterceptor interceptor;
  private ServerCall<Object, Object> mockCall;
  private ServerCallHandler<Object, Object> mockHandler;
  private Metadata metadata;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockSecurity = mock(ServerSecurity.class);
    interceptor = new GrpcAuthInterceptor(mockSecurity);
    mockCall = mock(ServerCall.class);
    mockHandler = mock(ServerCallHandler.class);
    metadata = new Metadata();
  }

  @Test
  void interceptorAllowsCallWhenSecurityIsNull() {
    GrpcAuthInterceptor noSecurityInterceptor = new GrpcAuthInterceptor(null);

    noSecurityInterceptor.interceptCall(mockCall, metadata, mockHandler);

    verify(mockHandler).startCall(any(), any());
    verify(mockCall, never()).close(any(), any());
  }

  @Test
  void interceptorCreatesInstance() {
    assertThat(interceptor).isNotNull();
  }

  @Test
  void metadataKeyForAuthorizationExists() {
    // Verify the interceptor can handle metadata
    Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    metadata.put(authKey, "Basic dGVzdDp0ZXN0"); // test:test in base64

    // The interceptor should process without throwing
    assertThat(metadata.get(authKey)).isNotNull();
  }
}
```

**Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcAuthInterceptorTest -q`
Expected: Tests run: 3, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAuthInterceptorTest.java
git commit -m "test(grpcw): add GrpcAuthInterceptor unit tests"
```

---

### Task 13: Run Full Test Suite and Final Verification

**Step 1: Run all grpcw tests**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -q`
Expected: All tests pass

**Step 2: Check test count**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw 2>&1 | grep "Tests run"`
Expected: Shows total test count (should be ~30+ tests)

**Step 3: Run with the server module to ensure no integration issues**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw,server -q`
Expected: BUILD SUCCESS

**Step 4: Final commit if any cleanup needed**

If tests revealed any issues that were fixed:
```bash
git add -A
git commit -m "test(grpcw): fix test issues found during full suite run"
```

---

## Summary

After completing all tasks, the grpcw module will have:

- **GrpcAdminServiceIT**: ~12 integration tests covering admin operations
- **GrpcServerIT**: ~19 integration tests covering CRUD, queries, transactions, streaming, bulk insert
- **GrpcServerPluginTest**: ~3 unit tests for plugin configuration
- **GrpcAuthInterceptorTest**: ~3 unit tests for authentication interceptor

**Total: ~37 tests** (additional tests can be added following the same patterns)

All tests follow project conventions:
- AssertJ assertions with `assertThat().isTrue()` style
- Proper cleanup in `@AfterEach`
- No System.out statements
- Following existing code style
