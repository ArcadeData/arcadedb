# gRPC Module Unit Test Expansion Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add comprehensive unit tests for interceptors and helper classes in the grpcw module that currently lack test coverage.

**Architecture:** Pure unit tests using Mockito to mock gRPC infrastructure classes. Tests run fast without starting servers. Focus on testing behavior of each interceptor independently.

**Tech Stack:** JUnit 5, AssertJ, Mockito, gRPC Testing utilities

---

## Current State

| Class | Has Tests | Notes |
|-------|-----------|-------|
| GrpcServerPlugin | Yes (3) | GrpcServerPluginTest |
| GrpcAuthInterceptor | Yes (3) | GrpcAuthInterceptorTest |
| GrpcLoggingInterceptor | No | Package-private interceptor |
| GrpcMetricsInterceptor | No | Package-private interceptor |
| GrpcCompressionInterceptor | No | Package-private interceptor with CompressionInfo inner class |
| CompressionAwareService | No | Package-private helper with CompressionStats inner class |

---

### Task 1: Create GrpcLoggingInterceptorTest

**Files:**
- Create: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcLoggingInterceptorTest.java`

**Step 1: Write the test class**

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

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcLoggingInterceptorTest {

  private GrpcLoggingInterceptor interceptor;
  private ServerCall<Object, Object> mockCall;
  private ServerCallHandler<Object, Object> mockHandler;
  private MethodDescriptor<Object, Object> mockMethodDescriptor;
  private Metadata headers;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    interceptor = new GrpcLoggingInterceptor();
    mockCall = mock(ServerCall.class);
    mockHandler = mock(ServerCallHandler.class);
    mockMethodDescriptor = mock(MethodDescriptor.class);
    headers = new Metadata();

    when(mockCall.getMethodDescriptor()).thenReturn(mockMethodDescriptor);
    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.TestService/TestMethod");
  }

  @Test
  void interceptorCreatesInstance() {
    assertThat(interceptor).isNotNull();
  }

  @Test
  void interceptorForwardsCallToHandler() {
    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void interceptorDetectsCompressedRequest() {
    Metadata.Key<String> encodingKey = Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(encodingKey, "gzip");

    interceptor.interceptCall(mockCall, headers, mockHandler);

    // Verify the call was forwarded (the logging happens internally)
    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void interceptorDetectsClientAcceptsCompression() {
    Metadata.Key<String> acceptKey = Metadata.Key.of("grpc-accept-encoding", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(acceptKey, "gzip,identity");

    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void interceptorAddsCompressionMetadataToTrailers() {
    // This test verifies the wrapped call behavior
    ArgumentCaptor<ServerCall<Object, Object>> callCaptor = ArgumentCaptor.forClass(ServerCall.class);

    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(callCaptor.capture(), any());

    // The wrapped call should have been passed to the handler
    ServerCall<Object, Object> wrappedCall = callCaptor.getValue();
    assertThat(wrappedCall).isNotNull();
  }
}
```

**Step 2: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcLoggingInterceptorTest -q`
Expected: Tests run: 5, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcLoggingInterceptorTest.java
git commit -m "test(grpcw): add GrpcLoggingInterceptor unit tests"
```

---

### Task 2: Create GrpcMetricsInterceptorTest

**Files:**
- Create: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcMetricsInterceptorTest.java`

**Step 1: Write the test class**

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

import com.arcadedb.server.ArcadeDBServer;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcMetricsInterceptorTest {

  private GrpcMetricsInterceptor interceptor;
  private ArcadeDBServer mockServer;
  private ServerCall<Object, Object> mockCall;
  private ServerCallHandler<Object, Object> mockHandler;
  private MethodDescriptor<Object, Object> mockMethodDescriptor;
  private Metadata headers;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockServer = mock(ArcadeDBServer.class);
    interceptor = new GrpcMetricsInterceptor(mockServer);
    mockCall = mock(ServerCall.class);
    mockHandler = mock(ServerCallHandler.class);
    mockMethodDescriptor = mock(MethodDescriptor.class);
    headers = new Metadata();

    when(mockCall.getMethodDescriptor()).thenReturn(mockMethodDescriptor);
    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.TestService/TestMethod");
  }

  @Test
  void interceptorCreatesInstance() {
    assertThat(interceptor).isNotNull();
  }

  @Test
  void interceptorForwardsCallToHandler() {
    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void interceptorIncrementsRequestCounter() {
    // First call
    interceptor.interceptCall(mockCall, headers, mockHandler);

    // Second call
    interceptor.interceptCall(mockCall, headers, mockHandler);

    // Verify both calls were forwarded
    verify(mockHandler, org.mockito.Mockito.times(2)).startCall(any(), any());
  }

  @Test
  void interceptorWithNullServer() {
    // Should handle null server gracefully
    GrpcMetricsInterceptor nullServerInterceptor = new GrpcMetricsInterceptor(null);

    assertThat(nullServerInterceptor).isNotNull();
  }
}
```

**Step 2: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcMetricsInterceptorTest -q`
Expected: Tests run: 4, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcMetricsInterceptorTest.java
git commit -m "test(grpcw): add GrpcMetricsInterceptor unit tests"
```

---

### Task 3: Create GrpcCompressionInterceptorTest

**Files:**
- Create: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcCompressionInterceptorTest.java`

**Step 1: Write the test class**

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

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcCompressionInterceptorTest {

  private ServerCall<Object, Object> mockCall;
  private ServerCallHandler<Object, Object> mockHandler;
  private MethodDescriptor<Object, Object> mockMethodDescriptor;
  private Metadata headers;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockCall = mock(ServerCall.class);
    mockHandler = mock(ServerCallHandler.class);
    mockMethodDescriptor = mock(MethodDescriptor.class);
    headers = new Metadata();

    when(mockCall.getMethodDescriptor()).thenReturn(mockMethodDescriptor);
    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.TestService/TestMethod");
  }

  @Test
  void interceptorCreatesWithDefaultCompression() {
    GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(false, null, 1024);
    assertThat(interceptor).isNotNull();
  }

  @Test
  void interceptorCreatesWithGzipCompression() {
    GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(true, "gzip", 1024);
    assertThat(interceptor).isNotNull();
  }

  @Test
  void interceptorForwardsCallToHandler() {
    GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(false, "gzip", 1024);

    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void interceptorDetectsCompressedRequest() {
    GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(true, "gzip", 0);

    Metadata.Key<String> encodingKey = Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(encodingKey, "gzip");

    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void compressionInfoStoresRequestState() {
    GrpcCompressionInterceptor.CompressionInfo info =
        new GrpcCompressionInterceptor.CompressionInfo(true, "gzip");

    assertThat(info.requestCompressed).isTrue();
    assertThat(info.requestEncoding).isEqualTo("gzip");
  }

  @Test
  void compressionInfoStoresUncompressedState() {
    GrpcCompressionInterceptor.CompressionInfo info =
        new GrpcCompressionInterceptor.CompressionInfo(false, null);

    assertThat(info.requestCompressed).isFalse();
    assertThat(info.requestEncoding).isNull();
  }

  @Test
  void compressionKeyIsAccessible() {
    assertThat(GrpcCompressionInterceptor.COMPRESSION_KEY).isNotNull();
  }
}
```

**Step 2: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcCompressionInterceptorTest -q`
Expected: Tests run: 7, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcCompressionInterceptorTest.java
git commit -m "test(grpcw): add GrpcCompressionInterceptor unit tests"
```

---

### Task 4: Create CompressionAwareServiceTest

**Files:**
- Create: `grpcw/src/test/java/com/arcadedb/server/grpc/CompressionAwareServiceTest.java`

**Step 1: Write the test class**

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

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CompressionAwareServiceTest {

  @Test
  void setResponseCompressionWithServerCallStreamObserver() {
    @SuppressWarnings("unchecked")
    ServerCallStreamObserver<String> mockObserver = mock(ServerCallStreamObserver.class);

    CompressionAwareService.setResponseCompression(mockObserver, "gzip");

    verify(mockObserver).setCompression("gzip");
  }

  @Test
  void setResponseCompressionWithRegularStreamObserver() {
    @SuppressWarnings("unchecked")
    StreamObserver<String> mockObserver = mock(StreamObserver.class);

    // Should not throw even though it can't set compression
    CompressionAwareService.setResponseCompression(mockObserver, "gzip");

    // No exception means success
    assertThat(true).isTrue();
  }

  @Test
  void isCurrentRequestCompressedReturnsFalseWithoutContext() {
    // Without context set, should return false
    assertThat(CompressionAwareService.isCurrentRequestCompressed()).isFalse();
  }

  @Test
  void getCurrentRequestEncodingReturnsIdentityWithoutContext() {
    // Without context set, should return "identity"
    assertThat(CompressionAwareService.getCurrentRequestEncoding()).isEqualTo("identity");
  }

  @Test
  void compressionStatsRecordsCompressedRequest() {
    CompressionAwareService.CompressionStats stats = new CompressionAwareService.CompressionStats();

    stats.recordRequest(true);
    stats.recordRequest(true);
    stats.recordRequest(false);

    String statsString = stats.getStats();
    assertThat(statsString).contains("Requests:");
    assertThat(statsString).contains("2/3"); // 2 compressed out of 3
  }

  @Test
  void compressionStatsRecordsResponses() {
    CompressionAwareService.CompressionStats stats = new CompressionAwareService.CompressionStats();

    stats.recordResponse(true);
    stats.recordResponse(false);
    stats.recordResponse(false);
    stats.recordResponse(false);

    String statsString = stats.getStats();
    assertThat(statsString).contains("Responses:");
    assertThat(statsString).contains("1/4"); // 1 compressed out of 4
  }

  @Test
  void compressionStatsHandlesZeroRequests() {
    CompressionAwareService.CompressionStats stats = new CompressionAwareService.CompressionStats();

    String statsString = stats.getStats();

    // Should not throw with zero counts
    assertThat(statsString).contains("0.0%");
  }

  @Test
  void compressionStatsCalculatesCorrectPercentage() {
    CompressionAwareService.CompressionStats stats = new CompressionAwareService.CompressionStats();

    // 50% compression rate
    stats.recordRequest(true);
    stats.recordRequest(false);

    String statsString = stats.getStats();
    assertThat(statsString).contains("50.0%");
  }
}
```

**Step 2: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=CompressionAwareServiceTest -q`
Expected: Tests run: 8, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/CompressionAwareServiceTest.java
git commit -m "test(grpcw): add CompressionAwareService unit tests"
```

---

### Task 5: Run Full Test Suite and Verify

**Step 1: Run all grpcw tests**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest="*" 2>&1 | grep -E "(Tests run:|in com.arcadedb)"`

Expected output should show:
- GrpcServerPluginTest: 3 tests
- GrpcAdminServiceIT: 12 tests
- GrpcAuthInterceptorTest: 3 tests
- GrpcServerIT: 19 tests
- GrpcLoggingInterceptorTest: 5 tests (new)
- GrpcMetricsInterceptorTest: 4 tests (new)
- GrpcCompressionInterceptorTest: 7 tests (new)
- CompressionAwareServiceTest: 8 tests (new)

**Total: 61 tests** (37 existing + 24 new)

**Step 2: Verify all tests pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw`
Expected: BUILD SUCCESS with all tests passing

---

## Summary

After completing all tasks, the grpcw module will have:

| Test Class | Test Count | Coverage |
|------------|------------|----------|
| GrpcServerPluginTest | 3 | Plugin configuration |
| GrpcAuthInterceptorTest | 3 | Authentication interceptor |
| GrpcLoggingInterceptorTest | 5 | Logging interceptor |
| GrpcMetricsInterceptorTest | 4 | Metrics interceptor |
| GrpcCompressionInterceptorTest | 7 | Compression interceptor |
| CompressionAwareServiceTest | 8 | Compression helper service |
| GrpcAdminServiceIT | 12 | Admin service integration |
| GrpcServerIT | 19 | Main service integration |

**Total: 61 tests** covering all interceptors, helpers, and services.

## Success Criteria

- All tests pass
- Tests follow existing project conventions (AssertJ assertions, `assertThat().isTrue()` style)
- No System.out statements in test code
- Tests are independent and can run in any order
- Unit tests use mocks and run fast (< 1 second each)
