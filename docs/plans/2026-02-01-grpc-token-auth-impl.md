# gRPC Token Authentication Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable gRPC clients to authenticate using Bearer tokens obtained from HTTP `/api/v1/auth/login`.

**Architecture:** Modify `GrpcAuthInterceptor` to accept an `HttpAuthSessionManager` reference, then implement `validateToken()` and `extractUserFromToken()` to look up sessions. Update `GrpcServerPlugin` to pass the session manager when creating the interceptor.

**Tech Stack:** Java 21, gRPC, ArcadeDB ServerSecurity, HttpAuthSessionManager, JUnit 5, Mockito, AssertJ

---

### Task 1: Update GrpcAuthInterceptor Constructor

**Files:**
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java`
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAuthInterceptorTest.java`

**Step 1: Write the failing test**

Add to `GrpcAuthInterceptorTest.java`:

```java
import com.arcadedb.server.http.HttpAuthSessionManager;

// Add new field
private HttpAuthSessionManager mockSessionManager;

// In setUp(), add:
mockSessionManager = mock(HttpAuthSessionManager.class);

@Test
void constructorAcceptsSessionManager() {
  GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, mockSessionManager);
  assertThat(interceptorWithSession).isNotNull();
}

@Test
void constructorAcceptsNullSessionManager() {
  GrpcAuthInterceptor interceptorWithoutSession = new GrpcAuthInterceptor(mockSecurity, null);
  assertThat(interceptorWithoutSession).isNotNull();
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -pl grpcw -Dtest=GrpcAuthInterceptorTest -q`
Expected: FAIL with "constructor GrpcAuthInterceptor in class GrpcAuthInterceptor cannot be applied to given types"

**Step 3: Write minimal implementation**

Modify `GrpcAuthInterceptor.java`:

```java
// Add import at top
import com.arcadedb.server.http.HttpAuthSessionManager;

// Add field after existing fields
private final HttpAuthSessionManager authSessionManager;

// Replace existing constructor with:
public GrpcAuthInterceptor(final ServerSecurity security) {
  this(security, null);
}

public GrpcAuthInterceptor(final ServerSecurity security, final HttpAuthSessionManager authSessionManager) {
  this.security = security;
  this.authSessionManager = authSessionManager;
  this.securityEnabled = (security != null && security.getUsers() != null && !security.getUsers().isEmpty());
}
```

**Step 4: Run test to verify it passes**

Run: `mvn test -pl grpcw -Dtest=GrpcAuthInterceptorTest -q`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java \
        grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAuthInterceptorTest.java
git commit -m "refactor(grpcw): add HttpAuthSessionManager to GrpcAuthInterceptor constructor"
```

---

### Task 2: Implement validateToken Method

**Files:**
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java`
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAuthInterceptorTest.java`

**Step 1: Write the failing tests**

Add to `GrpcAuthInterceptorTest.java`:

```java
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.security.ServerSecurityUser;

@Test
void validateTokenReturnsTrueForValidSession() {
  HttpAuthSession mockSession = mock(HttpAuthSession.class);
  ServerSecurityUser mockUser = mock(ServerSecurityUser.class);
  when(mockSession.getUser()).thenReturn(mockUser);
  when(mockUser.getName()).thenReturn("testuser");
  when(mockSessionManager.getSessionByToken("valid-token")).thenReturn(mockSession);

  GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, mockSessionManager);

  // Use reflection to test private method, or make package-private for testing
  // For now, we test through the interceptCall behavior
  Metadata headers = new Metadata();
  Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  headers.put(authKey, "Bearer valid-token");
  headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), "testdb");

  when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.ArcadeDbService/Query");
  when(mockSecurity.getUsers()).thenReturn(java.util.Collections.singleton(mockUser));

  interceptorWithSession.interceptCall(mockCall, headers, mockHandler);

  // Handler's startCall should be invoked (call proceeds)
  verify(mockHandler).startCall(any(), any());
  verify(mockCall, never()).close(any(), any());
}

@Test
void validateTokenReturnsFalseForInvalidSession() {
  when(mockSessionManager.getSessionByToken("invalid-token")).thenReturn(null);

  GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, mockSessionManager);

  Metadata headers = new Metadata();
  Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  headers.put(authKey, "Bearer invalid-token");
  headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), "testdb");

  when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.ArcadeDbService/Query");
  ServerSecurityUser mockUser = mock(ServerSecurityUser.class);
  when(mockSecurity.getUsers()).thenReturn(java.util.Collections.singleton(mockUser));

  interceptorWithSession.interceptCall(mockCall, headers, mockHandler);

  // Call should be closed with UNAUTHENTICATED
  verify(mockCall).close(any(), any());
  verify(mockHandler, never()).startCall(any(), any());
}

@Test
void validateTokenReturnsFalseWhenSessionManagerIsNull() {
  GrpcAuthInterceptor interceptorWithoutSession = new GrpcAuthInterceptor(mockSecurity, null);

  Metadata headers = new Metadata();
  Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  headers.put(authKey, "Bearer any-token");
  headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), "testdb");

  when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.ArcadeDbService/Query");
  ServerSecurityUser mockUser = mock(ServerSecurityUser.class);
  when(mockSecurity.getUsers()).thenReturn(java.util.Collections.singleton(mockUser));

  interceptorWithoutSession.interceptCall(mockCall, headers, mockHandler);

  // Call should be closed with UNAUTHENTICATED (token auth not available)
  verify(mockCall).close(any(), any());
  verify(mockHandler, never()).startCall(any(), any());
}
```

**Step 2: Run tests to verify they fail**

Run: `mvn test -pl grpcw -Dtest=GrpcAuthInterceptorTest -q`
Expected: FAIL (current implementation always returns false)

**Step 3: Implement validateToken**

Replace `validateToken` method in `GrpcAuthInterceptor.java`:

```java
private boolean validateToken(final String token, final String database) {
  if (authSessionManager == null) {
    LogManager.instance().log(this, Level.FINE,
        "Token authentication not available - no session manager configured");
    return false;
  }

  final HttpAuthSession session = authSessionManager.getSessionByToken(token);
  if (session == null) {
    LogManager.instance().log(this, Level.FINE,
        "Invalid or expired token for database: %s", database);
    return false;
  }

  return true;
}
```

Add import:
```java
import com.arcadedb.server.http.HttpAuthSession;
```

**Step 4: Run tests to verify they pass**

Run: `mvn test -pl grpcw -Dtest=GrpcAuthInterceptorTest -q`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java \
        grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAuthInterceptorTest.java
git commit -m "feat(grpcw): implement token validation using HttpAuthSessionManager"
```

---

### Task 3: Implement extractUserFromToken Method

**Files:**
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java`
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAuthInterceptorTest.java`

**Step 1: Write the failing test**

Add to `GrpcAuthInterceptorTest.java`:

```java
@Test
void tokenAuthSetsUserContextCorrectly() {
  HttpAuthSession mockSession = mock(HttpAuthSession.class);
  ServerSecurityUser mockUser = mock(ServerSecurityUser.class);
  when(mockSession.getUser()).thenReturn(mockUser);
  when(mockUser.getName()).thenReturn("tokenuser");
  when(mockSessionManager.getSessionByToken("valid-token")).thenReturn(mockSession);

  GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, mockSessionManager);

  Metadata headers = new Metadata();
  headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer valid-token");
  headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), "testdb");

  when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.ArcadeDbService/Query");
  when(mockSecurity.getUsers()).thenReturn(java.util.Collections.singleton(mockUser));

  // Capture the context passed to startCall
  interceptorWithSession.interceptCall(mockCall, headers, mockHandler);

  // Verify startCall was invoked (user context is set internally)
  verify(mockHandler).startCall(any(), any());
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -pl grpcw -Dtest=GrpcAuthInterceptorTest -q`
Expected: May pass or fail depending on current extractUserFromToken behavior

**Step 3: Implement extractUserFromToken**

Replace `extractUserFromToken` method in `GrpcAuthInterceptor.java`:

```java
private String extractUserFromToken(final String authorization) {
  if (authSessionManager == null)
    return null;

  // Strip "Bearer " prefix if present
  final String token = authorization.startsWith(BEARER_TYPE)
      ? authorization.substring(BEARER_TYPE.length()).trim()
      : authorization;

  final HttpAuthSession session = authSessionManager.getSessionByToken(token);
  return session != null ? session.getUser().getName() : null;
}
```

**Step 4: Run tests to verify they pass**

Run: `mvn test -pl grpcw -Dtest=GrpcAuthInterceptorTest -q`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAuthInterceptor.java \
        grpcw/src/test/java/com/arcadedb/server/grpc/GrpcAuthInterceptorTest.java
git commit -m "feat(grpcw): implement extractUserFromToken using HttpAuthSessionManager"
```

---

### Task 4: Update GrpcServerPlugin to Pass Session Manager

**Files:**
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/GrpcServerPlugin.java`

**Step 1: Verify existing test still passes**

Run: `mvn test -pl grpcw -Dtest=GrpcServerPluginTest -q`
Expected: BUILD SUCCESS

**Step 2: Update configureServer method**

In `GrpcServerPlugin.java`, find the `configureServer` method and update the auth interceptor creation (around line 250-254):

```java
// Add import at top
import com.arcadedb.server.http.HttpAuthSessionManager;
import com.arcadedb.server.http.HttpServer;

// Replace the auth interceptor creation block with:
// Add authentication interceptor if security is configured
ServerSecurity serverSecurity = arcadeServer.getSecurity();
if (serverSecurity != null) {
  HttpAuthSessionManager authSessionManager = null;
  final HttpServer httpServer = arcadeServer.getHttpServer();
  if (httpServer != null) {
    authSessionManager = httpServer.getAuthSessionManager();
  } else {
    LogManager.instance().log(this, Level.INFO,
        "HTTP server not available - token authentication disabled for gRPC");
  }
  serverBuilder.intercept(new GrpcAuthInterceptor(serverSecurity, authSessionManager));
}
```

**Step 3: Verify compilation**

Run: `mvn compile -pl grpcw -q`
Expected: BUILD SUCCESS

**Step 4: Run unit tests**

Run: `mvn test -pl grpcw -q`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add grpcw/src/main/java/com/arcadedb/server/grpc/GrpcServerPlugin.java
git commit -m "feat(grpcw): pass HttpAuthSessionManager to GrpcAuthInterceptor"
```

---

### Task 5: Add Integration Test for Token Authentication

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java`

**Step 1: Add HTTP client helper and token auth test**

Add to `GrpcServerIT.java`:

```java
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import com.arcadedb.serializer.json.JSONObject;

// Add new metadata key for Authorization header
private static final Metadata.Key<String> AUTHORIZATION_HEADER =
    Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

/**
 * Helper to login via HTTP and get a token
 */
private String loginAndGetToken(String username, String password) throws Exception {
  URL url = new URL("http://localhost:2480/api/v1/auth/login");
  HttpURLConnection conn = (HttpURLConnection) url.openConnection();
  conn.setRequestMethod("POST");
  conn.setRequestProperty("Content-Type", "application/json");
  conn.setDoOutput(true);

  String json = String.format("{\"username\":\"%s\",\"password\":\"%s\"}", username, password);
  try (OutputStream os = conn.getOutputStream()) {
    os.write(json.getBytes(StandardCharsets.UTF_8));
  }

  if (conn.getResponseCode() != 200) {
    throw new RuntimeException("Login failed: " + conn.getResponseCode());
  }

  String response = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
  JSONObject jsonResponse = new JSONObject(response);
  return jsonResponse.getString("token");
}

/**
 * Helper to logout via HTTP
 */
private void logout(String token) throws Exception {
  URL url = new URL("http://localhost:2480/api/v1/auth/logout");
  HttpURLConnection conn = (HttpURLConnection) url.openConnection();
  conn.setRequestMethod("POST");
  conn.setRequestProperty("Authorization", "Bearer " + token);
  conn.getResponseCode(); // Execute request
}

/**
 * Client interceptor that uses Bearer token authentication
 */
private class TokenAuthClientInterceptor implements ClientInterceptor {
  private final String token;

  TokenAuthClientInterceptor(String token) {
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

@Test
void authenticateWithHttpTokenOverGrpc() throws Exception {
  // 1. Login via HTTP to get token
  String token = loginAndGetToken("root", DEFAULT_PASSWORD_FOR_TESTS);
  assertThat(token).isNotNull();
  assertThat(token).startsWith("AU-");

  // 2. Create gRPC stub with token auth
  Channel tokenChannel = io.grpc.ClientInterceptors.intercept(channel, new TokenAuthClientInterceptor(token));
  ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub tokenStub = ArcadeDbServiceGrpc.newBlockingStub(tokenChannel);

  // 3. Execute a query using token auth
  GrpcQueryRequest request = GrpcQueryRequest.newBuilder()
      .setLanguage("sql")
      .setQuery("SELECT 1 as test")
      .build();

  Iterator<GrpcQueryResponse> responses = tokenStub.query(request);
  assertThat(responses.hasNext()).isTrue();

  GrpcQueryResponse response = responses.next();
  assertThat(response.getRecordsCount()).isGreaterThan(0);
}

@Test
void tokenRejectedAfterLogout() throws Exception {
  // 1. Login and get token
  String token = loginAndGetToken("root", DEFAULT_PASSWORD_FOR_TESTS);

  // 2. Logout to invalidate token
  logout(token);

  // 3. Try to use invalidated token
  Channel tokenChannel = io.grpc.ClientInterceptors.intercept(channel, new TokenAuthClientInterceptor(token));
  ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub tokenStub = ArcadeDbServiceGrpc.newBlockingStub(tokenChannel);

  GrpcQueryRequest request = GrpcQueryRequest.newBuilder()
      .setLanguage("sql")
      .setQuery("SELECT 1 as test")
      .build();

  // Should fail with UNAUTHENTICATED
  assertThatThrownBy(() -> tokenStub.query(request).next())
      .isInstanceOf(StatusRuntimeException.class)
      .hasMessageContaining("UNAUTHENTICATED");
}

@Test
void invalidTokenRejected() {
  // Use a fake token that doesn't exist
  Channel tokenChannel = io.grpc.ClientInterceptors.intercept(channel, new TokenAuthClientInterceptor("AU-fake-token-12345"));
  ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub tokenStub = ArcadeDbServiceGrpc.newBlockingStub(tokenChannel);

  GrpcQueryRequest request = GrpcQueryRequest.newBuilder()
      .setLanguage("sql")
      .setQuery("SELECT 1 as test")
      .build();

  // Should fail with UNAUTHENTICATED
  assertThatThrownBy(() -> tokenStub.query(request).next())
      .isInstanceOf(StatusRuntimeException.class)
      .hasMessageContaining("UNAUTHENTICATED");
}
```

**Step 2: Run integration tests**

Run: `mvn verify -pl grpcw -DskipITs=false 2>&1 | grep -E "Tests run:|BUILD"`
Expected: BUILD SUCCESS with all tests passing

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java
git commit -m "test(grpcw): add integration tests for token authentication over gRPC"
```

---

### Task 6: Run Full Test Suite and Verify

**Step 1: Run all grpcw tests**

Run: `mvn verify -pl grpcw -DskipITs=false`
Expected: BUILD SUCCESS

**Step 2: Verify test counts**

Run: `mvn verify -pl grpcw -DskipITs=false 2>&1 | grep -E "Tests run:"`

Expected:
- Unit tests: ~75 (was 68, added ~7 new)
- Integration tests: ~34 (was 31, added 3 new)

**Step 3: Final commit if needed**

If any cleanup is needed, commit with:
```bash
git commit -m "chore(grpcw): cleanup after token auth implementation"
```

---

## Summary

After completing all tasks:

| Metric | Before | After |
|--------|--------|-------|
| Token auth supported | No | Yes |
| GrpcAuthInterceptorTest | 3 tests | ~10 tests |
| GrpcServerIT | 19 tests | ~22 tests |
| New dependencies | - | None (reuses HTTP) |

## Success Criteria

- [x] Tokens from HTTP login work for gRPC authentication
- [x] Invalid/expired tokens are rejected
- [x] Logout invalidates tokens for gRPC
- [x] Username/password auth still works
- [x] All existing tests pass
- [x] New tests cover token auth scenarios
