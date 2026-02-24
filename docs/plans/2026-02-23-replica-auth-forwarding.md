# Replica→Leader Auth Forwarding Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix 401 errors when the Studio (or any client) sends commands to a replica that forwards them to the leader, by replacing per-node session tokens with a shared cluster-internal trust token.

**Architecture:** A shared `HA_CLUSTER_TOKEN` (auto-generated UUID, persisted to raft-storage) is set on all nodes. When a replica forwards a command to the leader, it replaces `Bearer AU-...` session tokens with two internal headers (`X-ArcadeDB-Cluster-Token` + `X-ArcadeDB-Forwarded-User`). The leader validates the cluster token and resolves the user by name, short-circuiting the normal session-lookup auth path.

**Tech Stack:** Java 21, Undertow HTTP server, Apache Ratis (for storage path), JUnit 5, AssertJ.

---

### Task 1: Add `HA_CLUSTER_TOKEN` to GlobalConfiguration

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`
- Test: `engine/src/test/java/com/arcadedb/ConfigurationTest.java`

**Step 1: Write the failing test**

Open `engine/src/test/java/com/arcadedb/ConfigurationTest.java` and add inside the class:

```java
@Test
void haClusterTokenDefaultsToBlank() {
    assertThat(GlobalConfiguration.HA_CLUSTER_TOKEN.getDefValue()).isEqualTo("");
    assertThat(GlobalConfiguration.HA_CLUSTER_TOKEN.getValueAsString()).isEqualTo("");
}
```

**Step 2: Run test to verify it fails**

```bash
mvn test -pl engine -Dtest=ConfigurationTest#haClusterTokenDefaultsToBlank -q
```
Expected: FAIL — `cannot find symbol: variable HA_CLUSTER_TOKEN`

**Step 3: Add the config entry**

In `GlobalConfiguration.java`, find the `// RAFT HA` section (around line 537) and add after `HA_RAFT_PORT`:

```java
HA_CLUSTER_TOKEN("arcadedb.ha.clusterToken", SCOPE.SERVER,
    "Shared secret for inter-node request forwarding authentication. " +
    "Must be identical on all cluster nodes. " +
    "If empty, a random token is auto-generated and stored in raft-storage at startup.",
    String.class, ""),
```

**Step 4: Run test to verify it passes**

```bash
mvn test -pl engine -Dtest=ConfigurationTest#haClusterTokenDefaultsToBlank -q
```
Expected: PASS

**Step 5: Compile the full engine to catch any issues**

```bash
mvn compile -pl engine -q
```

**Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/GlobalConfiguration.java \
        engine/src/test/java/com/arcadedb/ConfigurationTest.java
git commit -m "feat(ha-raft): add HA_CLUSTER_TOKEN configuration property"
```

---

### Task 2: Auto-generate and persist cluster token in RaftHAServer

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerTest.java`

**Background:** `RaftHAServer.start()` already computes `storageDir`. We add an `initClusterToken()` method called at the start of `start()`. It reads `HA_CLUSTER_TOKEN` from config; if blank, checks `<storageDir>/cluster-token.txt`; if that doesn't exist either, generates a `UUID`, writes it to the file, and updates the live config value in-memory via `configuration.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, token)`.

**Step 1: Write the failing tests**

Add to `RaftHAServerTest.java`:

```java
import java.io.File;
import java.nio.file.Files;

@Test
void initClusterTokenGeneratesAndPersistsTokenWhenBlank(@TempDir File tempDir) throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    // HA_CLUSTER_TOKEN starts blank by default

    RaftHAServer.initClusterToken(config, tempDir);

    final String token = config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    assertThat(token).isNotBlank();

    // token must also be in the file
    final File tokenFile = new File(tempDir, "cluster-token.txt");
    assertThat(tokenFile).exists();
    assertThat(Files.readString(tokenFile.toPath()).trim()).isEqualTo(token);
}

@Test
void initClusterTokenReadsExistingFileWhenConfigBlank(@TempDir File tempDir) throws Exception {
    final String existingToken = "my-existing-token";
    Files.writeString(new File(tempDir, "cluster-token.txt").toPath(), existingToken);

    final ContextConfiguration config = new ContextConfiguration();
    RaftHAServer.initClusterToken(config, tempDir);

    assertThat(config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN)).isEqualTo(existingToken);
}

@Test
void initClusterTokenKeepsExplicitConfigValueWithoutTouchingFile(@TempDir File tempDir) throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "explicit-token");

    RaftHAServer.initClusterToken(config, tempDir);

    assertThat(config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN)).isEqualTo("explicit-token");
    assertThat(new File(tempDir, "cluster-token.txt")).doesNotExist();
}
```

Add the necessary import at the top of the test file:
```java
import org.junit.jupiter.api.io.TempDir;
import java.io.File;
import java.nio.file.Files;
```

**Step 2: Run tests to verify they fail**

```bash
mvn test -pl ha-raft -Dtest=RaftHAServerTest#initClusterToken* -q
```
Expected: FAIL — `cannot find symbol: method initClusterToken`

**Step 3: Implement `initClusterToken`**

In `RaftHAServer.java`, add this static method:

```java
/**
 * Initialises the cluster token used for inter-node request forwarding.
 * <ul>
 *   <li>If {@code HA_CLUSTER_TOKEN} is already set in config, nothing changes.</li>
 *   <li>If the token file exists in {@code storageDir}, its value is loaded into config.</li>
 *   <li>Otherwise a new UUID is generated, written to the file, and set in config.</li>
 * </ul>
 */
static void initClusterToken(final ContextConfiguration configuration, final File storageDir) throws IOException {
    final String configured = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (configured != null && !configured.isBlank())
        return; // explicitly configured — use as-is

    final File tokenFile = new File(storageDir, "cluster-token.txt");
    if (tokenFile.exists()) {
        final String persisted = java.nio.file.Files.readString(tokenFile.toPath()).trim();
        configuration.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, persisted);
        return;
    }

    // Generate, persist, and set
    storageDir.mkdirs();
    final String newToken = UUID.randomUUID().toString();
    java.nio.file.Files.writeString(tokenFile.toPath(), newToken);
    configuration.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, newToken);
    LogManager.instance().log(RaftHAServer.class, Level.INFO,
        "Generated new cluster token (saved to %s)", tokenFile.getAbsolutePath());
}
```

Also call it from `start()`, right before the `raftServer = RaftServer.newBuilder()...` block:

```java
initClusterToken(configuration, storageDir);
```

**Step 4: Run tests to verify they pass**

```bash
mvn test -pl ha-raft -Dtest=RaftHAServerTest -q
```
Expected: all tests PASS (was 15, now 18)

**Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerTest.java
git commit -m "feat(ha-raft): auto-generate and persist cluster token at startup"
```

---

### Task 3: Replace session-token auth when forwarding from replica

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/PostCommandHandler.java`
- Create: `server/src/test/java/com/arcadedb/server/http/handler/PostCommandHandlerForwardAuthTest.java`

**Background:** The current `forwardToLeader(leaderHttpAddress, databaseName, jsonBody, authHeader)` passes the original auth header as-is. We need to:
1. Extract a package-private static `buildForwardRequest(...)` that handles auth header selection — this makes it unit-testable without mocking `HttpClient`.
2. `forwardToLeader` gains two additional parameters: `userName` (from the resolved `ServerSecurityUser`) and `clusterToken`.
3. If `authHeader` starts with `"Bearer AU-"`, the request omits `Authorization` and adds `X-ArcadeDB-Cluster-Token` + `X-ArcadeDB-Forwarded-User` instead.
4. Otherwise, the original `Authorization` header is forwarded unchanged (Basic auth, API tokens).

**Step 1: Write the failing tests**

Create `server/src/test/java/com/arcadedb/server/http/handler/PostCommandHandlerForwardAuthTest.java`:

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * ...
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;

import static org.assertj.core.api.Assertions.assertThat;

class PostCommandHandlerForwardAuthTest {

    @Test
    void sessionTokenIsReplacedWithInternalHeaders() {
        final HttpRequest request = PostCommandHandler.buildForwardRequest(
            "leader:2480", "mydb", "{}",
            "Bearer AU-some-uuid", "alice", "cluster-secret"
        ).build();

        assertThat(request.headers().firstValue("Authorization")).isEmpty();
        assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).hasValue("alice");
        assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).hasValue("cluster-secret");
    }

    @Test
    void basicAuthIsForwardedUnchanged() {
        final HttpRequest request = PostCommandHandler.buildForwardRequest(
            "leader:2480", "mydb", "{}",
            "Basic dXNlcjpwYXNz", null, "cluster-secret"
        ).build();

        assertThat(request.headers().firstValue("Authorization")).hasValue("Basic dXNlcjpwYXNz");
        assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).isEmpty();
        assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).isEmpty();
    }

    @Test
    void apiTokenIsForwardedUnchanged() {
        final HttpRequest request = PostCommandHandler.buildForwardRequest(
            "leader:2480", "mydb", "{}",
            "Bearer at-some-api-token", null, "cluster-secret"
        ).build();

        assertThat(request.headers().firstValue("Authorization")).hasValue("Bearer at-some-api-token");
        assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).isEmpty();
        assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).isEmpty();
    }

    @Test
    void nullAuthHeaderProducesNoAuthHeaders() {
        final HttpRequest request = PostCommandHandler.buildForwardRequest(
            "leader:2480", "mydb", "{}",
            null, null, "cluster-secret"
        ).build();

        assertThat(request.headers().firstValue("Authorization")).isEmpty();
        assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).isEmpty();
        assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).isEmpty();
    }
}
```

**Step 2: Run tests to verify they fail**

```bash
mvn test -pl server -Dtest=PostCommandHandlerForwardAuthTest -q
```
Expected: FAIL — `cannot find symbol: method buildForwardRequest`

**Step 3: Refactor `PostCommandHandler`**

Replace the current `forwardToLeader` with the extracted static builder + updated private method:

```java
/**
 * Builds the forwarded HTTP request, substituting internal cluster-auth headers
 * when the original auth is a per-node session token (Bearer AU-...).
 * Package-private for testing.
 */
static HttpRequest.Builder buildForwardRequest(final String leaderHttpAddress, final String databaseName,
    final String jsonBody, final String authHeader, final String userName, final String clusterToken) {

    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://" + leaderHttpAddress + "/api/v1/command/" + databaseName))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(jsonBody));

    if (authHeader != null && authHeader.startsWith("Bearer AU-")) {
        // Per-node session token: replace with cluster-internal identity headers
        if (userName != null)
            builder.header("X-ArcadeDB-Forwarded-User", userName);
        if (clusterToken != null && !clusterToken.isBlank())
            builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
    } else if (authHeader != null) {
        // Basic or API token: stateless, forward as-is
        builder.header("Authorization", authHeader);
    }
    return builder;
}

private ExecutionResponse forwardToLeader(final String leaderHttpAddress, final String databaseName,
    final String jsonBody, final String authHeader, final String userName, final String clusterToken)
    throws IOException {

    final HttpRequest request = buildForwardRequest(leaderHttpAddress, databaseName, jsonBody,
        authHeader, userName, clusterToken).build();
    try {
        final HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        return new ExecutionResponse(response.statusCode(), response.body());
    } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while forwarding command to leader at " + leaderHttpAddress, e);
    }
}
```

Update the call site inside `execute()` to pass `user.getName()` and the cluster token:

```java
if (wrapped instanceof HAReplicatedDatabase haDb && !haDb.isLeader()) {
    final String leaderHttpAddress = haDb.getLeaderHttpAddress();
    if (leaderHttpAddress != null) {
        final HeaderValues authValues = exchange.getRequestHeaders().get("Authorization");
        final String authHeader = authValues != null ? authValues.getFirst() : null;
        final String clusterToken = httpServer.getServer().getConfiguration()
            .getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
        final String userName = user != null ? user.getName() : null;
        return forwardToLeader(leaderHttpAddress, database.getName(), json.toString(),
            authHeader, userName, clusterToken);
    }
}
```

Add the missing import if not already present:
```java
import com.arcadedb.GlobalConfiguration;
```

**Step 4: Run tests to verify they pass**

```bash
mvn test -pl server -Dtest=PostCommandHandlerForwardAuthTest -q
```
Expected: PASS

**Step 5: Run full server tests to check for regressions**

```bash
mvn test -pl server -q
```
Expected: all tests PASS

**Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/PostCommandHandler.java \
        server/src/test/java/com/arcadedb/server/http/handler/PostCommandHandlerForwardAuthTest.java
git commit -m "feat(ha): replace session tokens with cluster-internal auth when forwarding to leader"
```

---

### Task 4: Leader validates internal cluster-auth headers

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java`
- Create: `server/src/test/java/com/arcadedb/server/http/handler/ClusterInternalAuthTest.java`

**Background:** `AbstractServerHttpHandler.handleRequest()` currently starts with the `Authorization` header check. We insert a new block **before** that: if `X-ArcadeDB-Cluster-Token` is present, validate it against `HA_CLUSTER_TOKEN`, look up the user by name, and skip all other auth.

The tests use `BaseGraphServerTest` to spin up a real ArcadeDB server (see existing tests like `PostCommandHandlerDecodeTest`). For cluster-internal auth tests, we configure `HA_CLUSTER_TOKEN` via the server's configuration before the test and hit the HTTP API with the internal headers.

**Step 1: Write failing tests**

Create `server/src/test/java/com/arcadedb/server/http/handler/ClusterInternalAuthTest.java`:

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * ...
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the leader accepts commands forwarded from a replica via
 * the cluster-internal trust-token headers, and rejects invalid tokens.
 */
class ClusterInternalAuthTest extends BaseGraphServerTest {

    private static final String CLUSTER_TOKEN = "test-cluster-secret-token";
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @BeforeEach
    void setClusterToken() {
        getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, CLUSTER_TOKEN);
    }

    @AfterEach
    void clearClusterToken() {
        getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "");
    }

    @Test
    void validClusterTokenWithKnownUserIsAccepted() throws Exception {
        final HttpResponse<String> resp = sendWithInternalHeaders(CLUSTER_TOKEN, "root");
        // Should execute the query successfully (200) or at least not be a 401
        assertThat(resp.statusCode()).isNotEqualTo(401);
        assertThat(resp.statusCode()).isNotEqualTo(403);
    }

    @Test
    void invalidClusterTokenIsRejected() throws Exception {
        final HttpResponse<String> resp = sendWithInternalHeaders("wrong-token", "root");
        assertThat(resp.statusCode()).isEqualTo(401);
    }

    @Test
    void validTokenWithUnknownUserIsRejected() throws Exception {
        final HttpResponse<String> resp = sendWithInternalHeaders(CLUSTER_TOKEN, "no-such-user");
        assertThat(resp.statusCode()).isEqualTo(401);
    }

    @Test
    void clusterTokenHeaderAloneWithoutUserHeaderIsRejected() throws Exception {
        final int port = getServer(0).getHttpServer().getPort();
        final HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/api/v1/command/" + getDatabaseName()))
            .header("Content-Type", "application/json")
            .header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN)
            // no X-ArcadeDB-Forwarded-User
            .POST(HttpRequest.BodyPublishers.ofString(
                "{\"language\":\"sql\",\"command\":\"SELECT 1\"}"))
            .build();
        final HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(401);
    }

    // ---

    private HttpResponse<String> sendWithInternalHeaders(final String clusterToken, final String userName)
        throws Exception {
        final int port = getServer(0).getHttpServer().getPort();
        final HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/api/v1/command/" + getDatabaseName()))
            .header("Content-Type", "application/json")
            .header("X-ArcadeDB-Cluster-Token", clusterToken)
            .header("X-ArcadeDB-Forwarded-User", userName)
            .POST(HttpRequest.BodyPublishers.ofString(
                "{\"language\":\"sql\",\"command\":\"SELECT 1\"}"))
            .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
```

To get the HTTP port, check how existing tests do it. Look for `getServer(0).getHttpServer()` usage — if a `getPort()` method doesn't exist, check `BaseGraphServerTest` and use whatever port-getter is available (typically `DEFAULT_PORT_NUMBER` constant or `getServer(0).getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT)`).

**Step 2: Run tests to verify they fail**

```bash
mvn test -pl server -Dtest=ClusterInternalAuthTest -q
```
Expected: tests for invalid token / missing user FAIL (they get 200 instead of 401) because the validation doesn't exist yet.

**Step 3: Implement cluster-internal auth in `AbstractServerHttpHandler`**

Add a private helper method:

```java
/**
 * Validates cluster-internal forwarded-auth headers.
 * Returns the resolved user if both headers are valid, or null (after sending 401) if not.
 * Called from handleRequest() before normal auth logic.
 */
private ServerSecurityUser validateClusterForwardedAuth(final HttpServerExchange exchange,
    final String providedToken, final HeaderValues forwardedUserValues) {

    final String clusterToken = httpServer.getServer().getConfiguration()
        .getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);

    if (clusterToken.isBlank() || !clusterToken.equals(providedToken)) {
        sendErrorResponse(exchange, 401, "Invalid cluster token", null, null);
        exchange.setStatusCode(401);
        return null;
    }

    if (forwardedUserValues == null || forwardedUserValues.isEmpty()) {
        sendErrorResponse(exchange, 401, "Missing forwarded user", null, null);
        exchange.setStatusCode(401);
        return null;
    }

    final ServerSecurityUser user = httpServer.getServer().getSecurity()
        .getUser(forwardedUserValues.getFirst());
    if (user == null) {
        sendErrorResponse(exchange, 401, "Unknown forwarded user", null, null);
        exchange.setStatusCode(401);
        return null;
    }
    return user;
}
```

In `handleRequest()`, add the cluster-auth check **before** `ServerSecurityUser user = null;`:

```java
// Check cluster-internal forwarded auth (highest priority — bypasses session lookup)
final HeaderValues clusterTokenHeader = exchange.getRequestHeaders().get("X-ArcadeDB-Cluster-Token");
ServerSecurityUser user = null;
if (clusterTokenHeader != null && !clusterTokenHeader.isEmpty()) {
    user = validateClusterForwardedAuth(exchange,
        clusterTokenHeader.getFirst(),
        exchange.getRequestHeaders().get("X-ArcadeDB-Forwarded-User"));
    if (user == null)
        return; // 401 already sent
}
```

Then change the existing `ServerSecurityUser user = null;` (which is now duplicated) — remove the old declaration and wrap the existing auth block in `if (user == null) { ... }`:

```java
if (user == null) {
    final HeaderValues authorization = exchange.getRequestHeaders().get("Authorization");
    if (isRequireAuthentication() && (authorization == null || authorization.isEmpty())) {
        exchange.setStatusCode(401);
        exchange.getResponseHeaders().put(Headers.WWW_AUTHENTICATE, "Basic");
        sendErrorResponse(exchange, 401, "", null, null);
        return;
    }

    if (authorization != null) {
        try {
            // ... existing Basic / Bearer session / Bearer API token logic unchanged ...
        } catch (...) {
            ...
        }
    }
}
```

Add the missing import:
```java
import com.arcadedb.GlobalConfiguration;
```

**Step 4: Run tests to verify they pass**

```bash
mvn test -pl server -Dtest=ClusterInternalAuthTest -q
```
Expected: all 4 tests PASS

**Step 5: Run full server tests to check for regressions**

```bash
mvn test -pl server -q
```
Expected: all tests PASS (was 263, now 267)

**Step 6: Compile ha-raft to make sure nothing is broken there**

```bash
mvn compile -pl ha-raft -q
```

**Step 7: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java \
        server/src/test/java/com/arcadedb/server/http/handler/ClusterInternalAuthTest.java
git commit -m "feat(ha): validate cluster-internal trust-token on leader to authenticate forwarded requests"
```

---

### Task 5: Final verification

**Step 1: Run all affected modules**

```bash
mvn test -pl engine,ha-raft,server -q
```
Expected: all tests PASS across all three modules.

**Step 2: Verify config documentation**

Confirm that `HA_CLUSTER_TOKEN` appears in the configuration listing:
```bash
mvn test -pl engine -Dtest=ConfigurationTest -q
```

**Step 3: Final commit if any cleanup needed**

If any debug output or minor adjustments were made:
```bash
git add -p
git commit -m "chore(ha): cleanup after cluster auth forwarding implementation"
```
