# gRPC Token Authentication Design

## Overview

Enable gRPC clients to authenticate using tokens obtained from the HTTP `/api/v1/auth/login` endpoint, reusing the existing `HttpAuthSessionManager` infrastructure.

## Architecture

```
┌─────────────────┐     POST /api/v1/auth/login     ┌─────────────────┐
│   gRPC Client   │ ─────────────────────────────▶  │   HTTP Server   │
│                 │     { username, password }      │                 │
│                 │  ◀───────────────────────────── │                 │
│                 │     { token: "AU-xxx..." }      │                 │
└────────┬────────┘                                 └────────┬────────┘
         │                                                   │
         │  gRPC call with                                   │
         │  Authorization: Bearer AU-xxx...                  │
         ▼                                                   ▼
┌─────────────────┐                              ┌─────────────────────┐
│ GrpcAuthInter-  │  getSessionByToken(token)    │ HttpAuthSession-    │
│   ceptor        │ ────────────────────────────▶│     Manager         │
│                 │  ◀─ HttpAuthSession (user)   │  (shared instance)  │
└─────────────────┘                              └─────────────────────┘
```

**Key points:**
- No new token system - reuses existing `HttpAuthSessionManager`
- Tokens have the same format (`AU-xxx`) and timeouts as HTTP
- Single source of truth for sessions across both protocols
- Clients login once via HTTP, use token for both HTTP and gRPC

**Access path:**
```java
GrpcAuthInterceptor
  → ArcadeDBServer.getHttpServer()
    → HttpServer.getAuthSessionManager()
      → HttpAuthSessionManager.getSessionByToken(token)
```

## Implementation Changes

### 1. GrpcAuthInterceptor.java - Add session manager access

```java
// New field
private final HttpAuthSessionManager authSessionManager;

// Updated constructor
public GrpcAuthInterceptor(ServerSecurity security, HttpAuthSessionManager authSessionManager) {
  this.security = security;
  this.authSessionManager = authSessionManager;
  this.securityEnabled = (security != null && ...);
}
```

### 2. validateToken() - Implement actual validation

```java
private boolean validateToken(String token, String database) {
  if (authSessionManager == null)
    return false;

  HttpAuthSession session = authSessionManager.getSessionByToken(token);
  if (session == null) {
    LogManager.instance().log(this, Level.FINE,
        "Invalid or expired token for database: %s", database);
    return false;
  }
  return true;
}
```

### 3. extractUserFromToken() - Get user from session

```java
private String extractUserFromToken(String token) {
  // Strip "Bearer " prefix if present
  String cleanToken = token.startsWith(BEARER_TYPE)
      ? token.substring(BEARER_TYPE.length()).trim()
      : token;

  HttpAuthSession session = authSessionManager.getSessionByToken(cleanToken);
  return session != null ? session.getUser().getName() : null;
}
```

### 4. GrpcServerPlugin.java - Pass session manager to interceptor

```java
// In startService() where interceptor is created:
HttpAuthSessionManager authSessionManager = arcadeServer.getHttpServer() != null
    ? arcadeServer.getHttpServer().getAuthSessionManager()
    : null;

GrpcAuthInterceptor authInterceptor = new GrpcAuthInterceptor(security, authSessionManager);
```

## Edge Cases & Error Handling

### Scenarios

| Scenario | Behavior |
|----------|----------|
| HTTP Server not available | Token auth rejected, username/password auth still works |
| Token expired | Returns `UNAUTHENTICATED` with "Invalid or expired token" |
| Session removed (logout) | Active streams continue; new requests fail |
| Database authorization | Checked separately via `ServerSecurity.authenticate()` |
| Null/empty token | Falls through to username/password auth |

### Error Responses (gRPC Status codes)

| Scenario | Status | Description |
|----------|--------|-------------|
| No auth provided | `UNAUTHENTICATED` | "Authentication required" |
| Invalid/expired token | `UNAUTHENTICATED` | "Invalid or expired token" |
| Invalid credentials | `UNAUTHENTICATED` | "Invalid credentials" |
| Auth system error | `INTERNAL` | "Authentication error" |

## Testing Strategy

### Unit Tests - GrpcAuthInterceptorTest.java

- `tokenAuthWithValidToken` - Mock session manager, verify request proceeds
- `tokenAuthWithExpiredToken` - Mock null return, verify UNAUTHENTICATED
- `tokenAuthWhenSessionManagerNull` - Verify graceful fallback
- `extractUserFromTokenReturnsCorrectUsername` - Verify user extraction

### Integration Tests - GrpcServerIT.java

- `authenticateWithHttpTokenOverGrpc` - Full login flow via HTTP, then gRPC call
- `tokenRejectedAfterLogout` - Verify logout invalidates token for gRPC
- `expiredTokenRejected` - Short timeout, verify expiry works

## Scope

**What we're building:**
- gRPC token authentication using existing HTTP session infrastructure
- ~50-80 lines of implementation changes
- ~150-200 lines of new tests

**Files to modify:**

| File | Changes |
|------|---------|
| `GrpcAuthInterceptor.java` | Add `authSessionManager` field, update constructor, implement methods |
| `GrpcServerPlugin.java` | Pass `authSessionManager` to interceptor constructor |
| `GrpcAuthInterceptorTest.java` | Add ~5 unit tests for token auth |
| `GrpcServerIT.java` | Add ~3-4 integration tests with HTTP login |

**What we're NOT building:**
- No new login RPC for gRPC (use HTTP)
- No JWT/OAuth2 (uses simple session tokens)
- No new session manager (reuses HTTP's)
- No gRPC-specific logout (use HTTP)

**Backward compatible:**
- Existing username/password auth unchanged
- Token auth is additive
