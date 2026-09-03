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

import com.arcadedb.Constants;
import com.arcadedb.database.Database;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.server.security.credential.CredentialsValidator;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * gRPC admin service for server and database administration.
 * Provides server info, database CRUD, and basic management operations.
 */
public class ArcadeDbGrpcAdminService extends ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceImplBase {

  private final ArcadeDBServer       server;
  private final CredentialsValidator credentialsValidator;

  public ArcadeDbGrpcAdminService(final ArcadeDBServer server, CredentialsValidator credentialsValidator) {

    this.server = Objects.requireNonNull(server, "server");
    this.credentialsValidator = Objects.requireNonNull(credentialsValidator, "credentialsValidator");
  }

  // ------------------------------------------------------------------------------------
  // RPCs
  // ------------------------------------------------------------------------------------

  @Override
  public void ping(final PingRequest req, final StreamObserver<PingResponse> resp) {
    respond(resp, "ping", () -> {
      // If you want ping to be open, comment out the next line
      authenticate(req.getCredentials());

      return PingResponse.newBuilder().setOk(true).setServerTimeMs(System.currentTimeMillis()).build();
    });
  }

  @Override
  public void getServerInfo(final GetServerInfoRequest req, final StreamObserver<GetServerInfoResponse> resp) {
    respond(resp, "getServerInfo", () -> {
      authenticate(req.getCredentials());

      final String version = getServerVersion();
      final long startMs = getServerStartMs();
      final long uptime = startMs > 0 ? Math.max(0, System.currentTimeMillis() - startMs) : 0L;

      final int httpPort = getHttpPort();
      final int grpcPort = getGrpcPort();
      final int binaryPort = getBinaryPort();

      final List<String> dbNames = new ArrayList<>(getDatabaseNames());
      final int dbCount = dbNames.size();

      return GetServerInfoResponse.newBuilder().setVersion(version)
          .setEdition("CE") // adjust if you expose edition
          .setStartTimeMs(startMs).setUptimeMs(uptime).setHttpPort(httpPort).setGrpcPort(grpcPort).setBinaryPort(binaryPort)
          .setDatabasesCount(dbCount).build();
    });
  }

  @Override
  public void listDatabases(final ListDatabasesRequest req, final StreamObserver<ListDatabasesResponse> resp) {
    respond(resp, "listDatabases", () -> {
      authenticate(req.getCredentials());

      final ArrayList<String> names = new ArrayList<>(getDatabaseNames());
      names.sort(String.CASE_INSENSITIVE_ORDER);

      return ListDatabasesResponse.newBuilder().addAllDatabases(names).build();
    });
  }

  @Override
  public void existsDatabase(final ExistsDatabaseRequest req, final StreamObserver<ExistsDatabaseResponse> resp) {
    respond(resp, "existsDatabase", () -> {
      authenticate(req.getCredentials());

      final String name = req.getName(); // proto should define 'name' for the DB

      return ExistsDatabaseResponse.newBuilder().setExists(containsDatabaseIgnoreCase(name)).build();
    });
  }

  @Override
  public void createDatabase(final CreateDatabaseRequest req, final StreamObserver<CreateDatabaseResponse> resp) {
    respond(resp, "createDatabase", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      final String name = req.getName(); // DB name in proto
      final String type = req.getType(); // "graph" or "document" (logical)

      if (containsDatabaseIgnoreCase(name))
        return CreateDatabaseResponse.newBuilder().build();

      // Physical creation (READ_WRITE is the common default)
      createDatabasePhysical(name);

      // Optional: if requested 'graph', initialize default graph types
      if ("graph".equalsIgnoreCase(type)) {
        // Use getDatabase which returns a shared ServerDatabase - don't close it
        final Database db = openDatabase(name);
        db.transaction(() -> {
          final Schema s = db.getSchema();
          if (!existsVertexType(s, "V"))
            s.createVertexType("V");
          if (!existsEdgeType(s, "E"))
            s.createEdgeType("E");
        });
      }
      return CreateDatabaseResponse.newBuilder().build();
    });
  }

  @Override
  public void dropDatabase(final DropDatabaseRequest req, final StreamObserver<DropDatabaseResponse> resp) {
    respond(resp, "dropDatabase", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      final String name = req.getName();

      if (containsDatabaseIgnoreCase(name))
        dropDatabasePhysical(name);

      return DropDatabaseResponse.newBuilder().build();
    });
  }

  @Override
  public void getDatabaseInfo(final GetDatabaseInfoRequest req, final StreamObserver<GetDatabaseInfoResponse> resp) {
    respond(resp, "getDatabaseInfo", () -> {
      authenticate(req.getCredentials());

      final String name = req.getName();

      if (!containsDatabaseIgnoreCase(name))
        throw Status.NOT_FOUND.withDescription("Database not found: " + name).asException();

      // Use getDatabase which returns a shared ServerDatabase - don't close it
      final Database db = openDatabase(name);
      if (db == null)
        throw Status.NOT_FOUND.withDescription("Database not found: " + name).asException();

      final Schema schema = db.getSchema();

      // Count classes
      int classes = 0;
      try {
        classes = schema.getTypes().size();
      } catch (Throwable ignore) {
      }

      // Count indexes (Index[] in your build)
      int indexes = 0;
      try {
        Index[] idx = schema.getIndexes();
        indexes = idx != null ? idx.length : 0;
      } catch (Throwable ignore) {
      }

      // Approximate record count (fast-ish; adjust to your needs)
      final long records = approximateRecordCount(db);

      // Infer db kind: "graph" if any vertex type exists
      String type = "document";
      try {
        final boolean hasVertexTypes = schema.getTypes().stream()
            .anyMatch(t -> t instanceof VertexType);
        if (hasVertexTypes)
          type = "graph";
      } catch (Exception e) {
        // Keep default "document" type if schema inspection fails
      }

      return GetDatabaseInfoResponse.newBuilder()
          .setDatabase(name)
          .setClasses(classes).setIndexes(indexes).setRecords(records).setType(type)
          .build();
    });
  }

  @Override
  public void createUser(CreateUserRequest req, StreamObserver<CreateUserResponse> resp) {
    // User management via gRPC is not yet implemented
    // Users should be managed via configuration files or HTTP API
    resp.onError(Status.UNIMPLEMENTED
        .withDescription("User management via gRPC is not yet implemented. Use HTTP API or configuration files.")
        .asException());
  }

  @Override
  public void deleteUser(DeleteUserRequest req, StreamObserver<DeleteUserResponse> resp) {
    // User management via gRPC is not yet implemented
    // Users should be managed via configuration files or HTTP API
    resp.onError(Status.UNIMPLEMENTED
        .withDescription("User management via gRPC is not yet implemented. Use HTTP API or configuration files.")
        .asException());
  }

  // ------------------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------------------

  /**
   * Every unary handler of this service goes through here so the call is terminated exactly once (issue #7035):
   * the same {@code responded} guard {@link ArcadeDbGrpcService} carries inline, applied by {@link GrpcUnaryCall}.
   * {@code operation} prefixes the description of an unexpected failure, as the inline catch blocks used to.
   */
  private static <T> void respond(final StreamObserver<T> resp, final String operation, final GrpcUnaryCall.Body<T> body) {
    GrpcUnaryCall.respond(resp, body, e -> toStatus(operation, e));
  }

  /**
   * Maps a handler failure to the status the client receives. A {@link StatusException} raised by the body (the
   * NOT_FOUND of {@code getDatabaseInfo}) is sent as it is; the authorization exception is checked before the
   * authentication one because it is the more specific outcome, not because of any inheritance between the two.
   */
  private static StatusException toStatus(final String operation, final Exception e) {
    if (e instanceof StatusException se)
      return se;
    if (e instanceof AdminAuthorizationException)
      return Status.PERMISSION_DENIED.withDescription(e.getMessage()).asException();
    if (e instanceof SecurityException)
      return Status.UNAUTHENTICATED.withDescription(e.getMessage()).asException();
    return Status.INTERNAL.withDescription(operation + ": " + e.getMessage()).asException();
  }

  // Defense-in-depth: GrpcAuthInterceptor already authenticates these body credentials centrally
  // before the call reaches this handler. This handler-side check is intentionally kept (do not
  // remove it assuming the interceptor covers it) so the service stays safe even if the central
  // gate is ever bypassed or reconfigured.
  private ServerSecurityUser authenticate(DatabaseCredentials creds) {

    if (creds == null)
      throw new SecurityException("Authentication required");
    final String user = creds.getUsername(); // matches your proto (not getUser())
    final String pass = creds.getPassword();

    if (user == null || user.isBlank())
      throw new SecurityException("Authentication required");

    // Validate format first
    credentialsValidator.validateCredentials(user, pass);

    // Then authenticate against server security. Fail closed: treat a null result the same as an
    // authentication failure so callers never proceed (or reach the role check) unauthenticated.
    try {
      final ServerSecurityUser authenticatedUser = server.getSecurity().authenticate(user, pass, null);
      if (authenticatedUser == null)
        throw new SecurityException("Invalid credentials");
      return authenticatedUser;
    } catch (ServerSecurityException e) {
      throw new SecurityException("Invalid credentials");
    }
  }

  /**
   * Ensures the authenticated caller holds the server-admin (root) role before running a mutating
   * admin operation such as creating or dropping a database. Authentication (via {@link #authenticate})
   * proves identity only; without this gate any valid account could create or drop any database.
   * Mirrors the HTTP {@code PostServerCommandHandler} which restricts server administration to root.
   */
  private void requireServerAdmin(final ServerSecurityUser user) {
    if (user == null || !"root".equals(user.getName()))
      throw new AdminAuthorizationException("User is not authorized to execute server administration commands");
  }

  /**
   * Raised when an authenticated caller lacks the server-admin role. Mapped to
   * {@code Status.PERMISSION_DENIED} (the caller is authenticated but not authorized), distinct from
   * the {@code UNAUTHENTICATED} used for authentication failures.
   */
  private static final class AdminAuthorizationException extends RuntimeException {
    AdminAuthorizationException(final String message) {
      super(message);
    }
  }

  /**
   * Get DB names from the server.
   */
  private Collection<String> getDatabaseNames() {
    return server.getDatabaseNames();
  }

  private boolean containsDatabaseIgnoreCase(String name) {
    for (String n : getDatabaseNames()) {
      if (n.equalsIgnoreCase(name))
        return true;
    }
    return false;
  }

  /**
   * Create DB physically with READ_WRITE mode.
   */
  private void createDatabasePhysical(final String name) {
    server.createDatabase(name, ComponentFile.MODE.READ_WRITE);
  }

  /**
   * Drop DB physically. Gets the database, drops it via embedded, then removes from server cache.
   */
  private void dropDatabasePhysical(final String name) {
    final ServerDatabase database = server.getDatabase(name);
    database.getEmbedded().drop();
    server.removeDatabase(database.getName());
  }

  /**
   * Open database for read ops.
   */
  private Database openDatabase(final String name) {
    return server.getDatabase(name);
  }

  /**
   * Approximate record count with a quick pass across types.
   */
  private long approximateRecordCount(Database db) {
    long total = 0L;
    try {
      for (DocumentType t : db.getSchema().getTypes()) {
        try {
          // exact=false when supported; otherwise this counts exactly
          total += db.countType(t.getName(), false);
        } catch (Throwable ignore) {
        }
      }
    } catch (Throwable ignore) {
    }
    return total;
  }

  private boolean existsVertexType(Schema s, String name) {

    try {
      return s.existsType(name);
    } catch (Throwable t) {
      return false;
    }
  }

  private boolean existsEdgeType(Schema s, String name) {

    try {

      return s.existsType(name);
    } catch (Throwable t) {
      return false;
    }
  }

  // ---------- Server info helpers using direct API calls ----------

  private String getServerVersion() {
    return Constants.getVersion();
  }

  private long getServerStartMs() {
    // ArcadeDBServer does not expose start time directly
    // Return 0 to indicate "not available"
    return 0L;
  }

  private int getHttpPort() {
    final HttpServer httpServer = server.getHttpServer();
    return httpServer != null ? httpServer.getPort() : -1;
  }

  private int getGrpcPort() {
    // Find the GrpcServerPlugin in the registered plugins
    for (final ServerPlugin plugin : server.getPlugins()) {
      if (plugin instanceof GrpcServerPlugin grpcPlugin) {
        final GrpcServerPlugin.ServerStatus status = grpcPlugin.getStatus();
        return status.standardPort;
      }
    }
    return -1;
  }

  private int getBinaryPort() {
    // ArcadeDB does not have a separate binary server plugin
    // Binary communication is part of HA (High Availability) infrastructure
    return -1;
  }
}
