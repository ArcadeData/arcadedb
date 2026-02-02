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
import com.arcadedb.server.security.credential.CredentialsValidator;
import io.grpc.Status;
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
  public void ping(PingRequest req, StreamObserver<PingResponse> resp) {
    try {
      // If you want ping to be open, comment out the next line
      authenticate(req.getCredentials());

      PingResponse out = PingResponse.newBuilder().setOk(true).setServerTimeMs(System.currentTimeMillis()).build();
      resp.onNext(out);
      resp.onCompleted();
    } catch (SecurityException se) {
      resp.onError(Status.UNAUTHENTICATED.withDescription(se.getMessage()).asException());
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("ping: " + e.getMessage()).asException());
    }
  }

  @Override
  public void getServerInfo(GetServerInfoRequest req, StreamObserver<GetServerInfoResponse> resp) {
    try {
      authenticate(req.getCredentials());

      final String version = getServerVersion();
      final long startMs = getServerStartMs();
      final long uptime = (startMs > 0) ? Math.max(0, System.currentTimeMillis() - startMs) : 0L;

      final int httpPort = getHttpPort();
      final int grpcPort = getGrpcPort();
      final int binaryPort = getBinaryPort();

      final List<String> dbNames = new ArrayList<>(getDatabaseNames());
      int dbCount = dbNames.size();

      GetServerInfoResponse out = GetServerInfoResponse.newBuilder().setVersion(version)
          .setEdition("CE") // adjust if you expose edition
          .setStartTimeMs(startMs).setUptimeMs(uptime).setHttpPort(httpPort).setGrpcPort(grpcPort).setBinaryPort(binaryPort)
          .setDatabasesCount(dbCount).build();

      resp.onNext(out);
      resp.onCompleted();
    } catch (SecurityException se) {
      resp.onError(Status.UNAUTHENTICATED.withDescription(se.getMessage()).asException());
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("getServerInfo: " + e.getMessage()).asException());
    }
  }

  @Override
  public void listDatabases(ListDatabasesRequest req, StreamObserver<ListDatabasesResponse> resp) {

    try {

      authenticate(req.getCredentials());

      ArrayList<String> names = new ArrayList<>(getDatabaseNames());
      names.sort(String.CASE_INSENSITIVE_ORDER);

      resp.onNext(ListDatabasesResponse.newBuilder().addAllDatabases(names).build());
      resp.onCompleted();
    } catch (SecurityException se) {
      resp.onError(Status.UNAUTHENTICATED.withDescription(se.getMessage()).asException());
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("listDatabases: " + e.getMessage()).asException());
    }
  }

  @Override
  public void existsDatabase(ExistsDatabaseRequest req, StreamObserver<ExistsDatabaseResponse> resp) {

    try {

      authenticate(req.getCredentials());

      final String name = req.getName(); // proto should define 'name' for the DB

      boolean exists = containsDatabaseIgnoreCase(name);

      resp.onNext(ExistsDatabaseResponse.newBuilder().setExists(exists).build());
      resp.onCompleted();
    } catch (SecurityException se) {
      resp.onError(Status.UNAUTHENTICATED.withDescription(se.getMessage()).asException());
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("existsDatabase: " + e.getMessage()).asException());
    }
  }

  @Override
  public void createDatabase(CreateDatabaseRequest req, StreamObserver<CreateDatabaseResponse> resp) {

    try {
      authenticate(req.getCredentials());

      final String name = req.getName(); // DB name in proto
      final String type = req.getType(); // "graph" or "document" (logical)

      if (containsDatabaseIgnoreCase(name)) {

        resp.onNext(CreateDatabaseResponse.newBuilder().build());
        resp.onCompleted();
        return;
      }

      // Physical creation (READ_WRITE is the common default)
      createDatabasePhysical(name);

      // Optional: if requested 'graph', initialize default graph types
      if ("graph".equalsIgnoreCase(type)) {
        // Use getDatabase which returns a shared ServerDatabase - don't close it
        try (Database db = openDatabase(name)) {
          db.transaction(() -> {
            Schema s = db.getSchema();
            if (!existsVertexType(s, "V"))
              s.createVertexType("V");
            if (!existsEdgeType(s, "E"))
              s.createEdgeType("E");
          });
        }
      }
      resp.onNext(CreateDatabaseResponse.newBuilder().build());
      resp.onCompleted();
    } catch (SecurityException se) {
      resp.onError(Status.UNAUTHENTICATED.withDescription(se.getMessage()).asException());
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("createDatabase: " + e.getMessage()).asException());
    }
  }

  @Override
  public void dropDatabase(DropDatabaseRequest req, StreamObserver<DropDatabaseResponse> resp) {

    try {

      authenticate(req.getCredentials());

      final String name = req.getName();

      if (!containsDatabaseIgnoreCase(name)) {
        resp.onNext(DropDatabaseResponse.newBuilder().build());
        resp.onCompleted();
        return;
      }

      dropDatabasePhysical(name);

      resp.onNext(DropDatabaseResponse.newBuilder().build());
      resp.onCompleted();
    } catch (SecurityException se) {
      resp.onError(Status.UNAUTHENTICATED.withDescription(se.getMessage()).asException());
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("dropDatabase: " + e.getMessage()).asException());
    }
  }

  @Override
  public void getDatabaseInfo(GetDatabaseInfoRequest req, StreamObserver<GetDatabaseInfoResponse> resp) {

    try {

      authenticate(req.getCredentials());

      final String name = req.getName();

      if (!containsDatabaseIgnoreCase(name)) {
        resp.onError(Status.NOT_FOUND.withDescription("Database not found: " + name).asException());
        return;
      }

      // Use getDatabase which returns a shared ServerDatabase - don't close it
      final Database db = openDatabase(name);
      if (db == null) {
        resp.onError(Status.NOT_FOUND.withDescription("Database not found: " + name).asException());
        return;
      }

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
        indexes = (idx != null) ? idx.length : 0;
      } catch (Throwable ignore) {
      }

      // Approximate record count (fast-ish; adjust to your needs)
      long records = approximateRecordCount(db);

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

      GetDatabaseInfoResponse out = GetDatabaseInfoResponse.newBuilder()
          .setDatabase(name)
          .setClasses(classes).setIndexes(indexes).setRecords(records).setType(type)
          .build();

      resp.onNext(out);
      resp.onCompleted();
    } catch (SecurityException se) {
      resp.onError(Status.UNAUTHENTICATED.withDescription(se.getMessage()).asException());
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("getDatabaseInfo: " + e.getMessage()).asException());
    }
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

  private void authenticate(DatabaseCredentials creds) {

    if (creds == null)
      throw new SecurityException("Authentication required");
    final String user = creds.getUsername(); // matches your proto (not getUser())
    final String pass = creds.getPassword();

    if (user == null || user.isBlank())
      throw new SecurityException("Authentication required");

    // Validate format first
    credentialsValidator.validateCredentials(user, pass);

    // Then authenticate against server security
    try {
      server.getSecurity().authenticate(user, pass, null);
    } catch (ServerSecurityException e) {
      throw new SecurityException("Invalid credentials");
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
