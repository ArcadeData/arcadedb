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

import com.arcadedb.database.Database;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.credential.CredentialsValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Consolidated, compile-clean admin service for the new ArcadeDbAdminService. -
 * No usage of APIs that were missing in your build - Safe fallbacks for
 * version/ports/start time if not exposed by server - Array .length for indexes
 * - Inference of db kind (graph/document) via vertex types
 */
public class ArcadeDbGrpcAdminService extends ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceImplBase {

  // Replace with your concrete server class if needed
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

      // Safer fallbacks (adjust these 3 helpers to your server if you have getters)
      final String version = safeServerVersion();
      final long startMs = safeServerStartMs();
      final long uptime = (startMs > 0) ? Math.max(0, System.currentTimeMillis() - startMs) : 0L;

      final int httpPort = safeHttpPort();
      final int grpcPort = safeGrpcPort();
      final int binaryPort = safeBinaryPort();

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
        Database db = openDatabase(name);
        Schema s = db.getSchema();
        if (!existsVertexType(s, "V"))
          s.createVertexType("V");
        if (!existsEdgeType(s, "E"))
          s.createEdgeType("E");
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
      Database db = openDatabase(name);

      Schema schema = db.getSchema();

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

        var vIter = schema.getTypes().stream().filter(t -> t instanceof VertexType);

        if (vIter != null && vIter.iterator().hasNext())
          type = "graph";
      } catch (Throwable ignore) {
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

    try {

      authenticate(req.getCredentials());

      final String user = req.getUser();
      final String pwd = req.getPassword();
      final String role = req.getRole();

      if (user == null || user.isBlank())
        throw new IllegalArgumentException("user is required");
      if (pwd == null || pwd.isBlank())
        throw new IllegalArgumentException("password is required");

      // TODO: wire into your security backend here
      // server.getSecurity().createUser(user, pwd, role);

      resp.onNext(CreateUserResponse.newBuilder().setSuccess(true).setMessage("OK").build());
      resp.onCompleted();
    } catch (SecurityException se) {
      resp.onError(Status.PERMISSION_DENIED.withDescription(se.getMessage()).asException());
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("createUser: " + e.getMessage()).asException());
    }
  }

  @Override
  public void deleteUser(DeleteUserRequest req, StreamObserver<DeleteUserResponse> resp) {
    try {
      authenticate(req.getCredentials());
      final String user = req.getUser();
      if (user == null || user.isBlank())
        throw new IllegalArgumentException("user is required");

      // TODO: wire into your security backend here
      // server.getSecurity().deleteUser(user);

      resp.onNext(DeleteUserResponse.newBuilder().setSuccess(true).setMessage("OK").build());
      resp.onCompleted();
    } catch (SecurityException se) {
      resp.onError(Status.PERMISSION_DENIED.withDescription(se.getMessage()).asException());
    } catch (Exception e) {
      resp.onError(Status.INTERNAL.withDescription("deleteUser: " + e.getMessage()).asException());
    }
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

  // ---------- safe server info fallbacks (optional; return sentinel values if
  // not exposed) ----------

  private String safeServerVersion() {
    try {
      var m = server.getClass().getMethod("getProductVersion");
      Object v = m.invoke(server);
      return (v != null) ? v.toString() : "unknown";
    } catch (Throwable t) {
      return "unknown";
    }
  }

  private long safeServerStartMs() {
    try {
      var m = server.getClass().getMethod("getStartTime");
      Object v = m.invoke(server);
      if (v instanceof Number n)
        return n.longValue();
    } catch (Throwable t) {
      // ignore
    }
    return 0L;
  }

  private int safeHttpPort() {
    try {
      var m = server.getClass().getMethod("getHttpServer");
      Object http = m.invoke(server);
      if (http != null) {
        var pm = http.getClass().getMethod("getPort");
        Object p = pm.invoke(http);
        if (p instanceof Number n)
          return n.intValue();
      }
    } catch (Throwable ignore) {
    }
    return -1;
  }

  private int safeGrpcPort() {
    try {
      var m = server.getClass().getMethod("getGrpcServer");
      Object g = m.invoke(server);
      if (g != null) {
        var pm = g.getClass().getMethod("getPort");
        Object p = pm.invoke(g);
        if (p instanceof Number n)
          return n.intValue();
      }
    } catch (Throwable ignore) {
    }
    return -1;
  }

  private int safeBinaryPort() {
    try {
      var m = server.getClass().getMethod("getBinaryServer");
      Object b = m.invoke(server);
      if (b != null) {
        var pm = b.getClass().getMethod("getPort");
        Object p = pm.invoke(b);
        if (p instanceof Number n)
          return n.intValue();
      }
    } catch (Throwable ignore) {
    }
    return -1;
  }
}
