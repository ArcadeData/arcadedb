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
import com.arcadedb.engine.OperationProgress;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.server.security.credential.CredentialsValidator;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
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
  /**
   * The transport-independent control plane, shared with the HTTP {@code POST /api/v1/server}
   * handler so the two protocols run one implementation of each administrative operation rather than
   * two that can drift (issue #7304).
   */
  private final ServerControlPlane   controlPlane;

  public ArcadeDbGrpcAdminService(final ArcadeDBServer server, CredentialsValidator credentialsValidator) {

    this.server = Objects.requireNonNull(server, "server");
    this.credentialsValidator = Objects.requireNonNull(credentialsValidator, "credentialsValidator");
    this.controlPlane = new ServerControlPlane(this.server);
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
      final ServerSecurityUser user = authenticate(req.getCredentials());

      final String version = getServerVersion();
      final long startMs = getServerStartMs();
      final long uptime = startMs > 0 ? Math.max(0, System.currentTimeMillis() - startMs) : 0L;

      final int httpPort = getHttpPort();
      final int grpcPort = getGrpcPort();
      final int binaryPort = getBinaryPort();

      // Counted over the databases this caller may access, as GET /api/v1/server reports them
      // (issue #7304): the total would otherwise tell an unprivileged caller how many databases it
      // cannot see.
      final int dbCount = controlPlane.listAuthorizedDatabases(user).size();

      return GetServerInfoResponse.newBuilder().setVersion(version)
          .setEdition("CE") // adjust if you expose edition
          .setStartTimeMs(startMs).setUptimeMs(uptime).setHttpPort(httpPort).setGrpcPort(grpcPort).setBinaryPort(binaryPort)
          .setDatabasesCount(dbCount).build();
    });
  }

  /**
   * Lists the databases the caller is allowed to see. Listing is the one control-plane read that is
   * not root-only, so the answer is narrowed to the caller instead - the same filter
   * {@code list databases} and {@code GET /api/v1/databases} apply over HTTP. Before issue #7304
   * this RPC answered every authenticated caller with every database name on the server.
   */
  @Override
  public void listDatabases(final ListDatabasesRequest req, final StreamObserver<ListDatabasesResponse> resp) {
    respond(resp, "listDatabases", () -> {
      final ServerSecurityUser user = authenticate(req.getCredentials());

      final ArrayList<String> names = new ArrayList<>(controlPlane.listAuthorizedDatabases(user));
      names.sort(String.CASE_INSENSITIVE_ORDER);

      return ListDatabasesResponse.newBuilder().addAllDatabases(names).build();
    });
  }

  /**
   * Whether the named database exists <i>and the caller may access it</i>. Both conjuncts, because
   * that is the predicate {@code GET /api/v1/exists/{database}} applies: {@code GetExistsDatabaseHandler}
   * skips the batch {@code filterAuthorizedDatabases} helper only to avoid building a whole authorized
   * set to answer one yes/no, and evaluates {@code canAccessToDatabase} for the single name instead.
   * Without the second conjunct this RPC lets any account enumerate the names of databases it has no
   * grant on, which is the disclosure {@link #listDatabases} and {@link #getDatabaseInfo} were narrowed
   * to close.
   */
  @Override
  public void existsDatabase(final ExistsDatabaseRequest req, final StreamObserver<ExistsDatabaseResponse> resp) {
    respond(resp, "existsDatabase", () -> {
      final ServerSecurityUser user = authenticate(req.getCredentials());

      final String name = req.getName(); // proto should define 'name' for the DB
      // Exact name, like createDatabase / dropDatabase (issue #7413): what this reports as existing is what a
      // create of the same name would refuse.
      final boolean exists = server.existsDatabase(name) && (user == null || user.canAccessToDatabase(name));

      return ExistsDatabaseResponse.newBuilder().setExists(exists).build();
    });
  }

  @Override
  public void createDatabase(final CreateDatabaseRequest req, final StreamObserver<CreateDatabaseResponse> resp) {
    respond(resp, "createDatabase", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));
      requireLeader("CreateDatabase");

      final String name = req.getName(); // DB name in proto
      final String type = req.getType(); // "graph" or "document" (logical)

      // Exact name, the registry's and the HTTP command's notion of "the same database": the case-folded guard
      // this used to be let CreateDatabase("MyDb") answer OK for an existing "mydb" and then DropDatabase("MyDb")
      // fail inside the drop (issue #7413). An empty OK told a provisioning tool nothing either way; now the
      // strict default refuses a taken name as HTTP does, and the idempotent form says which happened.
      if (server.existsDatabase(name)) {
        if (req.getIfNotExists())
          return CreateDatabaseResponse.newBuilder().setCreated(false).build();
        throw new ServerControlPlane.AlreadyExistsException("Database '" + name + "' already exists");
      }

      // Physical creation (READ_WRITE is the common default)
      final Database db = createDatabasePhysical(name);

      // Optional: if requested 'graph', initialize default graph types
      if ("graph".equalsIgnoreCase(type)) {
        // The shared ServerDatabase the create returned - don't close it
        db.transaction(() -> {
          final Schema s = db.getSchema();
          if (!existsVertexType(s, "V"))
            s.createVertexType("V");
          if (!existsEdgeType(s, "E"))
            s.createEdgeType("E");
        });
      }
      return CreateDatabaseResponse.newBuilder().setCreated(true).build();
    });
  }

  @Override
  public void dropDatabase(final DropDatabaseRequest req, final StreamObserver<DropDatabaseResponse> resp) {
    respond(resp, "dropDatabase", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));
      requireLeader("DropDatabase");

      final String name = req.getName();

      // See createDatabase: exact name, strict by default, explicit outcome either way (issue #7413).
      if (!server.existsDatabase(name)) {
        if (req.getIfExists())
          return DropDatabaseResponse.newBuilder().setDropped(false).build();
        throw new ServerControlPlane.NotFoundException("Database '" + name + "' does not exist");
      }

      dropDatabasePhysical(name);

      return DropDatabaseResponse.newBuilder().setDropped(true).build();
    });
  }

  @Override
  public void getDatabaseInfo(final GetDatabaseInfoRequest req, final StreamObserver<GetDatabaseInfoResponse> resp) {
    respond(resp, "getDatabaseInfo", () -> {
      final ServerSecurityUser user = authenticate(req.getCredentials());

      final String name = req.getName();

      // Schema shape and record counts are database content, so the caller has to be granted the
      // database - the same rule that decides whether GET /api/v1/server reports it (issue #7304).
      // The answer is NOT_FOUND rather than PERMISSION_DENIED so it is the one an unauthorized
      // caller already gets for a name that does not exist.
      if (user != null && !user.canAccessToDatabase(name))
        throw Status.NOT_FOUND.withDescription("Database not found: " + name).asException();

      if (!server.existsDatabase(name))
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

  /**
   * The long-running maintenance operations this server is running for one database, the RPC equivalent
   * of {@code GET /api/v1/progress/{database}} (issue #7310). Answered from the lock-free progress
   * registry, so a client may poll it as often as it likes without touching the database or the operation
   * it is watching.
   * <p>
   * Gated like its HTTP counterpart and not like the rest of this service: {@code GetProgressHandler} runs
   * {@code checkAuthorizationOnDatabase}, so any authenticated account may poll a database it is granted.
   * Making it root-only would be a stricter gate than HTTP applies and would put the progress of an
   * operation out of reach of the very user who started it.
   * <p>
   * An unauthorized caller is refused rather than answered empty. Unlike {@link #getDatabaseInfo}, which
   * hides existence behind NOT_FOUND, the progress registry discloses nothing about a database that is
   * not running an operation - the empty answer is the same for a database that does not exist - so the
   * refusal costs no secrecy and matches the 403 the HTTP route returns.
   */
  @Override
  public void getProgress(final GetProgressRequest req, final StreamObserver<GetProgressResponse> resp) {
    respond(resp, "getProgress", () -> {
      final ServerSecurityUser user = authenticate(req.getCredentials());

      final String database = req.getDatabase();

      // ORDER MATTERS, and it is the order checkAuthorizationOnDatabase applies: a missing name is
      // rejected BEFORE authorization is evaluated. Reversed, an empty name would be answered by
      // whatever canAccessToDatabase("") happens to return - PERMISSION_DENIED for a scoped account,
      // and for a wildcard-granted one a fall-through to the registry - instead of the INVALID_ARGUMENT
      // that says what the caller actually got wrong.
      if (database.isEmpty())
        throw new IllegalArgumentException("Database parameter is null");

      // This reproduces the ACCESS-CONTROL half of checkAuthorizationOnDatabase and not its other half,
      // which binds the authenticated principal onto the database's DatabaseContext so the engine's
      // per-type ACL layer enforces (GHSA-c23x-pqcj-7hfm). Safe here, and only here, because progress is
      // answered from the OperationProgressRegistry: no database is opened, no record or type is read, so
      // there is no per-type decision for a bound principal to inform. DO NOT copy this shape into a gRPC
      // handler that touches data - that handler needs the binding too, or it reopens that advisory.
      //
      // The null arm is unreachable today - authenticate() either returns a user or throws - and is kept
      // deliberately, as the same guard in getDatabaseInfo is: it is the shape checkAuthorizationOnDatabase
      // has on the HTTP side, where a null user means an unauthenticated handler, and it keeps this check
      // fail-safe rather than fail-open if authenticate() ever grows a permissive mode.
      if (user != null && !user.canAccessToDatabase(database))
        throw new ServerSecurityException(
            "User '" + user.getName() + "' is not allowed to access database '" + database + "'");

      final GetProgressResponse.Builder builder = GetProgressResponse.newBuilder();
      for (final OperationProgress operation : controlPlane.getProgress(database))
        builder.addOperations(toProgressInfo(operation));
      return builder.build();
    });
  }

  /**
   * Creates a server user with the per-database groups the request carries, which is the same document
   * the HTTP {@code create user} command takes. The deprecated {@code role} field is ignored: the
   * security model has no server-wide role, only groups held per database.
   */
  @Override
  public void createUser(final CreateUserRequest req, final StreamObserver<CreateUserResponse> resp) {
    respond(resp, "createUser", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));
      requireLeader("CreateUser");

      final JSONObject user = new JSONObject().put("name", req.getUser()).put("password", req.getPassword());
      if (!req.getDatabasesMap().isEmpty()) {
        final JSONObject databases = new JSONObject();
        req.getDatabasesMap().forEach((database, groups) -> databases.put(database, new JSONArray(groups.getGroupsList())));
        user.put("databases", databases);
      }
      controlPlane.createUser(user);

      return CreateUserResponse.newBuilder().setSuccess(true).setMessage("User '" + req.getUser() + "' created").build();
    });
  }

  @Override
  public void deleteUser(final DeleteUserRequest req, final StreamObserver<DeleteUserResponse> resp) {
    respond(resp, "deleteUser", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));
      requireLeader("DeleteUser");

      controlPlane.dropUser(req.getUser());

      return DeleteUserResponse.newBuilder().setSuccess(true).setMessage("User '" + req.getUser() + "' dropped").build();
    });
  }

  @Override
  public void listUsers(final ListUsersRequest req, final StreamObserver<ListUsersResponse> resp) {
    respond(resp, "listUsers", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      final ListUsersResponse.Builder builder = ListUsersResponse.newBuilder();
      final JSONArray users = controlPlane.listUsers();
      for (int i = 0; i < users.length(); i++) {
        final JSONObject user = users.getJSONObject(i);
        final UserInfo.Builder info = UserInfo.newBuilder().setName(user.getString("name"));

        // The grants are a database-name -> group-name-array map. An entry of any other shape is
        // skipped rather than raised: GetUsersHandler copies this document verbatim without looking
        // inside it, so a deployment carrying something unexpected here still reads its user list
        // over HTTP, and must over gRPC too.
        final JSONObject databases = user.getJSONObject("databases");
        for (final String database : databases.keySet()) {
          if (!(databases.get(database) instanceof final JSONArray groupNames))
            continue;
          final UserGroups.Builder groups = UserGroups.newBuilder();
          for (int g = 0; g < groupNames.length(); g++)
            groups.addGroups(String.valueOf(groupNames.get(g)));
          info.putDatabases(database, groups.build());
        }
        builder.addUsers(info.build());
      }
      return builder.build();
    });
  }

  /**
   * Updates an existing user, as {@code PUT /server/users} does. Both mutable fields carry explicit
   * presence in the proto, so "change the password and leave the grants alone" is expressible - which
   * is the whole reason this is a separate RPC rather than a re-run of {@code CreateUser}.
   * <p>
   * Leader-gated for the same reason {@code CreateUser} is: the update goes through
   * {@code updateUserClusterWide}, which on an HA cluster submits a Raft entry.
   */
  @Override
  public void updateUser(final UpdateUserRequest req, final StreamObserver<UpdateUserResponse> resp) {
    respond(resp, "updateUser", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));
      requireLeader("UpdateUser");

      JSONObject databases = null;
      if (req.hasDatabases()) {
        databases = new JSONObject();
        for (final var entry : req.getDatabases().getDatabasesMap().entrySet())
          databases.put(entry.getKey(), new JSONArray(entry.getValue().getGroupsList()));
      }

      controlPlane.updateUser(req.getUser(), req.hasPassword() ? req.getPassword() : null, databases);

      return UpdateUserResponse.newBuilder().setSuccess(true)
          .setMessage("User '" + req.getUser() + "' updated").build();
    });
  }

  // ------------------------------------------------------------------------------------
  // Groups
  // ------------------------------------------------------------------------------------

  /**
   * The group document. Not leader-gated: {@code ServerSecurity} writes groups to a node-local file
   * and submits no Raft entry, so there is no leader for this state to have - a divergence from the
   * user document that is tracked as issue #7373, not compensated for here.
   */
  @Override
  public void listGroups(final ListGroupsRequest req, final StreamObserver<ListGroupsResponse> resp) {
    respond(resp, "listGroups", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      return ListGroupsResponse.newBuilder().setGroupsJson(controlPlane.listGroups().toString()).build();
    });
  }

  @Override
  public void saveGroup(final SaveGroupRequest req, final StreamObserver<SaveGroupResponse> resp) {
    respond(resp, "saveGroup", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.saveGroup(req.getDatabase(), req.getName(), parseDocument(req.getGroupJson(), "group_json"));

      return SaveGroupResponse.newBuilder().setSuccess(true)
          .setMessage("Group '" + req.getName() + "' saved for database '" + req.getDatabase() + "'").build();
    });
  }

  @Override
  public void deleteGroup(final DeleteGroupRequest req, final StreamObserver<DeleteGroupResponse> resp) {
    respond(resp, "deleteGroup", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.deleteGroup(req.getDatabase(), req.getName());

      return DeleteGroupResponse.newBuilder().setSuccess(true)
          .setMessage("Group '" + req.getName() + "' deleted from database '" + req.getDatabase() + "'").build();
    });
  }

  // ------------------------------------------------------------------------------------
  // API tokens
  // ------------------------------------------------------------------------------------

  @Override
  public void listApiTokens(final ListApiTokensRequest req, final StreamObserver<ListApiTokensResponse> resp) {
    respond(resp, "listApiTokens", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      final ListApiTokensResponse.Builder builder = ListApiTokensResponse.newBuilder();
      final JSONArray tokens = controlPlane.listApiTokens();
      for (int i = 0; i < tokens.length(); i++)
        builder.addTokens(toApiTokenInfo(tokens.getJSONObject(i)));

      return builder.build();
    });
  }

  /**
   * Mints an API token. The one RPC on this service whose response carries secret material, and the
   * only one with a transport precondition on top of the usual authentication and authorization: the
   * plaintext token is written back only over TLS or to a loopback peer.
   * <p>
   * The refusal is {@code FAILED_PRECONDITION} rather than {@code PERMISSION_DENIED} because the
   * caller is not the problem - a root credential is exactly right, and the same call over a TLS
   * channel succeeds. What is wrong is the connection it arrived on, which is the caller's to fix by
   * reconnecting, not an authorization decision to appeal.
   */
  @Override
  public void createApiToken(final CreateApiTokenRequest req, final StreamObserver<CreateApiTokenResponse> resp) {
    respond(resp, "createApiToken", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));
      requireTransportSafeForSecrets();

      final JSONObject token = controlPlane.createApiToken(req.getName(), req.getDatabase(), req.getExpiresAt(),
          parseDocument(req.getPermissionsJson(), "permissions_json"));

      // getString with no default would raise on an absent key; the plaintext token is the one field
      // createToken always sets, and reading it defensively would hide its absence rather than report
      // it, so it is read strictly.
      return CreateApiTokenResponse.newBuilder()
          .setToken(token.getString("token"))
          .setInfo(toApiTokenInfo(token))
          .build();
    });
  }

  @Override
  public void deleteApiToken(final DeleteApiTokenRequest req, final StreamObserver<DeleteApiTokenResponse> resp) {
    respond(resp, "deleteApiToken", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.deleteApiToken(req.getTokenHash());

      return DeleteApiTokenResponse.newBuilder().setSuccess(true).setMessage("Token deleted").build();
    });
  }

  /**
   * Projects one stored token document onto the wire message. It names every field it copies, so a
   * field later added to the stored document - the plaintext token is already one such field on the
   * document {@code createApiToken} returns - is not carried out here by accident.
   */
  private static ApiTokenInfo toApiTokenInfo(final JSONObject token) {
    return ApiTokenInfo.newBuilder()
        .setName(token.getString("name", ""))
        .setDatabase(token.getString("database", ""))
        .setExpiresAt(token.getLong("expiresAt", 0L))
        .setCreatedAt(token.getLong("createdAt", 0L))
        .setPermissionsJson(token.getJSONObject("permissions", new JSONObject()).toString())
        .setTokenHash(token.getString("tokenHash", ""))
        .setTokenSuffix(token.getString("tokenSuffix", ""))
        .build();
  }

  /**
   * Reads a free-form JSON field of a request. An absent field is an empty document, which is what an
   * unset proto string looks like and what the HTTP body means by omitting the key; a present but
   * unparseable one is the caller's error and must not surface as an INTERNAL fault.
   */
  private static JSONObject parseDocument(final String json, final String fieldName) {
    if (json == null || json.isBlank())
      return new JSONObject();
    try {
      return new JSONObject(json);
    } catch (final JSONException e) {
      // Narrow deliberately: JSONObject(String) wraps a parse failure in exactly this type, and a
      // broader catch here would also turn a bug in this method into a 'your JSON is malformed'
      // answer, sending the caller after a document that is fine.
      throw new IllegalArgumentException("'" + fieldName + "' is not a valid JSON document: " + e.getMessage());
    }
  }

  /**
   * Refuses to write secret material back over a transport that does not protect it.
   * <p>
   * Fails closed when {@link GrpcTransportSecurityInterceptor} did not run: an absent key is not
   * "unknown, carry on" but "nothing vouched for this connection". That way removing the interceptor
   * stops tokens being minted rather than stopping them being protected.
   */
  private void requireTransportSafeForSecrets() throws StatusException {
    if (!Boolean.TRUE.equals(GrpcTransportSecurityInterceptor.SECRET_SAFE_TRANSPORT_KEY.get()))
      throw Status.FAILED_PRECONDITION.withDescription(
              "Refusing to return API token material over an unprotected transport. Enable gRPC TLS "
                  + "(arcadedb.grpc.tls.enabled), or issue the token from a client on the loopback interface.")
          .asException();
  }

  // ------------------------------------------------------------------------------------
  // Database lifecycle beyond create/drop
  // ------------------------------------------------------------------------------------

  @Override
  public void openDatabase(final OpenDatabaseRequest req, final StreamObserver<OpenDatabaseResponse> resp) {
    respond(resp, "openDatabase", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.openDatabase(req.getName());
      return OpenDatabaseResponse.newBuilder().build();
    });
  }

  @Override
  public void closeDatabase(final CloseDatabaseRequest req, final StreamObserver<CloseDatabaseResponse> resp) {
    respond(resp, "closeDatabase", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.closeDatabase(req.getName());
      return CloseDatabaseResponse.newBuilder().build();
    });
  }

  @Override
  public void alignDatabase(final AlignDatabaseRequest req, final StreamObserver<AlignDatabaseResponse> resp) {
    respond(resp, "alignDatabase", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.alignDatabase(req.getName());
      return AlignDatabaseResponse.newBuilder().build();
    });
  }

  // ------------------------------------------------------------------------------------
  // Settings
  // ------------------------------------------------------------------------------------

  @Override
  public void setServerSetting(final SetServerSettingRequest req, final StreamObserver<SetServerSettingResponse> resp) {
    respond(resp, "setServerSetting", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.setServerSetting(req.getKey(), req.getValue());
      return SetServerSettingResponse.newBuilder().build();
    });
  }

  @Override
  public void setDatabaseSetting(final SetDatabaseSettingRequest req, final StreamObserver<SetDatabaseSettingResponse> resp) {
    respond(resp, "setDatabaseSetting", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.setDatabaseSetting(req.getDatabase(), req.getKey(), req.getValue());
      return SetDatabaseSettingResponse.newBuilder().build();
    });
  }

  // ------------------------------------------------------------------------------------
  // Backup
  // ------------------------------------------------------------------------------------

  @Override
  public void getBackupConfig(final GetBackupConfigRequest req, final StreamObserver<GetBackupConfigResponse> resp) {
    respond(resp, "getBackupConfig", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      final JSONObject config = controlPlane.getBackupConfig();
      final Object configDocument = config.get("config");

      return GetBackupConfigResponse.newBuilder()
          .setEnabled(config.getBoolean("enabled", false))
          .setConfigJson(configDocument instanceof JSONObject document ? document.toString() : "")
          .setMessage(config.getString("message", ""))
          .build();
    });
  }

  @Override
  public void setBackupConfig(final SetBackupConfigRequest req, final StreamObserver<SetBackupConfigResponse> resp) {
    respond(resp, "setBackupConfig", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      if (req.getConfigJson().isBlank())
        throw new IllegalArgumentException("Missing 'config_json' in request");

      controlPlane.setBackupConfig(new JSONObject(req.getConfigJson()));
      return SetBackupConfigResponse.newBuilder().build();
    });
  }

  @Override
  public void listBackups(final ListBackupsRequest req, final StreamObserver<ListBackupsResponse> resp) {
    respond(resp, "listBackups", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      final JSONObject result = controlPlane.listBackups(req.getDatabase());
      final ListBackupsResponse.Builder builder = ListBackupsResponse.newBuilder()
          .setDatabase(result.getString("database", req.getDatabase()))
          .setTotalSize(result.getLong("totalSize", 0L))
          .setTotalCount(result.getLong("totalCount", 0L));

      final JSONArray backups = result.getJSONArray("backups");
      for (int i = 0; i < backups.length(); i++) {
        final JSONObject backup = backups.getJSONObject(i);
        final Object timestamp = backup.get("timestamp");
        builder.addBackups(BackupInfo.newBuilder()
            .setFileName(backup.getString("fileName", ""))
            .setSizeBytes(backup.getLong("size", 0L))
            .setLastModifiedMs(backup.getLong("lastModified", 0L))
            .setTimestamp(timestamp instanceof String isoTimestamp ? isoTimestamp : "")
            .build());
      }
      return builder.build();
    });
  }

  @Override
  public void triggerBackup(final TriggerBackupRequest req, final StreamObserver<TriggerBackupResponse> resp) {
    respond(resp, "triggerBackup", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      final JSONObject result = controlPlane.triggerBackup(req.getDatabase());
      return TriggerBackupResponse.newBuilder().setBackupFile(result.getString("backupFile", "")).build();
    });
  }

  @Override
  public void deleteBackup(final DeleteBackupRequest req, final StreamObserver<DeleteBackupResponse> resp) {
    respond(resp, "deleteBackup", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.deleteBackup(req.getDatabase(), req.getFileName());
      return DeleteBackupResponse.newBuilder().build();
    });
  }

  // ------------------------------------------------------------------------------------
  // Query profiler
  // ------------------------------------------------------------------------------------

  @Override
  public void profilerStart(final ProfilerStartRequest req, final StreamObserver<ProfilerStateResponse> resp) {
    respond(resp, "profilerStart", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      // The effective timeout, not the requested one: a non-positive request gets the server default and a
      // start against an already-recording profiler keeps the live recording's bound, so echoing the request
      // would tell the client a time its recording does not end at (issue #7394).
      final JSONObject state = controlPlane.profilerStart(req.getTimeoutSeconds());
      return ProfilerStateResponse.newBuilder()
          .setRecording(state.getBoolean("recording", true))
          .setTimeoutSeconds(state.getInt("timeoutSeconds", 0))
          .build();
    });
  }

  @Override
  public void profilerStop(final ProfilerStopRequest req, final StreamObserver<ProfilerDocumentResponse> resp) {
    respond(resp, "profilerStop", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      return ProfilerDocumentResponse.newBuilder().setResultsJson(controlPlane.profilerStop().toString()).build();
    });
  }

  @Override
  public void profilerReset(final ProfilerResetRequest req, final StreamObserver<ProfilerStateResponse> resp) {
    respond(resp, "profilerReset", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.profilerReset();
      return ProfilerStateResponse.newBuilder().setRecording(false).build();
    });
  }

  @Override
  public void profilerResults(final ProfilerResultsRequest req, final StreamObserver<ProfilerDocumentResponse> resp) {
    respond(resp, "profilerResults", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      return ProfilerDocumentResponse.newBuilder().setResultsJson(controlPlane.profilerResults().toString()).build();
    });
  }

  @Override
  public void profilerList(final ProfilerListRequest req, final StreamObserver<ProfilerListResponse> resp) {
    respond(resp, "profilerList", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      final ProfilerListResponse.Builder builder = ProfilerListResponse.newBuilder();
      final JSONArray runs = controlPlane.profilerList();
      for (int i = 0; i < runs.length(); i++) {
        final JSONObject run = runs.getJSONObject(i);
        builder.addRuns(ProfilerRunInfo.newBuilder()
            .setFileName(run.getString("fileName", ""))
            .setSizeBytes(run.getLong("size", 0L))
            .setLastModifiedMs(run.getLong("lastModified", 0L))
            .build());
      }
      return builder.build();
    });
  }

  @Override
  public void profilerLoad(final ProfilerLoadRequest req, final StreamObserver<ProfilerDocumentResponse> resp) {
    respond(resp, "profilerLoad", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      return ProfilerDocumentResponse.newBuilder()
          .setResultsJson(controlPlane.profilerLoad(req.getFileName()).toString()).build();
    });
  }

  // ------------------------------------------------------------------------------------
  // Restore and import
  // ------------------------------------------------------------------------------------

  /**
   * Restores a backup archive this server produced into a database, reporting progress as it goes
   * (issue #7308). The gRPC counterpart of {@code restore backup <db> <file> as <target>} over HTTP,
   * which streams the same progress as Server-Sent Events.
   * <p>
   * Server-streaming rather than unary because a restore takes as long as it takes: a unary call
   * would hold the connection open for its whole duration with nothing to show, and a client could
   * not tell a slow restore from a hung one.
   */
  @Override
  public void restoreBackup(final RestoreBackupRequest req, final StreamObserver<RestoreProgress> resp) {
    GrpcProgressStream.stream(resp, ArcadeDbGrpcAdminService::restoreLine, null,
        report -> RestoreProgress.newBuilder().setCompleted(true)
            .setMessage(req.getTargetDatabase() + " restored successfully").build(),
        listener -> {
          requireServerAdmin(authenticate(req.getCredentials()));
          requireLeader("RestoreBackup");

          controlPlane.restoreBackup(req.getDatabase(), req.getFileName(), req.getTargetDatabase(), req.getOverwrite(),
              listener);
          return null;
        },
        e -> toStatus("restoreBackup", e));
  }

  /**
   * Creates a database by restoring the archive at a URL into it, reporting progress as it goes.
   * The gRPC counterpart of {@code restore database <name> <url>}.
   * <p>
   * The URL is the caller's, so it goes through the same SSRF and local-file guard the HTTP command
   * applies - {@code ServerControlPlane.validateClientRestoreImportUrl}, which this transport reaches
   * through the shared implementation rather than by repeating the check here.
   */
  @Override
  public void restoreDatabase(final RestoreDatabaseRequest req, final StreamObserver<RestoreProgress> resp) {
    GrpcProgressStream.stream(resp, ArcadeDbGrpcAdminService::restoreLine, null,
        report -> RestoreProgress.newBuilder().setCompleted(true)
            .setMessage(req.getDatabase() + " restored successfully").build(),
        listener -> {
          requireServerAdmin(authenticate(req.getCredentials()));
          requireLeader("RestoreDatabase");

          controlPlane.restoreDatabase(req.getDatabase(), req.getUrl(), listener);
          return null;
        },
        e -> toStatus("restoreDatabase", e));
  }

  /**
   * Creates a database and imports a source URL into it in one step, reporting the importer's log
   * lines and its running counters. The gRPC counterpart of {@code import database <name> <url>}.
   */
  @Override
  public void importDatabase(final ImportDatabaseRequest req, final StreamObserver<ImportProgress> resp) {
    GrpcProgressStream.stream(resp,
        message -> ImportProgress.newBuilder().setMessage(message).build(),
        (parsed, vertices, edges) -> ImportProgress.newBuilder().setParsed(parsed).setVertices(vertices)
            .setEdges(edges).build(),
        report -> {
          final ImportProgress.Builder completed = ImportProgress.newBuilder().setCompleted(true)
              .setMessage(req.getDatabase() + " imported successfully");
          // The importer's own report, verbatim: its keys are the importer's and change with it, so
          // they travel as JSON rather than as proto fields that would have to be revised in step.
          if (report instanceof JSONObject document)
            completed.setResultJson(document.toString());
          return completed.build();
        },
        listener -> {
          requireServerAdmin(authenticate(req.getCredentials()));
          requireLeader("ImportDatabase");

          return controlPlane.importDatabase(req.getDatabase(), req.getUrl(), listener);
        },
        e -> toStatus("importDatabase", e));
  }

  private static RestoreProgress restoreLine(final String message) {
    return RestoreProgress.newBuilder().setMessage(message).build();
  }

  // ------------------------------------------------------------------------------------
  // Server lifecycle and cluster
  // ------------------------------------------------------------------------------------

  @Override
  public void getServerEvents(final GetServerEventsRequest req, final StreamObserver<GetServerEventsResponse> resp) {
    respond(resp, "getServerEvents", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      final JSONObject result = controlPlane.getServerEvents(req.getFileName());
      final GetServerEventsResponse.Builder builder = GetServerEventsResponse.newBuilder()
          .setEventsJson(result.getJSONArray("events").toString());

      final JSONArray files = result.getJSONArray("files");
      for (int i = 0; i < files.length(); i++)
        builder.addFiles(files.getString(i));

      return builder.build();
    });
  }

  /**
   * Stops this server, or the named HA peer. The local branch is asynchronous - the shared
   * implementation schedules the stop a second out - so the response is written before the JVM exits
   * rather than the call failing with the connection.
   */
  @Override
  public void shutdown(final ShutdownRequest req, final StreamObserver<ShutdownResponse> resp) {
    respond(resp, "shutdown", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.shutdownServer(req.getServerName());
      return ShutdownResponse.newBuilder().build();
    });
  }

  @Override
  public void disconnectCluster(final DisconnectClusterRequest req, final StreamObserver<DisconnectClusterResponse> resp) {
    respond(resp, "disconnectCluster", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.disconnectCluster();
      return DisconnectClusterResponse.newBuilder().build();
    });
  }

  /**
   * The other half of the cluster pair (issue #7400), and the last verb of
   * {@code PostServerCommandHandler}'s dispatch chain that had no RPC.
   * <p>
   * A thin adapter over the same {@code ServerControlPlane.connectCluster} the HTTP {@code connect
   * cluster} verb calls, so the two transports cannot drift on what the verb does. Today that shared
   * method refuses unconditionally - the current HA stack has never implemented a client-initiated
   * join - and the refusal reaches the caller as {@code FAILED_PRECONDITION} through the
   * {@code OperationNotAvailableException} arm of {@link #toStatusException}. That is the same answer
   * the HTTP caller gets, from the same method, which is the point: parity of the contract, not a
   * working join. Issue #7401 carries the decision on whether to implement the join or retire the
   * verb.
   * <p>
   * The address is passed through unvalidated because the HTTP verb passes it through unvalidated:
   * {@code extractTarget} yields {@code ""} for a bare {@code connect cluster} and the shared method
   * refuses before reading its argument. Root-only, as {@code checkRootUser} makes the HTTP verb, and
   * not leader-routed, because {@code PostServerCommandHandler} does not forward either half of the
   * pair to the leader.
   */
  @Override
  public void connectCluster(final ConnectClusterRequest req, final StreamObserver<ConnectClusterResponse> resp) {
    respond(resp, "connectCluster", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      controlPlane.connectCluster(req.getServerAddress());
      return ConnectClusterResponse.newBuilder().build();
    });
  }

  /**
   * The server's open HTTP authentication sessions, the RPC equivalent of {@code GET /api/v1/sessions}
   * (issue #7310), root-only as {@code GetSessionsHandler}'s {@code checkRootUser} makes it.
   * <p>
   * This is not a session API. gRPC has no session of its own - every admin RPC authenticates from the
   * credentials on the request body - so there is no {@code Login}/{@code Logout} to go with it. What it
   * is, is an administrative read of server state like any other, and without it an operator driving the
   * server over gRPC alone cannot see who is logged in over HTTP.
   * <p>
   * The token is returned because {@code GET /api/v1/sessions} returns it, to the same root principal.
   * Redacting it here alone would make the two administrative views disagree while leaving the HTTP
   * disclosure exactly as it was.
   */
  @Override
  public void listSessions(final ListSessionsRequest req, final StreamObserver<ListSessionsResponse> resp) {
    respond(resp, "listSessions", () -> {
      requireServerAdmin(authenticate(req.getCredentials()));

      // Empty on a server running without the HTTP listener: no HTTP listener, no HTTP sessions.
      final List<HttpAuthSession> sessions = controlPlane.listHttpSessions();

      final ListSessionsResponse.Builder builder = ListSessionsResponse.newBuilder();
      for (final HttpAuthSession session : sessions)
        builder.addSessions(toSessionInfo(session));
      return builder.setCount(sessions.size()).build();
    });
  }

  // ------------------------------------------------------------------------------------
  // Probes
  // ------------------------------------------------------------------------------------

  /**
   * Liveness. Unauthenticated, like {@code GET /api/v1/health}: {@link GrpcAuthInterceptor} exempts
   * this method from the admin authentication choke point so an orchestrator can probe the node
   * without credentials. It answers the same way whatever the server's status is - reaching here
   * proves the process is live, and a node still warming up must not be killed.
   */
  @Override
  public void health(final HealthRequest req, final StreamObserver<HealthResponse> resp) {
    respond(resp, "health", () -> HealthResponse.newBuilder().setOk(controlPlane.isLive()).build());
  }

  /**
   * Readiness. Unauthenticated for the same reason as {@link #health}. Unlike the HTTP probe, which
   * answers 503, this returns {@code ready=false} with the reason rather than an error status: a
   * not-ready node is a successful answer to "are you ready", and a gRPC health checker reads the
   * payload.
   */
  @Override
  public void ready(final ReadyRequest req, final StreamObserver<ReadyResponse> resp) {
    respond(resp, "ready", () -> {
      final String reason = controlPlane.notReadyReason();
      return ReadyResponse.newBuilder().setReady(reason == null).setReason(reason == null ? "" : reason).build();
    });
  }

  // ------------------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------------------

  /**
   * One progress entry as the wire message, field for field the document {@code OperationProgress.toJSON()}
   * emits over HTTP.
   * <p>
   * Each volatile is read ONCE, and {@code percentage} is computed from the values already read rather than
   * through {@code getPercentage()}, which would re-read {@code done} and {@code total}: that is what keeps
   * the three mutually consistent inside one message, the same rule {@code toJSON()} follows. The snapshot
   * stays weakly consistent across fields - the producer takes no lock - which is the documented contract
   * and harmless for a progress display.
   */
  private static OperationProgressInfo toProgressInfo(final OperationProgress operation) {
    final long done = operation.getDone();
    final long total = operation.getTotal();
    final long startedOn = operation.getStartedOn();

    return OperationProgressInfo.newBuilder()
        .setId(operation.getId())
        .setDatabase(nullToEmpty(operation.getDatabaseName()))
        .setOperation(nullToEmpty(operation.getOperation()))
        .setStepName(nullToEmpty(operation.getStepName()))
        .setStepIndex(operation.getStepIndex())
        .setTotalSteps(operation.getTotalSteps())
        .setDone(done)
        .setTotal(total)
        .setPercentage(total <= 0 ? -1 : (int) Math.min(100L, done * 100L / total))
        .setStartedOn(startedOn)
        .setElapsedMs(System.currentTimeMillis() - startedOn)
        .build();
  }

  /**
   * One session as the wire message, field for field the document {@code GetSessionsHandler} emits.
   * {@code sourceIp}, {@code userAgent}, {@code country} and {@code city} are null on a session logged in
   * without those client headers, and proto3 has no null, so they travel as empty strings.
   */
  private static SessionInfo toSessionInfo(final HttpAuthSession session) {
    return SessionInfo.newBuilder()
        .setToken(nullToEmpty(session.getToken()))
        .setUser(session.getUser() == null ? "" : nullToEmpty(session.getUser().getName()))
        .setCreatedAt(session.getCreatedAt())
        .setLastUpdate(session.getLastUpdate())
        .setElapsedMs(session.elapsedFromLastUpdate())
        .setSourceIp(nullToEmpty(session.getSourceIp()))
        .setUserAgent(nullToEmpty(session.getUserAgent()))
        .setCountry(nullToEmpty(session.getCountry()))
        .setCity(nullToEmpty(session.getCity()))
        .build();
  }

  /** proto3 string fields reject null; an absent value is the empty string on this wire. */
  private static String nullToEmpty(final String value) {
    return value == null ? "" : value;
  }

  /**
   * Every unary handler of this service goes through here so the call is terminated exactly once (issue #7035):
   * the same {@code responded} guard {@link ArcadeDbGrpcService} carries inline, applied by {@link GrpcUnaryCall}.
   * {@code operation} prefixes the description of an unexpected failure, as the inline catch blocks used to.
   */
  private <T> void respond(final StreamObserver<T> resp, final String operation, final GrpcUnaryCall.Body<T> body) {
    GrpcUnaryCall.respond(resp, body, e -> toStatus(operation, e));
  }

  /**
   * Maps a handler failure to the status the client receives. A {@link StatusException} raised by the body (the
   * NOT_FOUND of {@code getDatabaseInfo}) is sent as it is; the authorization exception is checked before the
   * authentication one because it is the more specific outcome, not because of any inheritance between the two.
   */
  private StatusException toStatus(final String operation, final Exception e) {
    if (e instanceof StatusException se)
      return se;
    // A leader-only operation refused on a follower. Routed through the shared mapper so the answer carries the
    // LeaderRedirectProtocol trailers (issue #6183) and a client can redirect itself, rather than reading prose:
    // gRPC has no equivalent of the HTTP handler's forwardToLeaderIfReplica, which proxies the request body to
    // the leader, so naming the leader is how this transport reproduces that gate.
    if (e instanceof ServerIsNotTheLeaderException) {
      final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(e, operation, ha());
      return new StatusException(mapped.getStatus(), mapped.getTrailers());
    }
    if (e instanceof AdminAuthorizationException)
      return Status.PERMISSION_DENIED.withDescription(e.getMessage()).asException();
    // A restore/import URL this server refuses to fetch. Checked before the plain SecurityException
    // arm below because it is a subtype of it, and because it is emphatically not an authentication
    // failure: the caller is authenticated, and re-sending credentials will not make the URL
    // acceptable. PERMISSION_DENIED says that; UNAUTHENTICATED would send the caller to fix the wrong
    // thing (issue #7308).
    if (e instanceof ServerControlPlane.RestoreImportUrlNotAllowedException)
      return Status.PERMISSION_DENIED.withDescription(e.getMessage()).asException();
    if (e instanceof SecurityException)
      return Status.UNAUTHENTICATED.withDescription(e.getMessage()).asException();
    // ServerSecurityException does NOT extend java.lang.SecurityException, so it reaches here rather
    // than the arm above. Authentication failures never do - authenticate() converts those to a plain
    // SecurityException - so what is left is a security policy refusing the operation's arguments,
    // such as the shared credentials validator rejecting a short password on createUser. The HTTP
    // control plane answers that 403 (AbstractServerHttpHandler.isSecurityFailure treats the two
    // exception types alike), and PERMISSION_DENIED is the status that says the same thing.
    if (e instanceof ServerSecurityException)
      return Status.PERMISSION_DENIED.withDescription(e.getMessage()).asException();
    // The operation named a user, group or token that does not exist. HTTP answers these 404, and
    // NOT_FOUND is the status that says the same thing (issue #7309).
    if (e instanceof ServerControlPlane.NotFoundException)
      return Status.NOT_FOUND.withDescription(e.getMessage()).asException();
    // A token name already issued: HTTP's 409 on this transport, and distinct from INVALID_ARGUMENT
    // because the request is well formed - it is the identity that is taken.
    if (e instanceof ServerControlPlane.AlreadyExistsException)
      return Status.ALREADY_EXISTS.withDescription(e.getMessage()).asException();
    // A backup, restore or import already running on the same database is HTTP's 409 on the other
    // transport: the request is well formed and authorized, and retrying once the other operation
    // finishes is the fix. The engine's base type, so RestoreBackup, RestoreDatabase and ImportDatabase
    // get the same status TriggerBackup already got (issue #7384 - BackupInProgressException and
    // ServerControlPlane.OperationInProgressException both extend it). ExecuteCommand, which is where a SQL
    // 'BACKUP DATABASE' refused by the same slot surfaces, is on the other service and maps it to the same
    // ABORTED through GrpcErrorMapper (issue #7443).
    if (e instanceof DatabaseOperationInProgressException)
      return Status.ABORTED.withDescription(e.getMessage()).asException();
    // A rejected argument must not read as a server fault: an empty database name, an unparseable
    // setting value or a backup file name outside the backup directory are all the caller's to fix.
    if (e instanceof IllegalArgumentException)
      return Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException();
    // The operation cannot run in this server's configuration at all - HA not enabled, connect
    // cluster unsupported - rather than having been attempted and failed. Only that subtype: a plain
    // CommandExecutionException from here means the operation ran and failed (a backup archive that
    // could not be deleted), which is INTERNAL, not a precondition the caller can satisfy.
    if (e instanceof ServerControlPlane.OperationNotAvailableException)
      return Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException();
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
  /**
   * Refuses an operation that may only run on the cluster leader when this node is a follower.
   * <p>
   * The HTTP control plane forwards these same commands - create/drop database and create/drop user, the set
   * {@code PostServerCommandHandler.execute} hands to {@code forwardToLeaderIfReplica} - by proxying the request
   * to the leader. gRPC has no such proxy, so the equivalent gate is a refusal that names the leader, which is
   * the pattern {@code graphBatchLoad} already established on this transport (issues #6091 and #6183). Without
   * it, {@code createUserClusterWide} would reach {@code HAServerPlugin.replicateSecurityUsers} on a follower,
   * which is exactly the state the HTTP path never gets into.
   * <p>
   * A server with HA inactive is always allowed: there is no leader to be, and {@code getHA()} is null.
   */
  private void requireLeader(final String rpc) {
    final HAServerPlugin ha = ha();
    if (ha != null && !ha.isLeader())
      throw new ServerIsNotTheLeaderException(rpc + " must run on the cluster leader and this server is not it",
          ha.getLeaderAddress());
  }

  /** This server's HA plugin, or null when HA is inactive: the source of the leader address on a refusal. */
  private HAServerPlugin ha() {
    return server.getHA();
  }

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

  /**
   * Creates the database across the cluster, through the same control-plane method the HTTP
   * {@code create database} command uses: on a replicated database that also submits the Raft
   * install-database entry, so the peers install it too. This RPC created the database locally only
   * until issue #7389, and the {@link #requireLeader} gate above meant the divergence it produced
   * always landed on the node the followers treat as authoritative.
   *
   * @return the created database, so the {@code graph} branch of {@link #createDatabase} initialises
   * its default types on the instance the create already resolved rather than looking it up again
   */
  private Database createDatabasePhysical(final String name) {
    return controlPlane.createDatabase(name);
  }

  /**
   * Drops the database across the cluster, through the same control-plane method the HTTP
   * {@code drop database} command uses: Raft-first on a replicated database, local otherwise. See
   * {@link #createDatabasePhysical} for why this RPC no longer touches the embedded database itself.
   */
  private void dropDatabasePhysical(final String name) {
    controlPlane.dropDatabase(name);
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
