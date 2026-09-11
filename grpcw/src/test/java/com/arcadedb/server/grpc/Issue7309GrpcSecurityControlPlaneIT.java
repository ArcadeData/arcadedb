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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ApiTokenConfiguration;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #7309: the rows of the {@code /server/*} security table that #7304 left HTTP-only - updating
 * a user, the three group routes and the three API-token routes - reachable over gRPC.
 * <p>
 * One test per RPC, each driving it against a real server and then reading the result back through a
 * DIFFERENT surface than the one that wrote it wherever possible: a group written by {@code SaveGroup}
 * is read through {@code ListGroups}, a token minted by {@code CreateApiToken} is looked for in
 * {@code ListApiTokens}, and a user updated by {@code UpdateUser} is checked against
 * {@code ServerSecurity} itself. A test that asserted only on the RPC's own response would pass
 * against a handler that built a convincing answer and wrote nothing.
 * <p>
 * The channel is loopback, so {@code CreateApiToken}'s transport gate permits the mint here - which
 * makes the successful mint a positive control for the gate as well as for the RPC. The refusal side
 * is {@link #createApiTokenMintsOnlyOverAProtectedTransport}, which drives the verdict into the
 * handler directly because every channel an in-process test can open is loopback, and
 * {@link GrpcTransportSecurityInterceptorTest}, which covers how that verdict is reached.
 */
class Issue7309GrpcSecurityControlPlaneIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 51181;

  private ManagedChannel                                              channel;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub   admin;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    GlobalConfiguration.GRPC_PORT.setValue(GRPC_PORT);
  }

  @BeforeEach
  void openChannel() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    admin = ArcadeDbAdminServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
  }

  @AfterEach
  void closeChannel() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      channel = null;
    }
  }

  private static DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  // -------------------------------------------------------------------------------------------
  // Row 1 - PUT /server/users
  // -------------------------------------------------------------------------------------------

  /**
   * The distinction that makes UpdateUser a separate RPC rather than a re-run of CreateUser: each
   * mutable field carries presence, so an update naming only one of them leaves the other alone. A
   * handler that rebuilt the user from the request would pass a password-only assertion and silently
   * drop the grants, so both halves are asserted after each of the two updates.
   */
  @Test
  void updateUserChangesOnlyTheFieldsTheRequestCarries() {
    admin.createUser(CreateUserRequest.newBuilder().setCredentials(root())
        .setUser("elena").setPassword("initialPassword1")
        .putDatabases("*", UserGroups.newBuilder().addGroups("admin").build()).build());

    // Password only: the grants must survive it.
    admin.updateUser(UpdateUserRequest.newBuilder().setCredentials(root())
        .setUser("elena").setPassword("replacedPassword1").build());

    assertThat(grantsOf("elena")).containsExactly("admin");
    assertThat(getServer(0).getSecurity().authenticate("elena", "replacedPassword1", null)).isNotNull();

    // Grants only: the password must survive it.
    admin.updateUser(UpdateUserRequest.newBuilder().setCredentials(root())
        .setUser("elena")
        .setDatabases(UserDatabases.newBuilder()
            .putDatabases("*", UserGroups.newBuilder().addGroups("reader").build()).build())
        .build());

    assertThat(grantsOf("elena")).containsExactly("reader");
    assertThat(getServer(0).getSecurity().authenticate("elena", "replacedPassword1", null)).isNotNull();
  }

  @Test
  void updateUserRejectsAPasswordShorterThanThePolicy() {
    admin.createUser(CreateUserRequest.newBuilder().setCredentials(root())
        .setUser("shortpw").setPassword("longEnoughPassword").build());

    final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
        () -> admin.updateUser(UpdateUserRequest.newBuilder().setCredentials(root())
            .setUser("shortpw").setPassword("short").build()));

    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(failure.getStatus().getDescription()).contains("at least 8 characters");
  }

  @Test
  void updateUserAnswersNotFoundForAnAbsentUser() {
    final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
        () -> admin.updateUser(UpdateUserRequest.newBuilder().setCredentials(root())
            .setUser("nobody").setPassword("longEnoughPassword").build()));

    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
  }

  // -------------------------------------------------------------------------------------------
  // Rows 2-4 - the group routes
  // -------------------------------------------------------------------------------------------

  @Test
  void listGroupsReturnsTheGroupDocument() {
    final JSONObject groups = new JSONObject(admin.listGroups(
        ListGroupsRequest.newBuilder().setCredentials(root()).build()).getGroupsJson());

    assertThat(groups.getJSONObject("databases").getJSONObject("*").getJSONObject("groups").has("admin")).isTrue();
  }

  /**
   * A group saved over gRPC has to be readable as a group, not merely stored: the assertion reads the
   * normalized defaults back, which is the part the HTTP handler used to apply and the shared
   * implementation now applies for both transports.
   */
  @Test
  void saveGroupWritesAGroupReadableThroughListGroups() {
    admin.saveGroup(SaveGroupRequest.newBuilder().setCredentials(root())
        .setDatabase("*").setName("analyst")
        .setGroupJson(new JSONObject().put("resultSetLimit", 250L)
            .put("types", new JSONObject().put("*", new JSONObject()
                .put("access", new JSONArray().put("readRecord"))))
            .toString())
        .build());

    final JSONObject analyst = wildcardGroups().getJSONObject("analyst");
    assertThat(analyst.getLong("resultSetLimit")).isEqualTo(250L);
    // Not supplied by the caller, so it must carry the shared default rather than being absent.
    assertThat(analyst.getLong("readTimeout")).isEqualTo(-1L);
    assertThat(analyst.getJSONObject("types").getJSONObject("*").getJSONArray("access").getString(0))
        .isEqualTo("readRecord");
  }

  @Test
  void deleteGroupRemovesIt() {
    admin.saveGroup(SaveGroupRequest.newBuilder().setCredentials(root())
        .setDatabase("*").setName("temporary").setGroupJson("{}").build());
    assertThat(wildcardGroups().has("temporary")).isTrue();

    admin.deleteGroup(DeleteGroupRequest.newBuilder().setCredentials(root())
        .setDatabase("*").setName("temporary").build());

    assertThat(wildcardGroups().has("temporary")).isFalse();
  }

  @Test
  void deleteGroupAnswersNotFoundForAnAbsentGroup() {
    final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
        () -> admin.deleteGroup(DeleteGroupRequest.newBuilder().setCredentials(root())
            .setDatabase("*").setName("neverExisted").build()));

    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
  }

  /**
   * The refusal that keeps a deployment from locking itself out. It lives in the shared
   * implementation, so proving gRPC inherits it is proving it was not left behind in the HTTP handler.
   */
  @Test
  void deleteGroupRefusesTheAdminGroupOfTheDefaultDatabase() {
    final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
        () -> admin.deleteGroup(DeleteGroupRequest.newBuilder().setCredentials(root())
            .setDatabase("*").setName("admin").build()));

    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(failure.getStatus().getDescription()).contains("admin group");
    assertThat(wildcardGroups().has("admin")).isTrue();
  }

  // -------------------------------------------------------------------------------------------
  // Rows 5-7 - the API-token routes
  // -------------------------------------------------------------------------------------------

  @Test
  void createApiTokenReturnsTheMaterialOnceAndListsTheTokenWithout() {
    final CreateApiTokenResponse minted = admin.createApiToken(CreateApiTokenRequest.newBuilder()
        .setCredentials(root()).setName("etl").setDatabase("graph").setExpiresAt(0)
        .setPermissionsJson(new JSONObject()
            .put("types", new JSONObject().put("*", new JSONObject()
                .put("access", new JSONArray().put("readRecord")))).toString())
        .build());

    assertThat(minted.getToken()).startsWith("at-");
    assertThat(minted.getInfo().getName()).isEqualTo("etl");
    assertThat(minted.getInfo().getDatabase()).isEqualTo("graph");
    assertThat(minted.getInfo().getTokenHash()).isNotEmpty();
    // The listing carries the hash needed to revoke the token and never the token itself - the
    // property that makes the mint's one-time answer meaningful.
    final List<ApiTokenInfo> listed = admin.listApiTokens(
        ListApiTokensRequest.newBuilder().setCredentials(root()).build()).getTokensList();

    final ApiTokenInfo etl = listed.stream().filter(t -> "etl".equals(t.getName())).findFirst().orElseThrow();
    assertThat(etl.getTokenHash()).isEqualTo(minted.getInfo().getTokenHash());
    assertThat(etl.getTokenSuffix()).isEqualTo(minted.getToken().substring(minted.getToken().length() - 4));
    assertThat(etl.toString()).doesNotContain(minted.getToken());
    assertThat(new JSONObject(etl.getPermissionsJson()).getJSONObject("types").getJSONObject("*")
        .getJSONArray("access").getString(0)).isEqualTo("readRecord");
  }

  @Test
  void createApiTokenRefusesADuplicateNameWithAlreadyExists() {
    admin.createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root()).setName("unique").build());

    final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
        () -> admin.createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root())
            .setName("unique").build()));

    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
  }

  /**
   * The permission-document check that used to be private to {@code PostApiTokenHandler}. An access
   * verb the engine does not define would grant nothing at all, so it is refused rather than stored.
   */
  @Test
  void createApiTokenRefusesAnUnknownAccessVerb() {
    final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
        () -> admin.createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root())
            .setName("bogus")
            .setPermissionsJson(new JSONObject().put("types", new JSONObject().put("*", new JSONObject()
                .put("access", new JSONArray().put("readAllTheThings")))).toString())
            .build()));

    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(failure.getStatus().getDescription()).contains("readAllTheThings");
  }

  @Test
  void deleteApiTokenRevokesByHash() {
    final CreateApiTokenResponse minted = admin.createApiToken(CreateApiTokenRequest.newBuilder()
        .setCredentials(root()).setName("revokeMe").build());

    admin.deleteApiToken(DeleteApiTokenRequest.newBuilder().setCredentials(root())
        .setTokenHash(minted.getInfo().getTokenHash()).build());

    assertThat(admin.listApiTokens(ListApiTokensRequest.newBuilder().setCredentials(root()).build())
        .getTokensList()).noneMatch(t -> "revokeMe".equals(t.getName()));
  }

  /**
   * Revocation by plaintext token is refused for the reason revocation exists: the token would then
   * appear in whatever recorded the request.
   */
  @Test
  void deleteApiTokenRefusesAPlaintextToken() {
    final CreateApiTokenResponse minted = admin.createApiToken(CreateApiTokenRequest.newBuilder()
        .setCredentials(root()).setName("plaintextRevoke").build());

    final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
        () -> admin.deleteApiToken(DeleteApiTokenRequest.newBuilder().setCredentials(root())
            .setTokenHash(minted.getToken()).build()));

    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    // Still live: a refused revocation must not half-happen.
    assertThat(admin.listApiTokens(ListApiTokensRequest.newBuilder().setCredentials(root()).build())
        .getTokensList()).anyMatch(t -> "plaintextRevoke".equals(t.getName()));
  }

  @Test
  void deleteApiTokenAnswersNotFoundForAnUnknownHash() {
    final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
        () -> admin.deleteApiToken(DeleteApiTokenRequest.newBuilder().setCredentials(root())
            .setTokenHash("0".repeat(64)).build()));

    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
  }

  // -------------------------------------------------------------------------------------------
  // Authorization - the gate every one of these RPCs shares
  // -------------------------------------------------------------------------------------------

  /**
   * Every route in this group is {@code checkRootUser}-gated on HTTP, so every RPC has to be
   * {@code requireServerAdmin}-gated here. Driven once per RPC rather than once overall: the gate is a
   * line in each handler, and a handler that forgot it would still pass a test that only checked its
   * neighbours.
   */
  @Test
  void everyNewRpcRefusesANonRootAccount() {
    admin.createUser(CreateUserRequest.newBuilder().setCredentials(root())
        .setUser("plainuser").setPassword("plainPassword1").build());

    final DatabaseCredentials plain = DatabaseCredentials.newBuilder()
        .setUsername("plainuser").setPassword("plainPassword1").build();

    assertPermissionDenied(() -> admin.updateUser(
        UpdateUserRequest.newBuilder().setCredentials(plain).setUser("plainuser").build()));
    assertPermissionDenied(() -> admin.listGroups(
        ListGroupsRequest.newBuilder().setCredentials(plain).build()));
    assertPermissionDenied(() -> admin.saveGroup(
        SaveGroupRequest.newBuilder().setCredentials(plain).setDatabase("*").setName("x").build()));
    assertPermissionDenied(() -> admin.deleteGroup(
        DeleteGroupRequest.newBuilder().setCredentials(plain).setDatabase("*").setName("x").build()));
    assertPermissionDenied(() -> admin.listApiTokens(
        ListApiTokensRequest.newBuilder().setCredentials(plain).build()));
    assertPermissionDenied(() -> admin.createApiToken(
        CreateApiTokenRequest.newBuilder().setCredentials(plain).setName("x").build()));
    assertPermissionDenied(() -> admin.deleteApiToken(
        DeleteApiTokenRequest.newBuilder().setCredentials(plain).setTokenHash("0".repeat(64)).build()));
  }

  private static void assertPermissionDenied(final Runnable call) {
    final StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class, call::run);
    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  // -------------------------------------------------------------------------------------------
  // The two questions the issue attaches to the mint
  // -------------------------------------------------------------------------------------------

  /**
   * 6a - the mint is refused unless the call's transport protects the answer.
   * <p>
   * Every channel an in-process test can open is loopback, so the three transport verdicts are driven
   * into the handler through the context key that {@link GrpcTransportSecurityInterceptor} publishes,
   * against the real service and the real server. The <b>absent</b> case is the one that matters most:
   * it is what a future refactor that drops the interceptor registration produces, and it must read as
   * "nothing vouched for this connection", not as "no objection".
   * <p>
   * The permitted case is the positive control. Without it, a handler that refused unconditionally
   * would pass the two refusal assertions.
   */
  @Test
  void createApiTokenMintsOnlyOverAProtectedTransport() {
    final ArcadeDbGrpcAdminService service = new ArcadeDbGrpcAdminService(getServer(0),
        getServer(0).getSecurity().getCredentialsValidator());

    final CreateApiTokenRequest request = CreateApiTokenRequest.newBuilder()
        .setCredentials(root()).setName("gated").build();

    assertThat(mintUnder(service, request, null))
        .as("no interceptor ran, so nothing vouched for the transport")
        .isEqualTo(Status.Code.FAILED_PRECONDITION);

    assertThat(mintUnder(service, request, false))
        .as("the interceptor read the transport as unprotected")
        .isEqualTo(Status.Code.FAILED_PRECONDITION);

    // Nothing was minted by either refusal.
    assertThat(admin.listApiTokens(ListApiTokensRequest.newBuilder().setCredentials(root()).build())
        .getTokensList()).noneMatch(t -> "gated".equals(t.getName()));

    // Positive control: the same call over a transport the interceptor vouched for succeeds.
    final CapturingObserver<CreateApiTokenResponse> permitted = new CapturingObserver<>();
    contextWith(true).run(() -> service.createApiToken(request, permitted));
    assertThat(permitted.error).isNull();
    assertThat(permitted.value.getToken()).startsWith("at-");
  }

  /**
   * 6b - a minted token reaches no log sink and no metric tag.
   * <p>
   * The two interceptors that see every message read the method name, the status and, for one specific
   * response type, a boolean; neither stringifies a payload. That is a property of today's code that
   * nothing enforces, which is what this test is for: it mints a real token over the real server with
   * every engine log call captured, and searches the captured text - and every meter the gRPC
   * interceptors registered - for the token material.
   * <p>
   * The capture is installed through {@link LogManager#setLogger}, the engine's own logging seam,
   * rather than by attaching a {@code java.util.logging} handler to the root logger. That distinction
   * decides whether this test works at all: the engine logs through its pluggable
   * {@link Logger}, so a JUL handler sees whatever that implementation chooses to
   * forward and at whatever level it is configured for - which was verified to miss an INFO-level leak
   * deliberately injected into {@code GrpcLoggingInterceptor.sendMessage}. Capturing at the seam sees
   * every call the engine makes, before any level or handler filtering.
   */
  @Test
  void aMintedTokenReachesNoLogSinkAndNoMetricTag() {
    final LogManager logManager = LogManager.instance();
    final Logger previousLogger = logManager.getLogger();
    final CapturingLogger captured = new CapturingLogger(previousLogger);

    final String token;
    try {
      logManager.setLogger(captured);
      token = admin.createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root())
          .setName("loggingProbe").build()).getToken();
      // Emitted while the capture is still installed, obviously - but stated explicitly because
      // getting this wrong is silent: a canary logged after the restore proves nothing, and the
      // assertion below would then fail for a reason that has nothing to do with token material.
      logManager.log(this, Level.FINE, "canary %s", "value");
    } finally {
      logManager.setLogger(previousLogger);
    }

    assertThat(token).startsWith("at-");
    // The capture has to have seen SOMETHING, or this test would pass however loudly the token leaked.
    // The gRPC interceptors' own FINE lines would satisfy that, so the canary is explicit rather than
    // inherited from them - it also pins that arguments, not just format strings, are captured.
    assertThat(captured.text()).contains("canary").contains("value");

    assertThat(captured.text()).doesNotContain(token);
    // The suffix is four characters and could collide with anything; the hash could not, and it is the
    // value an over-helpful log line would most plausibly carry.
    assertThat(captured.text()).doesNotContain(ApiTokenConfiguration.hashToken(token));

    final String meters = Metrics.globalRegistry.getMeters().stream()
        .map(meter -> meter.getId().toString())
        .collect(Collectors.joining(" "));
    assertThat(meters).doesNotContain(token);
  }

  /**
   * Calls {@code createApiToken} with the transport verdict set to {@code safe} - or, when it is null,
   * with the key never set at all - and returns the failure.
   */
  private static Status.Code mintUnder(final ArcadeDbGrpcAdminService service,
      final CreateApiTokenRequest request, final Boolean safe) {
    final CapturingObserver<CreateApiTokenResponse> observer = new CapturingObserver<>();
    contextWith(safe).run(() -> service.createApiToken(request, observer));

    assertThat(observer.error).as("the call must have failed").isNotNull();
    // Called directly rather than over a channel, so the service's own StatusException arrives here -
    // a channel would have re-thrown it to the caller as the StatusRuntimeException the other tests
    // in this class catch. Reading the status off either shape keeps the assertion about the status.
    return Status.fromThrowable(observer.error).getCode();
  }

  private static Context contextWith(final Boolean safe) {
    return safe == null ? Context.current()
        : Context.current().withValue(GrpcTransportSecurityInterceptor.SECRET_SAFE_TRANSPORT_KEY, safe);
  }

  /** Records what the service wrote back, so a unary call can be driven without a channel. */
  private static final class CapturingObserver<T> implements StreamObserver<T> {
    private T         value;
    private Throwable error;

    @Override
    public void onNext(final T value) {
      this.value = value;
    }

    @Override
    public void onError(final Throwable error) {
      this.error = error;
    }

    @Override
    public void onCompleted() {
    }
  }

  /**
   * Records every engine log call, message and arguments alike, and forwards it to the logger it
   * replaced so a failing run still shows its normal output.
   */
  private static final class CapturingLogger implements Logger {
    private final Logger delegate;
    private final StringBuilder           text = new StringBuilder();

    private CapturingLogger(final Logger delegate) {
      this.delegate = delegate;
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      record(message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11,
          arg12, arg13, arg14, arg15, arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8,
          arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      record(message, exception, context, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }

    private void record(final String message, final Throwable exception, final String context, final Object... values) {
      synchronized (text) {
        text.append(message).append(' ').append(context).append(' ');
        if (exception != null)
          text.append(exception).append(' ');
        for (final Object value : values)
          text.append(value).append(' ');
      }
    }

    private String text() {
      synchronized (text) {
        return text.toString();
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  /** The groups {@code userName} holds on the wildcard database, read from the server's own security. */
  private List<String> grantsOf(final String userName) {
    final JSONObject databases = getServer(0).getSecurity().getUser(userName).toJSON().getJSONObject("databases");
    final JSONArray groups = databases.getJSONArray("*");
    return IntStream.range(0, groups.length()).mapToObj(groups::getString).toList();
  }

  private JSONObject wildcardGroups() {
    return new JSONObject(admin.listGroups(ListGroupsRequest.newBuilder().setCredentials(root()).build())
        .getGroupsJson()).getJSONObject("databases").getJSONObject("*").getJSONObject("groups");
  }
}
