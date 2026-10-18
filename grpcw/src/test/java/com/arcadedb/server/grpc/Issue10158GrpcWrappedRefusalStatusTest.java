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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.DatabaseAlreadyExistsException;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.UserAlreadyExistsException;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10158: #10089 taught ONE arm of {@code ArcadeDbGrpcAdminService.toStatus} - the leader refusal - to look along
 * the cause chain; the other eleven still tested {@code e instanceof X} on the head frame, so the same refusal wrapped
 * once by the engine walked past every arm into the {@code INTERNAL} catch-all, and in production mode the classified
 * sentence was replaced by the wrapper's. {@code ArcadeDbGrpcService.toSearchStatus} had the same head-only arm for
 * {@code ServerSecurityException}, and {@code GrpcErrorMapper} for the duplicated key and the maintenance-slot conflict.
 * <p>
 * The assertion is over the whole ladder, not one arm: for every classified failure, wrapping must not change the answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10158GrpcWrappedRefusalStatusTest {
  private static final String LEADER = "leader-host:2480";

  /** The wrappers the engine and the control plane actually put around a failure on its way out. */
  private static final Map<String, Function<Exception, Exception>> WRAPPERS = new LinkedHashMap<>();

  static {
    WRAPPERS.put("DatabaseOperationException", e -> new DatabaseOperationException("Error on saving the user", e));
    WRAPPERS.put("CommandExecutionException", e -> new CommandExecutionException("Error activating restored database 'x'", e));
    WRAPPERS.put("TransactionException", e -> new TransactionException("Transaction error on commit", e));
    // ServerControlPlane.triggerBackup re-throws the reflective call's cause inside a plain RuntimeException
    WRAPPERS.put("RuntimeException", e -> new RuntimeException("Error triggering backup for database 'x'", e));
    WRAPPERS.put("two wrappers", e -> new TransactionException("commit", new DatabaseOperationException("listener", e)));
  }

  @TempDir
  Path root;

  /** Every failure the admin ladder classifies by itself, with the status it must answer bare and wrapped alike. */
  private static Map<Exception, Status.Code> adminRefusals() {
    final Map<Exception, Status.Code> refusals = new LinkedHashMap<>();
    refusals.put(new ArcadeDbGrpcAdminService.AdminAuthorizationException("Only root can do that"), Status.Code.PERMISSION_DENIED);
    refusals.put(new ServerControlPlane.RestoreImportUrlNotAllowedException("URL scheme 'file' is not allowed"),
        Status.Code.PERMISSION_DENIED);
    refusals.put(new SecurityException("Invalid credentials"), Status.Code.UNAUTHENTICATED);
    refusals.put(new ServerSecurityException("Password must be at least 8 characters"), Status.Code.PERMISSION_DENIED);
    refusals.put(new ServerControlPlane.NotFoundException("User 'ghost' not found"), Status.Code.NOT_FOUND);
    refusals.put(new ServerControlPlane.AlreadyExistsException("API token 'ci' already issued"), Status.Code.ALREADY_EXISTS);
    refusals.put(new UserAlreadyExistsException("bob"), Status.Code.ALREADY_EXISTS);
    refusals.put(new DatabaseAlreadyExistsException("Database 'shop' already exists"), Status.Code.ALREADY_EXISTS);
    refusals.put(new DatabaseOperationInProgressException("A backup of database 'shop' is already running"), Status.Code.ABORTED);
    refusals.put(new ServerControlPlane.OperationInProgressException("An import of database 'shop' is already running"),
        Status.Code.ABORTED);
    refusals.put(new IllegalArgumentException("Database name is empty"), Status.Code.INVALID_ARGUMENT);
    refusals.put(new ServerControlPlane.OperationNotAvailableException("HA is not enabled on this server"),
        Status.Code.FAILED_PRECONDITION);
    refusals.put(new NeedRetryException("Resync in progress"), Status.Code.UNAVAILABLE);
    refusals.put(new QuorumNotReachedException("Quorum not reached"), Status.Code.UNAVAILABLE);
    refusals.put(new ConcurrentModificationException("Concurrent modification on page 3"), Status.Code.UNAVAILABLE);
    return refusals;
  }

  @Test
  void everyClassifiedAdminRefusalAnswersTheSameStatusAndSentenceWhateverWrapsIt() {
    final ArcadeDbGrpcAdminService service = adminService(false);

    for (final Map.Entry<Exception, Status.Code> refusal : adminRefusals().entrySet()) {
      final StatusException bare = service.toStatus("createUser", refusal.getKey());
      assertThat(bare.getStatus().getCode()).as("bare %s", refusal.getKey().getClass().getSimpleName())
          .isEqualTo(refusal.getValue());

      for (final Map.Entry<String, Function<Exception, Exception>> wrapper : WRAPPERS.entrySet()) {
        final StatusException wrapped = service.toStatus("createUser", wrapper.getValue().apply(refusal.getKey()));
        assertThat(wrapped.getStatus().getCode()).as("%s inside %s", refusal.getKey().getClass().getSimpleName(), wrapper.getKey())
            .isEqualTo(refusal.getValue());
        assertThat(wrapped.getStatus().getDescription())
            .as("%s inside %s keeps the classified sentence, not the wrapper's", refusal.getKey().getClass().getSimpleName(),
                wrapper.getKey())
            .isEqualTo(bare.getStatus().getDescription());
      }
    }
  }

  @Test
  void productionModeKeepsTheSentenceOfAWrappedArcadeDbRefusalAndConcealsABuriedJdkOne() {
    final ArcadeDbGrpcAdminService service = adminService(true);

    for (final Map.Entry<Exception, Status.Code> refusal : adminRefusals().entrySet()) {
      final String name = refusal.getKey().getClass().getSimpleName();
      final StatusException bare = service.toStatus("createUser", refusal.getKey());
      final StatusException wrapped = service.toStatus("createUser",
          new DatabaseOperationException("Error on saving the user", refusal.getKey()));

      assertThat(bare.getStatus().getDescription()).as("bare %s is a sentence about the request", name)
          .isEqualTo(refusal.getKey().getMessage());
      assertThat(wrapped.getStatus().getCode()).as(name).isEqualTo(refusal.getValue());
      // The broad JDK/replication types say nothing about who wrote their message once they are buried under a wrapper
      assertThat(wrapped.getStatus().getDescription()).as(name)
          .isEqualTo(armFor(refusal.getKey()).broad() ?
              "createUser: " + GrpcErrorMapper.CONCEALED_DESCRIPTION :
              refusal.getKey().getMessage());
    }
  }

  @Test
  void aBuriedLibraryArgumentErrorKeepsItsStatusButNotItsTextInProduction() {
    final Exception buried = new CommandExecutionException("Error activating restored database 'shop'",
        new IllegalArgumentException("Illegal char <:> at index 2: /var/lib/arcadedb/databases/shop:tmp"));

    final StatusException production = adminService(true).toStatus("restoreDatabase", buried);
    assertThat(production.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(production.getStatus().getDescription()).isEqualTo("restoreDatabase: " + GrpcErrorMapper.CONCEALED_DESCRIPTION)
        .doesNotContain("/var/lib");

    final StatusException development = adminService(false).toStatus("restoreDatabase", buried);
    assertThat(development.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(development.getStatus().getDescription()).contains("/var/lib/arcadedb/databases/shop");
  }

  @Test
  void onlyAnArmThatDeclaresATrailerSendsOne() {
    // issue #10143: the trailer is part of the arm's contract with the client, so an arm that declares none sends none
    final ArcadeDbGrpcAdminService service = adminService(false);
    for (final Exception refusal : adminRefusals().keySet()) {
      final ArcadeDbGrpcAdminService.RefusalArm arm = armFor(refusal);
      final Metadata trailers = service.toStatus("createUser", refusal).getTrailers();
      if (arm.trailer() == null)
        assertThat(trailers).as(refusal.getClass().getSimpleName()).isNull();
      else
        assertThat(trailers.get(arm.trailer())).as(refusal.getClass().getSimpleName()).isEqualTo(refusal.getClass().getName());
    }
  }

  @Test
  void theTestCoversEveryArmOfTheLadder() {
    // A new arm added to the table without a case here fails this, so the assertion above stays over the whole ladder
    final List<Class<? extends Throwable>> covered = adminRefusals().keySet().stream()
        .<Class<? extends Throwable>>map(Exception::getClass).toList();

    for (final ArcadeDbGrpcAdminService.RefusalArm arm : ArcadeDbGrpcAdminService.REFUSAL_ARMS)
      assertThat(covered).as("a case for the %s arm", arm.type().getSimpleName()).contains(arm.type());
  }

  @Test
  void aClassifiedFailureKeepsItsOwnAnswerWhateverItsCauseCarriesAndAWrapperChangesNothing() {
    // The first frame an arm classifies decides, so a typed failure raised as itself is answered by its own arm - as it
    // always was, and as the HTTP control plane answers it - and the same failure under a wrapper is answered alike
    final ArcadeDbGrpcAdminService service = adminService(false);
    final Exception rejected = new IllegalArgumentException("Cannot create database 'x'",
        new QuorumNotReachedException("Quorum not reached"));
    final Exception refused = new ServerSecurityException("Password too short", new SecurityException("policy"));

    for (final Exception failure : List.of(rejected, refused)) {
      final StatusException bare = service.toStatus("createDatabase", failure);
      for (final Map.Entry<String, Function<Exception, Exception>> wrapper : WRAPPERS.entrySet()) {
        final StatusException wrapped = service.toStatus("createDatabase", wrapper.getValue().apply(failure));
        assertThat(wrapped.getStatus().getCode()).as("%s inside %s", failure, wrapper.getKey()).isEqualTo(bare.getStatus().getCode());
        assertThat(wrapped.getStatus().getDescription()).as("%s inside %s", failure, wrapper.getKey())
            .isEqualTo(bare.getStatus().getDescription());
      }
    }
    assertThat(service.toStatus("createDatabase", rejected).getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(service.toStatus("createUser", refused).getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  void aDuplicatedKeyThatRanIntoAConflictIsRetryableAsErrorCategoryRanksIt() {
    // Pinned on purpose (PR #10174 review): the data-plane mapper answers by ErrorCategory, where RETRY outranks
    // DUPLICATED_KEY whatever the depth. DuplicatedKeyException takes no cause, so the engine never builds this chain
    final DuplicatedKeyException duplicate = new DuplicatedKeyException("User[email]", "[x]", null);
    duplicate.initCause(new ConcurrentModificationException("conflict"));

    assertThat(GrpcErrorMapper.toStatusRuntimeException(duplicate, "ctx", null, false).getStatus().getCode())
        .isEqualTo(Status.Code.ABORTED);
  }

  @Test
  void aStatementTerminatedOnRequestIsCancelledAndNeverRetryable() {
    // Walking the chain for NeedRetryException must not turn a termination into a retry: the rollback of a terminated
    // statement can run into a conflict, and ErrorCategory decides TERMINATED before anything else for that reason
    final StatusException mapped = adminService(false).toStatus("importDatabase", new CommandExecutionException("Import failed",
        terminated(new ConcurrentModificationException("conflict on rollback"))));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(mapped.getTrailers())).isEqualTo(QueryTerminatedException.class.getName());
  }

  @Test
  void terminationOutranksEveryRefusalItsChainCarries() {
    // Admin: a termination is decided before the table, whatever classified refusal rides below it
    final ArcadeDbGrpcAdminService admin = adminService(false);
    for (final Exception below : List.of(new DatabaseOperationInProgressException("A backup is already running"),
        new ServerSecurityException("User 'bob' cannot do that"), new IllegalArgumentException("bad"))) {
      final StatusException mapped = admin.toStatus("importDatabase",
          new CommandExecutionException("Import failed", terminated(below)));
      assertThat(mapped.getStatus().getCode()).as(below.getClass().getSimpleName()).isEqualTo(Status.Code.CANCELLED);
      assertThat(exceptionClassOf(mapped.getTrailers())).isEqualTo(QueryTerminatedException.class.getName());
    }

    // Search: the security arm does not mask a search stopped on request
    final StatusException search = searchService().toSearchStatus("VectorSearch",
        new DatabaseOperationException("stopped", terminated(new ServerSecurityException("User 'bob' cannot read type 'Secret'"))));
    assertThat(search.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(search.getTrailers())).isEqualTo(QueryTerminatedException.class.getName());
  }

  @Test
  void theLeaderRefusalStillRedirectsBareAndWrapped() {
    final ArcadeDbGrpcAdminService service = adminService(false);
    final ServerIsNotTheLeaderException refusal = new ServerIsNotTheLeaderException("This server is not the leader", LEADER);

    for (final Exception failure : List.of(refusal, new DatabaseOperationException("Error on saving the user", refusal),
        new RuntimeException("Error triggering backup", refusal))) {
      final StatusException mapped = service.toStatus("createUser", failure);
      assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(exceptionClassOf(mapped.getTrailers())).isEqualTo(ServerIsNotTheLeaderException.class.getName());
      assertThat(mapped.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo(LEADER);
    }
  }

  @Test
  void anUnclassifiedFailureInsideAWrapperIsStillAConcealedInternalError() {
    final StatusException mapped = adminService(true).toStatus("createUser",
        new DatabaseOperationException("Error on saving the user", new IOException("/var/lib/arcadedb/config/users.jsonl")));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    assertThat(mapped.getStatus().getDescription()).isEqualTo("createUser: " + GrpcErrorMapper.CONCEALED_DESCRIPTION);
  }

  @Test
  void aWrappedSecurityRefusalOfASearchIsStillPermissionDenied() {
    final ServerSecurityException refusal = new ServerSecurityException("User 'bob' cannot read type 'Secret'");
    final ArcadeDbGrpcService service = searchService();

    final StatusException bare = service.toSearchStatus("VectorSearch", refusal);
    assertThat(bare.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);

    for (final Map.Entry<String, Function<Exception, Exception>> wrapper : WRAPPERS.entrySet()) {
      final StatusException wrapped = service.toSearchStatus("VectorSearch", wrapper.getValue().apply(refusal));
      assertThat(wrapped.getStatus().getCode()).as(wrapper.getKey()).isEqualTo(Status.Code.PERMISSION_DENIED);
      assertThat(wrapped.getStatus().getDescription()).as(wrapper.getKey()).isEqualTo(bare.getStatus().getDescription());
    }
  }

  @Test
  void aWrappedDuplicatedKeyKeepsItsTypeAndItsTrailers() {
    final DuplicatedKeyException duplicate = new DuplicatedKeyException("User[email]", "[bob@example.com]", null);
    final StatusRuntimeException bare = GrpcErrorMapper.toStatusRuntimeException(duplicate, "ctx", null, false);

    for (final Map.Entry<String, Function<Exception, Exception>> wrapper : WRAPPERS.entrySet()) {
      final StatusRuntimeException wrapped = GrpcErrorMapper.toStatusRuntimeException(wrapper.getValue().apply(duplicate), "ctx",
          null, false);
      assertThat(wrapped.getStatus().getCode()).as(wrapper.getKey()).isEqualTo(Status.Code.ALREADY_EXISTS);
      assertThat(exceptionClassOf(wrapped.getTrailers())).as("the type the driver rebuilds, not the wrapper's: %s", wrapper.getKey())
          .isEqualTo(DuplicatedKeyException.class.getName());
      assertThat(wrapped.getTrailers().get(GrpcErrorMapper.DUP_INDEX_KEY)).as(wrapper.getKey())
          .isEqualTo(bare.getTrailers().get(GrpcErrorMapper.DUP_INDEX_KEY));
      assertThat(wrapped.getTrailers().get(GrpcErrorMapper.DUP_KEYS_KEY)).as(wrapper.getKey())
          .isEqualTo(bare.getTrailers().get(GrpcErrorMapper.DUP_KEYS_KEY));
    }
  }

  @Test
  void aWrappedMaintenanceSlotConflictIsStillAborted() {
    final DatabaseOperationInProgressException inProgress = new DatabaseOperationInProgressException("A backup is already running");

    for (final Map.Entry<String, Function<Exception, Exception>> wrapper : WRAPPERS.entrySet())
      assertThat(GrpcErrorMapper.toStatusRuntimeException(wrapper.getValue().apply(inProgress), "ctx", null, false).getStatus()
          .getCode()).as(wrapper.getKey()).isEqualTo(Status.Code.ABORTED);
  }

  @Test
  void aWrappedSchemaErrorNamesItsOwnTypeSoTheDriverDoesNotRebuildARecordNotFound() {
    // NOT_FOUND alone is rebuilt client-side as RecordNotFoundException: only the class-name trailer tells the two apart
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new CommandExecutionException("Error on executing command", new SchemaException("Type 'Missing' was not found")), "ctx", null,
        false);

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(exceptionClassOf(mapped.getTrailers())).isEqualTo(SchemaException.class.getName());
  }

  @Test
  void aTerminatedStatementThatRanIntoADuplicateIsStillCancelled() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(new CommandExecutionException("stopped",
        terminated(new DuplicatedKeyException("User[email]", "[x]", null))), "ctx", null,
        false);

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(mapped.getTrailers())).isEqualTo(QueryTerminatedException.class.getName());
  }

  @Test
  void aWrappedDuplicatedKeyOnAStreamedLoadIsAConflict() {
    final DuplicatedKeyException duplicate = new DuplicatedKeyException("User[email]", "[x]", null);

    assertThat(ArcadeDbGrpcService.commitErrorCode(duplicate)).isEqualTo("CONFLICT");
    for (final Map.Entry<String, Function<Exception, Exception>> wrapper : WRAPPERS.entrySet())
      assertThat(ArcadeDbGrpcService.commitErrorCode(wrapper.getValue().apply(duplicate))).as(wrapper.getKey()).isEqualTo("CONFLICT");
    // A duplicate wins over a leader refusal it rides with: the key is taken on the leader too, so a redirect cannot help
    final ServerIsNotTheLeaderException refusal = new ServerIsNotTheLeaderException("This server is not the leader", LEADER);
    refusal.initCause(duplicate);
    assertThat(ArcadeDbGrpcService.commitErrorCode(new DatabaseOperationException("commit", refusal))).isEqualTo("CONFLICT");
    // The contract rejection is the stream's OWN refusal, raised as itself: one buried under a commit failure is not one
    assertThat(ArcadeDbGrpcService.commitErrorCode(new TransactionException("commit", new IllegalArgumentException("bad"))))
        .isEqualTo("COMMIT_FAILED");
  }

  /**
   * The guarantee that stops this coming back: the two ladders ask the cause chain, never their own {@code instanceof}
   * on the head frame. The only head test left is the pass-through of a status the handler already chose.
   */
  @Test
  void theStatusLaddersHoldNoHeadOnlyInstanceofArm() throws IOException {
    final Path sources = Path.of("src/main/java/com/arcadedb/server/grpc");

    assertThat(instanceofTargets(Files.readString(sources.resolve("ArcadeDbGrpcAdminService.java"), StandardCharsets.UTF_8),
        "StatusException toStatus(")).as("toStatus").containsOnly("StatusException");
    assertThat(instanceofTargets(Files.readString(sources.resolve("ArcadeDbGrpcService.java"), StandardCharsets.UTF_8),
        "StatusException toSearchStatus(")).as("toSearchStatus").containsOnly("StatusException", "StatusRuntimeException");
  }

  /** The type names every {@code instanceof} in the body of the method whose signature starts with {@code signature} tests. */
  private static List<String> instanceofTargets(final String source, final String signature) {
    final int start = source.indexOf(signature);
    assertThat(start).as("the method %s is where the census expects it", signature).isPositive();
    final int end = source.indexOf("\n  }\n", start);
    final Matcher matcher = Pattern.compile("instanceof\\s+(?:final\\s+)?([\\w.]+)").matcher(source.substring(start, end));
    final List<String> targets = new ArrayList<>();
    while (matcher.find())
      targets.add(matcher.group(1));
    return targets;
  }

  /** The arm of the table that answers {@code refusal}: the first whose type it is, as {@code toStatus} walks them. */
  private static ArcadeDbGrpcAdminService.RefusalArm armFor(final Exception refusal) {
    for (final ArcadeDbGrpcAdminService.RefusalArm arm : ArcadeDbGrpcAdminService.REFUSAL_ARMS)
      if (arm.type().isInstance(refusal))
        return arm;
    throw new AssertionError("No arm answers " + refusal.getClass().getName());
  }

  private static QueryTerminatedException terminated(final Throwable cause) {
    final QueryTerminatedException terminated = new QueryTerminatedException("terminated by admin");
    terminated.initCause(cause);
    return terminated;
  }

  private static String exceptionClassOf(final Metadata trailers) {
    return trailers != null ? trailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY) : null;
  }

  private ArcadeDbGrpcAdminService adminService(final boolean production) {
    final ContextConfiguration configuration = new ContextConfiguration();
    if (production)
      configuration.setValue(GlobalConfiguration.SERVER_MODE, "production");
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root.resolve(production ? "production" : "development"),
        configuration);
    return new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator());
  }

  private ArcadeDbGrpcService searchService() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root.resolve("search"), new ContextConfiguration());
    // No reaper: nothing here opens a transaction
    return new ArcadeDbGrpcService("unused", server, 0L, 0L, 0L);
  }
}
