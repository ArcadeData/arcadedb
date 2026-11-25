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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseIsReopeningException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.exception.DeletedEntityException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.EquivalentIndexException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.ObjectInUseException;
import com.arcadedb.exception.QueryAdmissionException;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.index.fulltext.FullTextQueryParseException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.parser.AlignDatabaseStatement;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.DatabaseAlreadyExistsException;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.UserAlreadyExistsException;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import com.example.plugin.ForeignRetryableException;
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
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The status, the description and the trailers a refused or failed request is answered with on the gRPC transport: the admin
 * service's {@code toStatus} ladder, the search RPCs' {@code toSearchStatus}, and the {@link GrpcErrorMapper} answers they are
 * held against. Pure unit tests over unstarted servers; the mapper's own arms are in {@link GrpcErrorMapperTest}.
 */
class GrpcRefusalStatusMappingTest {
  private static final String LEADER = "leader-host:2480";
  @TempDir
  Path root;

  /*
   * Issue #7511 on the gRPC transport: a group or API-token change refused because a peer cannot decode the entry it
   * would be replicated as reaches the client as {@code FAILED_PRECONDITION}, not {@code INTERNAL}.
   * <p>
   * The distinction is the client's to act on. {@code INTERNAL} says the server broke and tells a generated client
   * nothing about whether to retry; {@code FAILED_PRECONDITION} says the request was refused because the SYSTEM is
   * not in a state where it can succeed, which is exactly true here and clears itself when the last node is
   * upgraded. It is the status HTTP's {@code 409} is paired with everywhere else in this service.
   * <p>
   * Driven through {@code toStatus}, the mapper every admin RPC's {@code respond} wrapper delegates to, so
   * {@code SaveGroup}, {@code DeleteGroup}, {@code CreateApiToken} and {@code DeleteApiToken} are all covered by the
   * one arm rather than four tests of the same line.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */

  // Regression: #7511
  @Test
  void aClusterNotReadyRefusalIsAPreconditionFailureRatherThanAnInternalError() {
    final StatusException mapped = adminService().toStatus("saveGroup", new ClusterCapabilityNotReadyException(
        "Refusing to replicate the group document: peer(s) [arcadedb2] have not advertised the "
            + "'security-groups-entry' capability", "security-groups-entry", List.of("arcadedb2")));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(mapped.getStatus().getDescription())
        .as("the peer holding the cluster back is the only actionable half, so it must survive the mapping")
        .contains("arcadedb2");
  }

  // Regression: #7511
  @Test
  void theSameArmAnswersATokenRevocationRefusal() {
    final StatusException mapped = adminService().toStatus("deleteApiToken", new ClusterCapabilityNotReadyException(
        "Refusing to replicate the API-token document: peer(s) [arcadedb2] have not advertised the "
            + "'security-api-tokens-entry' capability", "security-api-tokens-entry", List.of("arcadedb2")));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  /*
   * Issue #7532 (absorbing #7550) on the gRPC transport: the status a residual seed failure reaches the client as.
   * <p>
   * The HTTP add-peer route has answered 503 for a joined-but-unseeded peer since issue #7521. The gRPC
   * {@code ConnectCluster} RPC answered OK and left the failure in a SEVERE log line, so an operator joining a
   * peer over gRPC had nothing their automation could branch on. {@code UNAVAILABLE} is this transport's 503 and,
   * as there, re-issuing the call is idempotent on the membership change and reissues the seed.
   * <p>
   * The second half is the {@code toStatus} gap the issue names: a {@link NeedRetryException} - concretely a
   * {@link QuorumNotReachedException} out of any admin operation that has to commit a Raft entry - had no arm of
   * its own and came out {@code INTERNAL}, telling a client the server broke rather than to send the same request
   * again. HTTP answers every one of them 503.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */

  // Regression: #7532
  @Test
  void aQuorumThatCouldNotBeReachedIsRetryableRatherThanAnInternalError() {
    final StatusException mapped = adminService().toStatus("createUser",
        new QuorumNotReachedException("Quorum 2 not reached on database 'db'"));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(mapped.getStatus().getDescription()).contains("Quorum 2 not reached");
    // issue #10143: named, or the client cannot tell it from a lost connection
    assertThat(mapped.getTrailers()).isNotNull();
    assertThat(mapped.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(QuorumNotReachedException.class.getName());
  }

  // Regression: #7532
  /**
   * Issue #10143: a retryable subtype from outside ArcadeDB is named by the arm's own type, not by a class name that would
   * tell the client what is installed on the server.
   */
  @Test
  void aForeignRetryableSubtypeIsNamedByTheArmsType() {
    final StatusException mapped = adminService().toStatus("createUser", new ForeignRetryableException("try again"));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(mapped.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(NeedRetryException.class.getName());
  }

  // Regression: #7532
  /**
   * Issue #10143: the unseeded-join answer is named as a {@link NeedRetryException}, so the client rebuilds the type the
   * {@code RemoteGrpcServer.connectCluster} javadoc promises rather than reading a lost connection.
   */
  @Test
  void anUnseededJoinIsNamedAsRetryable() {
    final StatusException status = ArcadeDbGrpcAdminService.seedFailureStatus(
        new ServerControlPlane.ConnectClusterResult("db2:2435", List.of("users")));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(status.getStatus().getDescription()).contains("db2:2435", "users");
    assertThat(status.getTrailers()).isNotNull();
    assertThat(status.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(NeedRetryException.class.getName());
    // and toStatus passes it through untouched, which is how the RPC sends it
    assertThat(adminService().toStatus("connectCluster", status)).isSameAs(status);
  }

  // Regression: #7532
  /**
   * Order: {@code ServerIsNotTheLeaderException} also extends {@link NeedRetryException}, and it must keep the
   * arm further up that gives it the {@code LeaderRedirectProtocol} trailers a client redirects itself with.
   * A new supertype arm placed above it would have swallowed that silently - the status would still look
   * retryable and the redirect would simply be gone.
   */
  @Test
  void theNotLeaderRefusalKeepsItsOwnArmDespiteExtendingTheSupertype() {
    final StatusException mapped = adminService()
        .toStatus("createDatabase",
            new ServerIsNotTheLeaderException("Not the leader", "db2:2480"));

    assertThat(mapped.getStatus().getCode())
        .as("routed through GrpcErrorMapper, not through the new NeedRetryException arm")
        .isNotEqualTo(Status.Code.UNAVAILABLE);
  }

  // Regression: #7532
  /**
   * The shared control-plane result the RPC reads. A clean join carries nothing to report; a residual failure
   * carries the document names and the remediation, and both halves have to survive into the status
   * description because a gRPC client has no response body to read them from.
   */
  @Test
  void theResultTheRpcReportsFromCarriesBothHalvesOfTheMessage() {
    final ServerControlPlane.ConnectClusterResult clean =
        new ServerControlPlane.ConnectClusterResult("db2:2435", List.of());
    assertThat(clean.hasFailedSeeds()).isFalse();

    final ServerControlPlane.ConnectClusterResult failed =
        new ServerControlPlane.ConnectClusterResult("db2:2435", List.of("users", "API tokens"));
    assertThat(failed.hasFailedSeeds()).isTrue();

    final String description = failed.errorMessage() + " " + failed.detailMessage();
    assertThat(description).contains("db2:2435", "users", "API tokens", "connect cluster");
  }

  /*
   * Issue #10075: the Java gRPC client reads a trailer-less {@code ALREADY_EXISTS} from any admin RPC as "the name is taken"
   * and raises {@code RemoteAlreadyExistsException}. That is only sound while the admin service keeps the convention this
   * test pins: it answers {@code ALREADY_EXISTS} for a taken user, token or database name and for nothing else, and it sends
   * those without an exception-class trailer. A new admin conflict that needs the same status has to name itself in the
   * trailer, or this test goes red before the client mislabels it.
   * <p>
   * Issue #10143: a taken name also carries {@code GrpcErrorMapper.NAME_TAKEN_KEY}, which is what a current client keys on,
   * since the admin service now sends class-name trailers on other arms.
   */

  // Regression: #10075
  @Test
  void everyTakenNameIsATrailerlessAlreadyExists() {
    final ArcadeDbGrpcAdminService service = adminService();
    for (final Exception taken : List.of(new UserAlreadyExistsException("bob"),
        new ServerControlPlane.AlreadyExistsException("A token with name 'ci' already exists"),
        new DatabaseAlreadyExistsException("Database 'db' already exists"))) {
      final StatusException status = service.toStatus("create", taken);

      assertThat(status.getStatus().getCode()).as(taken.getClass().getSimpleName()).isEqualTo(Status.Code.ALREADY_EXISTS);
      final Metadata trailers = status.getTrailers();
      // issue #10143: named by a trailer of its own, so the client does not depend on the absence of one
      assertThat(trailers).as(taken.getClass().getSimpleName()).isNotNull();
      assertThat(trailers.get(GrpcErrorMapper.NAME_TAKEN_KEY)).as(taken.getClass().getSimpleName())
          .isEqualTo(taken.getClass().getName());
      assertThat(trailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
          .as("a client that predates the name-taken trailer recognises a taken name by the missing class-name trailer")
          .isNull();
    }
  }

  // Regression: #10075
  @Test
  void nothingElseTheAdminServiceRaisesIsAlreadyExists() {
    final ArcadeDbGrpcAdminService service = adminService();
    for (final Exception other : List.of(new IllegalArgumentException("Database 'x' already exists"),
        new ServerSecurityException("not allowed"), new ServerControlPlane.NotFoundException("User 'bob' not found"),
        new DatabaseOperationInProgressException("backup running"), new DuplicatedKeyException("idx", "[k]", null),
        new RuntimeException("boom"))) {
      final StatusException status = service.toStatus("op", other);

      // a conflict that does share the status must name itself, so the client keeps its type
      if (status.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
        assertThat(status.getTrailers()).as(other.getClass().getSimpleName()).isNotNull();
        assertThat(status.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).as(other.getClass().getSimpleName()).isNotNull();
      }
    }
  }

  /*
   * Issue #10089: a leader refusal the engine wrapped (a commit failure, a record-event listener, a trigger) reached a gRPC
   * client as {@code ABORTED} with no leader address, so the driver retried the same follower until its retries ran out,
   * while nine other consumers found the refusal anywhere along the cause chain. {@code GrpcErrorMapper} tested
   * {@code cause instanceof} after peeling exactly one {@code ExecutionException}, and the admin service did the same.
   * <p>
   * What the client acts on is three things at once: the status ({@code FAILED_PRECONDITION}, not the {@code ABORTED} a
   * retry earns), the leader's address on the trailers, and the exception class name the driver rebuilds the typed
   * exception from. A wrapper's name there lands the driver in the status-code table, where {@code ABORTED} is a
   * {@code ConcurrentModificationException}, which is retried.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  private static ServerIsNotTheLeaderException refusal(final String leader) {
    return new ServerIsNotTheLeaderException("This server is not the leader", leader);
  }

  private static void assertRedirect(final Status status, final Metadata trailers) {
    assertThat(status.getCode()).as("not the ABORTED a retry on the same follower would earn").isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(trailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).as("the type the driver rebuilds, not the wrapper's")
        .isEqualTo(ServerIsNotTheLeaderException.class.getName());
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo(LEADER);
    assertThat(status.getDescription()).contains(LEADER);
  }

  // Regression: #10089
  @Test
  void theBareRefusalIsTheReference() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(refusal(LEADER), "ctx", null, false);

    assertRedirect(mapped.getStatus(), mapped.getTrailers());
  }

  // Regression: #10089
  @Test
  void aRefusalOneFrameDownIsAnsweredAsARedirect() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationException("Error on saving the record", refusal(LEADER)), "ctx", null, false);

    assertRedirect(mapped.getStatus(), mapped.getTrailers());
  }

  // Regression: #10089
  @Test
  void aRefusalSeveralFramesDownAndBehindAnExecutionExceptionIsAnsweredAsARedirect() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new ExecutionException(new TransactionException("commit", new DatabaseOperationException("listener", refusal(LEADER)))), "ctx",
        null, false);

    assertRedirect(mapped.getStatus(), mapped.getTrailers());
  }

  // Regression: #10089
  @Test
  void theConcealedAnswerStillNamesTheLeader() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationException("Error on saving the record", refusal(LEADER)), "ctx", null, true);

    assertRedirect(mapped.getStatus(), mapped.getTrailers());
    assertThat(mapped.getStatus().getDescription()).as("the engine text is concealed, the address is not")
        .doesNotContain("Error on saving the record");
  }

  // Regression: #10089
  @Test
  void aWrappedRefusalNamingNoLeaderAnswersWhatTheBareOneDoes() {
    final StatusRuntimeException bare = GrpcErrorMapper.toStatusRuntimeException(refusal(null), "ctx", null, false);
    final StatusRuntimeException wrapped = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationException("Error on saving the record", refusal(null)), "ctx", null, false);

    assertThat(wrapped.getStatus().getCode()).isEqualTo(bare.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(wrapped.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
        .isEqualTo(bare.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY));
    assertThat(wrapped.getStatus().getDescription()).contains("The leader is currently unknown");
  }

  // Regression: #10089
  @Test
  void aStatementTerminatedOnRequestKeepsItsVerdictWhateverItWraps() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationException("stopped", new QueryTerminatedException("terminated by admin")), "ctx", null, false);

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(mapped.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(QueryTerminatedException.class.getName());
  }

  // Regression: #10089
  @Test
  void aWrapperAroundSomethingElseIsUnchanged() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationException("Error on saving the record", new IllegalStateException("boom")), "ctx", null, false);

    assertThat(mapped.getStatus().getCode()).isNotEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(mapped.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(DatabaseOperationException.class.getName());
    assertThat(mapped.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isNull();
  }

  // Regression: #10089
  @Test
  void theAdminServiceFindsAWrappedRefusalToo() {
    final StatusException mapped = adminService().toStatus("createUser",
        new DatabaseOperationException("Error on saving the user", refusal(LEADER)));

    assertRedirect(mapped.getStatus(), mapped.getTrailers());
  }

  // Regression: #10089
  @Test
  void theAdminServiceStillAnswersAnUnrelatedWrapperAsBefore() {
    final StatusException mapped = adminService().toStatus("createUser",
        new DatabaseOperationException("Error on saving the user", new IllegalStateException("boom")));

    assertThat(mapped.getStatus().getCode()).isNotEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  /*
   * Issue #10090: {@code VectorSearch}, {@code HybridSearch} and {@code FullTextSearch} answered a failure through
   * {@code toSearchStatus}, which took only the classification of {@link GrpcErrorMapper} and none of what the mapper does
   * around it. No {@code arcadedb-exception-class} trailer went out, so a search stopped by {@code terminate query} (or by the
   * client cancelling the call, which the admission interceptor turns into the same termination) reached the driver as a
   * generic {@code RemoteException} instead of the {@code QueryTerminatedException} that tells it not to retry (#9689), a
   * missing index and a missing record collapsed onto one type, and a leader refusal raised while the transaction opened
   * carried neither {@code FAILED_PRECONDITION} nor the leader's address.
   * <p>
   * What a client acts on is the status, the class trailer and the redirect trailers together, so each case asserts the
   * answer against the one the same failure earns from {@code mapError}, the mapper entry point the other RPCs answer through.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  private ArcadeDbGrpcService service(final boolean production) {
    final ContextConfiguration configuration = new ContextConfiguration();
    if (production)
      configuration.setValue(GlobalConfiguration.SERVER_MODE, "production");
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, configuration);
    // No reaper: nothing here opens a transaction
    return new ArcadeDbGrpcService("unused", server, 0L, 0L, 0L);
  }

  private static String exceptionClassOf(final StatusException e) {
    return e.getTrailers() != null ? e.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY) : null;
  }

  // Regression: #10090
  @Test
  void aTerminatedSearchTellsTheDriverNotToRetry() {
    final StatusException status = service(false).toSearchStatus("VectorSearch", new QueryTerminatedException("terminated by admin"));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(status)).isEqualTo(QueryTerminatedException.class.getName());
  }

  // Regression: #10090
  @Test
  void aTerminationTheEngineWrappedIsStillATermination() {
    final StatusException status = service(false).toSearchStatus("HybridSearch",
        new DatabaseOperationException("stopped", new QueryTerminatedException("terminated by admin")));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(status)).as("named for the termination, not for the wrapper")
        .isEqualTo(QueryTerminatedException.class.getName());
  }

  // Regression: #10090
  @Test
  void aMissingIndexAndAMissingRecordKeepTheirOwnTypes() {
    final ArcadeDbGrpcService service = service(false);

    final StatusException schema = service.toSearchStatus("FullTextSearch", new SchemaException("Index 'T[text]' not found"));
    final StatusException record = service.toSearchStatus("FullTextSearch", new RecordNotFoundException("gone", null));

    assertThat(schema.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(record.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(exceptionClassOf(schema)).isEqualTo(SchemaException.class.getName());
    assertThat(exceptionClassOf(record)).isEqualTo(RecordNotFoundException.class.getName());
  }

  // Regression: #10090
  @Test
  void aLeaderRefusalWhileTheTransactionOpensNamesTheLeader() {
    final StatusException status = service(false).toSearchStatus("VectorSearch",
        new DatabaseOperationException("begin", new ServerIsNotTheLeaderException("This server is not the leader", LEADER)));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(exceptionClassOf(status)).isEqualTo(ServerIsNotTheLeaderException.class.getName());
    assertThat(status.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo(LEADER);
  }

  // Regression: #10090
  /** The bounds contract of the search RPCs: the offending argument is named, and the code is the caller's mistake. */
  @Test
  void aBoundCrossedStaysInvalidArgumentWithItsMessage() {
    final StatusException status = service(false).toSearchStatus("VectorSearch", new IllegalArgumentException("'k' must be between 1 and 10"));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getStatus().getDescription()).contains("'k' must be between 1 and 10");
    assertThat(exceptionClassOf(status)).isEqualTo(IllegalArgumentException.class.getName());
  }

  // Regression: #10090
  @Test
  void aFailureWrappedByATransactionExecutorIsMappedForWhatItCarries() {
    final StatusException status = service(false).toSearchStatus("VectorSearch",
        new ExecutionException(new QueryTerminatedException("terminated by admin")));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(status)).isEqualTo(QueryTerminatedException.class.getName());
  }

  // Regression: #10090
  @Test
  void everyFailureIsAnsweredLikeTheOtherRpcsAnswerIt() {
    final ArcadeDbGrpcService service = service(false);
    final Exception[] failures = { new QueryTerminatedException("terminated"), new SchemaException("no index"),
        new RecordNotFoundException("gone", null), new IllegalArgumentException("bad"), new IllegalStateException("boom"),
        new ServerIsNotTheLeaderException("not the leader", LEADER) };

    for (final Exception failure : failures) {
      final StatusException search = service.toSearchStatus("VectorSearch", failure);
      final StatusRuntimeException reference = GrpcErrorMapper.toStatusRuntimeException(failure, "VectorSearch", null, false);

      assertThat(search.getStatus().getCode()).as("code for %s", failure).isEqualTo(reference.getStatus().getCode());
      assertThat(exceptionClassOf(search)).as("class trailer for %s", failure)
          .isEqualTo(reference.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isNotNull();
    }
  }

  // Regression: #10090
  /** A sentence this server wrote about the request is not engine text, and is not concealed (PR #7755 review). */
  @Test
  void aSecurityRefusalKeepsItsSentenceEvenInProduction() {
    final StatusException status = service(true).toSearchStatus("VectorSearch", new ServerSecurityException("User 'u' cannot read 'db'"));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getStatus().getDescription()).contains("User 'u' cannot read 'db'");
  }

  // Regression: #10090
  @Test
  void anAlreadyMappedStatusPassesThroughWithItsTrailers() {
    final ArcadeDbGrpcService service = service(false);
    final Metadata trailers = new Metadata();
    trailers.put(GrpcErrorMapper.EXCEPTION_CLASS_KEY, "x.Y");

    final StatusException checked = service.toSearchStatus("VectorSearch", new StatusException(Status.RESOURCE_EXHAUSTED, trailers));
    final StatusException unchecked = service.toSearchStatus("VectorSearch",
        new StatusRuntimeException(Status.RESOURCE_EXHAUSTED, trailers));

    assertThat(checked.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    assertThat(exceptionClassOf(checked)).isEqualTo("x.Y");
    assertThat(unchecked.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    assertThat(exceptionClassOf(unchecked)).isEqualTo("x.Y");
  }

  // Regression: #10090
  /** Concealment is the other thing the mapper does that the bare classification did not: the engine text stays home. */
  @Test
  void productionConcealsEngineTextButKeepsWhatTheDriverActsOn() {
    final StatusException status = service(true).toSearchStatus("VectorSearch",
        new SchemaException("Index '/var/db/secret/T[text]' not found"));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(status.getStatus().getDescription()).doesNotContain("/var/db/secret");
    assertThat(exceptionClassOf(status)).isEqualTo(SchemaException.class.getName());
  }

  /*
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
    // The statement an admin RPC runs, refused by the database (issue #12464): the ALIGN DATABASE refusal of #11796 and its
    // supertype, which is what the arm names
    refusals.put(new CommandSemanticException(AlignDatabaseStatement.ALIGN_DATABASE_NOT_SUPPORTED, new UnsupportedOperationException()),
        Status.Code.INVALID_ARGUMENT);
    refusals.put(new CommandParsingException("Statement 'align database' is not valid here"), Status.Code.INVALID_ARGUMENT);
    refusals.put(new NeedRetryException("Resync in progress"), Status.Code.UNAVAILABLE);
    refusals.put(new QuorumNotReachedException("Quorum not reached"), Status.Code.UNAVAILABLE);
    refusals.put(new ConcurrentModificationException("Concurrent modification on page 3"), Status.Code.UNAVAILABLE);
    return refusals;
  }

  // Regression: #10158
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

  // Regression: #10158
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

  // Regression: #10158
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

  // Regression: #10158
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

  // Regression: #10158
  @Test
  void theTestCoversEveryArmOfTheLadder() {
    // A new arm added to the table without a case here fails this, so the assertion above stays over the whole ladder
    final List<Class<? extends Throwable>> covered = adminRefusals().keySet().stream()
        .<Class<? extends Throwable>>map(Exception::getClass).toList();

    for (final ArcadeDbGrpcAdminService.RefusalArm arm : ArcadeDbGrpcAdminService.REFUSAL_ARMS)
      assertThat(covered).as("a case for the %s arm", arm.type().getSimpleName()).contains(arm.type());
  }

  // Regression: #10158
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

  // Regression: #10158
  @Test
  void aDuplicatedKeyThatRanIntoAConflictIsRetryableAsErrorCategoryRanksIt() {
    // Pinned on purpose (PR #10174 review): the data-plane mapper answers by ErrorCategory, where RETRY outranks
    // DUPLICATED_KEY whatever the depth. DuplicatedKeyException takes no cause, so the engine never builds this chain
    final DuplicatedKeyException duplicate = new DuplicatedKeyException("User[email]", "[x]", null);
    duplicate.initCause(new ConcurrentModificationException("conflict"));

    assertThat(GrpcErrorMapper.toStatusRuntimeException(duplicate, "ctx", null, false).getStatus().getCode())
        .isEqualTo(Status.Code.ABORTED);
  }

  // Regression: #10158
  @Test
  void aStatementTerminatedOnRequestIsCancelledAndNeverRetryable() {
    // Walking the chain for NeedRetryException must not turn a termination into a retry: the rollback of a terminated
    // statement can run into a conflict, and ErrorCategory decides TERMINATED before anything else for that reason
    final StatusException mapped = adminService(false).toStatus("importDatabase", new CommandExecutionException("Import failed",
        terminated(new ConcurrentModificationException("conflict on rollback"))));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(mapped.getTrailers())).isEqualTo(QueryTerminatedException.class.getName());
  }

  // Regression: #10158
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

  // Regression: #10158
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

  // Regression: #10158
  @Test
  void anUnclassifiedFailureInsideAWrapperIsStillAConcealedInternalError() {
    final StatusException mapped = adminService(true).toStatus("createUser",
        new DatabaseOperationException("Error on saving the user", new IOException("/var/lib/arcadedb/config/users.jsonl")));

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    assertThat(mapped.getStatus().getDescription()).isEqualTo("createUser: " + GrpcErrorMapper.CONCEALED_DESCRIPTION);
  }

  // Regression: #10158
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

  // Regression: #10158
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

  // Regression: #10158
  @Test
  void aWrappedMaintenanceSlotConflictIsStillAborted() {
    final DatabaseOperationInProgressException inProgress = new DatabaseOperationInProgressException("A backup is already running");

    for (final Map.Entry<String, Function<Exception, Exception>> wrapper : WRAPPERS.entrySet())
      assertThat(GrpcErrorMapper.toStatusRuntimeException(wrapper.getValue().apply(inProgress), "ctx", null, false).getStatus()
          .getCode()).as(wrapper.getKey()).isEqualTo(Status.Code.ABORTED);
  }

  // Regression: #10158
  @Test
  void aWrappedSchemaErrorNamesItsOwnTypeSoTheDriverDoesNotRebuildARecordNotFound() {
    // NOT_FOUND alone is rebuilt client-side as RecordNotFoundException: only the class-name trailer tells the two apart
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new CommandExecutionException("Error on executing command", new SchemaException("Type 'Missing' was not found")), "ctx", null,
        false);

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(exceptionClassOf(mapped.getTrailers())).isEqualTo(SchemaException.class.getName());
  }

  // Regression: #10158
  @Test
  void aTerminatedStatementThatRanIntoADuplicateIsStillCancelled() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(new CommandExecutionException("stopped",
        terminated(new DuplicatedKeyException("User[email]", "[x]", null))), "ctx", null,
        false);

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(mapped.getTrailers())).isEqualTo(QueryTerminatedException.class.getName());
  }

  // Regression: #10158
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

  // Regression: #10158
  /**
   * The guarantee that stops this coming back: the two ladders ask the cause chain, never their own {@code instanceof}
   * on the head frame. The only head test left is the pass-through of a status the handler already chose.
   */
  @Test
  void theStatusLaddersHoldNoHeadOnlyInstanceofArm() throws IOException {
    final Path sources = Path.of(System.getProperty("basedir", "."), "src/main/java/com/arcadedb/server/grpc");

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

  private ArcadeDbGrpcService searchService() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root.resolve("search"), new ContextConfiguration());
    // No reaper: nothing here opens a transaction
    return new ArcadeDbGrpcService("unused", server, 0L, 0L, 0L);
  }

  /*
   * Issue #12464: since #11796 the {@code ALIGN DATABASE} statement refuses with a {@link CommandSemanticException}, which HTTP
   * answers 400. The admin RPC {@code AlignDatabase} runs that statement through {@code ServerControlPlane.alignDatabase}, and
   * {@code ArcadeDbGrpcAdminService.toStatus} had no arm for a refused statement, so it answered {@code INTERNAL} - a server
   * fault - and in production mode concealed the sentence that says why. The refusal is the caller's, as the data plane's
   * {@code ExecuteCommand} already answers the same statement ({@code ErrorCategory.PARSING}): {@code INVALID_ARGUMENT}, with
   * the ArcadeDB-authored sentence kept in production mode as HTTP keeps it.
   * <p>
   * The failure is the one a real database raises, not one built by hand, so the test follows the chain the engine actually
   * puts around the refusal.
   */

  // Regression: #12464
  @Test
  void theAlignDatabaseRefusalIsARejectedRequestAndNotAServerFault() {
    final Exception refusal = alignDatabaseRefusal();

    final StatusException status = adminService(false).toStatus("alignDatabase", refusal);

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getStatus().getDescription()).contains(AlignDatabaseStatement.ALIGN_DATABASE_NOT_SUPPORTED);
  }

  // Regression: #12464
  @Test
  void productionModeKeepsTheSentenceOfTheAlignDatabaseRefusal() {
    final Exception refusal = alignDatabaseRefusal();

    final StatusException status = adminService(true).toStatus("alignDatabase", refusal);

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getStatus().getDescription())
        .as("the refusal is a sentence ArcadeDB wrote about the request, which HTTP keeps in production mode too")
        .contains(AlignDatabaseStatement.ALIGN_DATABASE_NOT_SUPPORTED)
        .doesNotContain(GrpcErrorMapper.CONCEALED_DESCRIPTION);
  }

  /** The failure {@code ServerControlPlane.alignDatabase} propagates: the statement it runs, refused by a real database. */
  private Exception alignDatabaseRefusal() {
    try (final DatabaseFactory factory = new DatabaseFactory(root.resolve("align").toString());
        final Database database = factory.create()) {
      try (final var rs = database.command("sql", "align database")) {
        fail("ALIGN DATABASE is refused by every database (issue #11796), but it ran");
      } catch (final CommandSemanticException e) {
        assertThat(e).hasMessageContaining(AlignDatabaseStatement.ALIGN_DATABASE_NOT_SUPPORTED);
        return e;
      }
      throw new AssertionError("unreachable");
    }
  }

  /*
   * Issue #12550: {@code ArcadeDbGrpcAdminService.toStatus} answered from its own table of exception types, and whatever no
   * arm named fell to the {@code INTERNAL} catch-all, while the data plane answers from {@link ErrorCategory}. A whole category
   * of refusals the caller can act on (a duplicated key, a missing type, a refused value, raised while an import or a restore
   * runs on the caller's data) therefore read as a server fault on the admin RPCs, was logged as one, and lost its sentence in
   * production mode. The table was patched one type at a time (#12464 added one cell), and the test ranged over the cells
   * somebody remembered.
   * <p>
   * The census below ranges over {@link ErrorCategory#values()}, the authority both services answer from: for every category
   * the admin service and {@link GrpcErrorMapper} agree, bare and wrapped, except for the arms that differ on purpose, which
   * are declared here and have to keep differing.
   */

  /**
   * The failures each category is decided on, at least one per category and one per shape the admin table could get wrong: a
   * type no arm names, and a type an arm of a JDK supertype names although the engine classifies it elsewhere.
   */
  private static Map<ErrorCategory, List<Exception>> failuresByCategory() {
    final Map<ErrorCategory, List<Exception>> failures = new EnumMap<>(ErrorCategory.class);
    failures.put(ErrorCategory.RETRY,
        List.of(new ConcurrentModificationException("Concurrent modification on page 3"), new QuorumNotReachedException("Quorum not reached"),
            new QueryAdmissionException("The server is busy"), new DatabaseIsReopeningException("Database 'shop' is being reopened"),
            new QueryHeapBudgetExceededException("The query heap budget is held by other queries")));
    failures.put(ErrorCategory.ARITHMETIC, List.of(new ArithmeticErrorException("Division by zero")));
    failures.put(ErrorCategory.DUPLICATED_KEY, List.of(new DuplicatedKeyException("User[email]", "[bob@example.com]", null)));
    failures.put(ErrorCategory.ALREADY_EXISTS, List.of(new AlreadyExistsException(AlreadyExistsException.Kind.TYPE, "Type 'User' already exists"),
        new EquivalentIndexException("An equivalent index 'User[email]' already exists")));
    failures.put(ErrorCategory.NOT_FOUND,
        List.of(new RecordNotFoundException("Record #12:0 not found", null), new DeletedEntityException("The node was deleted")));
    failures.put(ErrorCategory.SCHEMA, List.of(new SchemaException("Type 'Missing' was not found")));
    failures.put(ErrorCategory.SECURITY, List.of(new SecurityException("User 'bob' cannot read type 'Secret'")));
    failures.put(ErrorCategory.VALIDATION,
        List.of(new ValidationException("Property 'age' is mandatory"), new ObjectInUseException("Type 'User' holds records"),
            new TypeMismatchException("Expected a number"), new InvalidArgumentException("Argument 'k' must be positive"),
            new IllegalArgumentException("Database name is empty")));
    failures.put(ErrorCategory.PARSING, List.of(new CommandParsingException("Statement is not valid here"),
        new FullTextQueryParseException("Cannot parse 'a AND'", new IllegalStateException("eof")),
        new UnknownFunctionException("nope", "Unknown function 'nope'")));
    failures.put(ErrorCategory.TIMEOUT, List.of(new TimeoutException("Timeout on locking the database")));
    failures.put(ErrorCategory.TERMINATED, List.of(new QueryTerminatedException("terminated by admin")));
    failures.put(ErrorCategory.UNAVAILABLE, List.of(new DatabaseNotAvailableException("Database 'shop' is being restored"),
        new DatabaseIsClosedException("Database 'shop' is closed")));
    failures.put(ErrorCategory.DATABASE_NOT_FOUND, List.of(new DatabaseNotFoundException("Database 'ghost' does not exist")));
    failures.put(ErrorCategory.SERVER, List.of(new IllegalStateException("boom"), new CommandExecutionException("The backup archive could not be deleted")));
    return failures;
  }

  /**
   * The arms of the admin table that answer another status than the data plane ON PURPOSE, by the type of the arm, with the
   * status the admin service answers. The whole list: anything else that differs is the defect of issue #12550.
   * <ul>
   * <li>a {@link SecurityException} on an admin RPC is what {@code authenticate()} raises for refused credentials, so it is
   * {@code UNAUTHENTICATED}; on the data plane it is a query-time permission denial, {@code PERMISSION_DENIED}</li>
   * <li>a {@link NeedRetryException} out of an admin operation is a Raft entry that could not be committed, HTTP's 503 and
   * {@code UNAVAILABLE} (issue #7532); on the data plane it is the conflict of a commit, {@code ABORTED}</li>
   * </ul>
   */
  private static final Map<Class<? extends Throwable>, Status.Code> DELIBERATE_DIVERGENCES = Map.of(SecurityException.class,
      Status.Code.UNAUTHENTICATED, NeedRetryException.class, Status.Code.UNAVAILABLE);

  // Regression: #12550
  @Test
  void theAdminServiceAnswersEveryErrorCategoryAsTheDataPlaneDoes() {
    final ArcadeDbGrpcAdminService service = adminService(false);
    final Map<ErrorCategory, List<Exception>> failures = failuresByCategory();
    final Set<Class<? extends Throwable>> diverged = new HashSet<>();
    final Set<ErrorCategory> reached = EnumSet.noneOf(ErrorCategory.class);

    for (final ErrorCategory category : ErrorCategory.values()) {
      assertThat(failures.get(category)).as("ErrorCategory.%s has no failure in this census: add one, with the status the admin "
          + "service is to answer it", category).isNotNull().isNotEmpty();

      for (final Exception failure : failures.get(category)) {
        final String name = failure.getClass().getSimpleName();
        assertThat(ErrorCategory.of(failure)).as("%s is the failure of the category it is listed under", name).isEqualTo(category);
        reached.add(category);

        for (final Exception chain : List.of(failure, new CommandExecutionException("Error importing database 'shop'", failure))) {
          final String shape = chain == failure ? name : name + " inside a CommandExecutionException";
          final StatusException admin = service.toStatus("importDatabase", chain);
          final StatusRuntimeException dataPlane = GrpcErrorMapper.toStatusRuntimeException(chain, "importDatabase", null, false);

          final Class<? extends Throwable> divergence = deliberateDivergenceOf(failure);
          if (divergence != null) {
            assertThat(admin.getStatus().getCode()).as("%s is answered by the %s arm", shape, divergence.getSimpleName())
                .isEqualTo(DELIBERATE_DIVERGENCES.get(divergence)).isNotEqualTo(dataPlane.getStatus().getCode());
            diverged.add(divergence);
            continue;
          }

          assertThat(admin.getStatus().getCode()).as("%s (ErrorCategory.%s) on an admin RPC and on the data plane", shape, category)
              .isEqualTo(dataPlane.getStatus().getCode());
          if (category != ErrorCategory.SERVER)
            assertThat(admin.getStatus().getCode()).as("%s is the caller's to act on, not a server fault", shape)
                .isNotEqualTo(Status.Code.INTERNAL);
        }
      }
    }

    // The scan still reaches the authority, ErrorCategory: every category, and above all the two the admin table never named
    assertThat(reached).as("the census ranges over ErrorCategory.values()").containsExactlyInAnyOrder(ErrorCategory.values())
        .contains(ErrorCategory.SCHEMA, ErrorCategory.VALIDATION);
    assertThat(diverged).as("every declared divergence still diverges: one that no longer does is removed from the list")
        .containsExactlyInAnyOrderElementsOf(DELIBERATE_DIVERGENCES.keySet());
  }

  // Regression: #12550
  /**
   * A refusal answered by its category is named in the class-name trailer, as the data plane names it. Without the trailer the
   * Java client reads {@code ALREADY_EXISTS} from an admin RPC as a taken user, token or database name, and {@code NOT_FOUND}
   * as a missing record.
   */
  @Test
  void aRefusalAnsweredByItsCategoryIsNamedAsTheDataPlaneNamesIt() {
    final ArcadeDbGrpcAdminService service = adminService(false);

    for (final Exception failure : List.of(new DuplicatedKeyException("User[email]", "[bob@example.com]", null),
        new SchemaException("Type 'Missing' was not found"), new ValidationException("Property 'age' is mandatory"),
        new EquivalentIndexException("An equivalent index 'User[email]' already exists"), new TimeoutException("Timeout on locking"))) {
      final Exception wrapped = new CommandExecutionException("Error importing database 'shop'", failure);
      final StatusException admin = service.toStatus("importDatabase", wrapped);
      final StatusRuntimeException dataPlane = GrpcErrorMapper.toStatusRuntimeException(wrapped, "importDatabase", null, false);

      assertThat(exceptionClassOf(admin.getTrailers())).as(failure.getClass().getSimpleName())
          .isEqualTo(failure.getClass().getName()).isEqualTo(exceptionClassOf(dataPlane.getTrailers()));
      assertThat(admin.getTrailers().get(GrpcErrorMapper.NAME_TAKEN_KEY)).as("not a taken user, token or database name").isNull();
      assertThat(admin.getStatus().getDescription()).as(failure.getClass().getSimpleName()).isEqualTo(dataPlane.getStatus().getDescription());
    }
  }

  // Regression: #12550
  /** The path that reaches it today: {@code ServerControlPlane.runImport} wraps whatever the importer raises on the caller's data. */
  @Test
  void aDuplicatedKeyRaisedByAnImportIsAConflictAndNotAServerFault() {
    final Exception failure = new CommandExecutionException("Error importing database 'shop'",
        new DuplicatedKeyException("User[email]", "[bob@example.com]", null));

    final StatusException development = adminService(false).toStatus("importDatabase", failure);
    assertThat(development.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    // The index and the key travel in the trailers the client rebuilds the DuplicatedKeyException from, as on the data plane
    assertThat(development.getTrailers().get(GrpcErrorMapper.DUP_INDEX_KEY)).isEqualTo(GrpcErrorMapper.encodeTrailer("User[email]"));
    assertThat(development.getTrailers().get(GrpcErrorMapper.DUP_KEYS_KEY)).isEqualTo(GrpcErrorMapper.encodeTrailer("[bob@example.com]"));

    // Engine text, which carries a stored key value: concealed in production as the data plane conceals it, with the status
    // and the trailers a driver acts on kept
    final StatusException production = adminService(true).toStatus("importDatabase", failure);
    assertThat(production.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(production.getStatus().getDescription()).isEqualTo("importDatabase: " + GrpcErrorMapper.CONCEALED_DESCRIPTION)
        .doesNotContain("bob@example.com");
    assertThat(exceptionClassOf(production.getTrailers())).isEqualTo(DuplicatedKeyException.class.getName());
    assertThat(production.getTrailers().get(GrpcErrorMapper.DUP_KEYS_KEY))
        .isEqualTo(GrpcErrorMapper.encodeTrailer(ArcadeDBServer.CONCEALED_DUPLICATED_KEYS));
  }

  // Regression: #12550
  /**
   * An arm of a JDK type does not take a subtype the engine classifies elsewhere: an {@link EquivalentIndexException} is an
   * {@link IllegalArgumentException} by inheritance and {@code ALREADY_EXISTS} by {@link ErrorCategory}, which ranks it above
   * its supertype for that reason. A plain one, and one that only CARRIES another verdict in its cause, keep the arm.
   */
  @Test
  void anArmOfAJdkTypeDoesNotTakeASubtypeTheEngineClassifiesElsewhere() {
    final ArcadeDbGrpcAdminService service = adminService(false);

    assertThat(service.toStatus("importDatabase", new EquivalentIndexException("An equivalent index already exists")).getStatus().getCode())
        .isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(service.toStatus("createDatabase", new IllegalArgumentException("Database name is empty")).getStatus().getCode())
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(service.toStatus("createDatabase",
        new IllegalArgumentException("Cannot create database 'x'", new SchemaException("Type 'V' was not found"))).getStatus().getCode())
        .as("a failure raised as itself keeps its own arm whatever its cause carries (issue #10158)")
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  /** The type of the declared divergence that answers {@code failure}, or {@code null} when the category is to decide. */
  private static Class<? extends Throwable> deliberateDivergenceOf(final Exception failure) {
    for (final Class<? extends Throwable> type : DELIBERATE_DIVERGENCES.keySet())
      if (type.isInstance(failure))
        return type;
    return null;
  }

  private ArcadeDbGrpcAdminService adminService() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, new ContextConfiguration());
    return new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator());
  }

  private ArcadeDbGrpcAdminService adminService(final boolean production) {
    final ContextConfiguration configuration = new ContextConfiguration();
    if (production)
      configuration.setValue(GlobalConfiguration.SERVER_MODE, "production");
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root.resolve(production ? "production" : "development"),
        configuration);
    return new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator());
  }
}
