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
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue10089GrpcWrappedLeaderRefusalTest {
  private static final String LEADER = "leader-host:2480";

  @TempDir
  Path root;

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

  @Test
  void theBareRefusalIsTheReference() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(refusal(LEADER), "ctx", null, false);

    assertRedirect(mapped.getStatus(), mapped.getTrailers());
  }

  @Test
  void aRefusalOneFrameDownIsAnsweredAsARedirect() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationException("Error on saving the record", refusal(LEADER)), "ctx", null, false);

    assertRedirect(mapped.getStatus(), mapped.getTrailers());
  }

  @Test
  void aRefusalSeveralFramesDownAndBehindAnExecutionExceptionIsAnsweredAsARedirect() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new ExecutionException(new TransactionException("commit", new DatabaseOperationException("listener", refusal(LEADER)))), "ctx",
        null, false);

    assertRedirect(mapped.getStatus(), mapped.getTrailers());
  }

  @Test
  void theConcealedAnswerStillNamesTheLeader() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationException("Error on saving the record", refusal(LEADER)), "ctx", null, true);

    assertRedirect(mapped.getStatus(), mapped.getTrailers());
    assertThat(mapped.getStatus().getDescription()).as("the engine text is concealed, the address is not")
        .doesNotContain("Error on saving the record");
  }

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

  @Test
  void aStatementTerminatedOnRequestKeepsItsVerdictWhateverItWraps() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationException("stopped", new QueryTerminatedException("terminated by admin")), "ctx", null, false);

    assertThat(mapped.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(mapped.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(QueryTerminatedException.class.getName());
  }

  @Test
  void aWrapperAroundSomethingElseIsUnchanged() {
    final StatusRuntimeException mapped = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationException("Error on saving the record", new IllegalStateException("boom")), "ctx", null, false);

    assertThat(mapped.getStatus().getCode()).isNotEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(mapped.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(DatabaseOperationException.class.getName());
    assertThat(mapped.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isNull();
  }

  @Test
  void theAdminServiceFindsAWrappedRefusalToo() {
    final StatusException mapped = adminService().toStatus("createUser",
        new DatabaseOperationException("Error on saving the user", refusal(LEADER)));

    assertRedirect(mapped.getStatus(), mapped.getTrailers());
  }

  @Test
  void theAdminServiceStillAnswersAnUnrelatedWrapperAsBefore() {
    final StatusException mapped = adminService().toStatus("createUser",
        new DatabaseOperationException("Error on saving the user", new IllegalStateException("boom")));

    assertThat(mapped.getStatus().getCode()).isNotEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  private ArcadeDbGrpcAdminService adminService() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, new ContextConfiguration());
    return new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator());
  }
}
