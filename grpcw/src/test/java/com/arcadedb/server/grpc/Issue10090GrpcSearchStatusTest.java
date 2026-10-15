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
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.ServerSecurityException;
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
class Issue10090GrpcSearchStatusTest {
  private static final String LEADER = "leader-host:2480";

  @TempDir
  Path root;

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

  @Test
  void aTerminatedSearchTellsTheDriverNotToRetry() {
    final StatusException status = service(false).toSearchStatus("VectorSearch", new QueryTerminatedException("terminated by admin"));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(status)).isEqualTo(QueryTerminatedException.class.getName());
  }

  @Test
  void aTerminationTheEngineWrappedIsStillATermination() {
    final StatusException status = service(false).toSearchStatus("HybridSearch",
        new DatabaseOperationException("stopped", new QueryTerminatedException("terminated by admin")));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(status)).as("named for the termination, not for the wrapper")
        .isEqualTo(QueryTerminatedException.class.getName());
  }

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

  @Test
  void aLeaderRefusalWhileTheTransactionOpensNamesTheLeader() {
    final StatusException status = service(false).toSearchStatus("VectorSearch",
        new DatabaseOperationException("begin", new ServerIsNotTheLeaderException("This server is not the leader", LEADER)));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(exceptionClassOf(status)).isEqualTo(ServerIsNotTheLeaderException.class.getName());
    assertThat(status.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo(LEADER);
  }

  /** The bounds contract of the search RPCs: the offending argument is named, and the code is the caller's mistake. */
  @Test
  void aBoundCrossedStaysInvalidArgumentWithItsMessage() {
    final StatusException status = service(false).toSearchStatus("VectorSearch", new IllegalArgumentException("'k' must be between 1 and 10"));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getStatus().getDescription()).contains("'k' must be between 1 and 10");
    assertThat(exceptionClassOf(status)).isEqualTo(IllegalArgumentException.class.getName());
  }

  @Test
  void aFailureWrappedByATransactionExecutorIsMappedForWhatItCarries() {
    final StatusException status = service(false).toSearchStatus("VectorSearch",
        new ExecutionException(new QueryTerminatedException("terminated by admin")));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(exceptionClassOf(status)).isEqualTo(QueryTerminatedException.class.getName());
  }

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

  /** A sentence this server wrote about the request is not engine text, and is not concealed (PR #7755 review). */
  @Test
  void aSecurityRefusalKeepsItsSentenceEvenInProduction() {
    final StatusException status = service(true).toSearchStatus("VectorSearch", new ServerSecurityException("User 'u' cannot read 'db'"));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getStatus().getDescription()).contains("User 'u' cannot read 'db'");
  }

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

  /** Concealment is the other thing the mapper does that the bare classification did not: the engine text stays home. */
  @Test
  void productionConcealsEngineTextButKeepsWhatTheDriverActsOn() {
    final StatusException status = service(true).toSearchStatus("VectorSearch",
        new SchemaException("Index '/var/db/secret/T[text]' not found"));

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(status.getStatus().getDescription()).doesNotContain("/var/db/secret");
    assertThat(exceptionClassOf(status)).isEqualTo(SchemaException.class.getName());
  }
}
