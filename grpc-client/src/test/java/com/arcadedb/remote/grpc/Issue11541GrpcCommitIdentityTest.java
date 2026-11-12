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
package com.arcadedb.remote.grpc;

import com.arcadedb.remote.RemoteDatabase;
import io.grpc.Metadata;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11541, gRPC side: {@link RemoteGrpcDatabase#commit()} resets the identity of the records created in the transaction
 * only when the exception the commit RPC is mapped to proves the commit was not applied. The mapping
 * ({@link GrpcClientErrorMapper#toCommitException}) and the verdict are composed here as {@code commit()} composes them, on
 * the failures the mapping tells apart. No server is required.
 */
class Issue11541GrpcCommitIdentityTest {

  @Test
  void aLostConnectionIsAnUnknownOutcomeAndKeepsTheIdentity() {
    assertThat(provesNotApplied(Status.UNAVAILABLE.withDescription("channel dropped"), null)).isFalse();
  }

  @Test
  void anExpiredDeadlineIsAnUnknownOutcomeAndKeepsTheIdentity() {
    assertThat(provesNotApplied(Status.DEADLINE_EXCEEDED.withDescription("deadline"), null)).isFalse();
  }

  @Test
  void anUntypedServerFaultKeepsTheIdentity() {
    // A ReplicationDispatchedTimeoutException is a SERVER fault on the wire: INTERNAL, with no type this client rebuilds
    final Metadata trailers = trailer("com.arcadedb.server.ha.raft.ReplicationDispatchedTimeoutException");
    assertThat(provesNotApplied(Status.INTERNAL.withDescription("dispatched, outcome unknown"), trailers)).isFalse();
  }

  @Test
  void anOutcomeTheServerCouldNotLearnKeepsTheIdentity() {
    final Metadata trailers = trailer("com.arcadedb.exception.TransactionOutcomeUnknownException");
    assertThat(provesNotApplied(Status.INTERNAL.withDescription("outcome unknown"), trailers)).isFalse();
  }

  @Test
  void aCommitThatLandedRemotelyKeepsTheIdentity() {
    final Metadata trailers = trailer("com.arcadedb.exception.TransactionCommittedRemotelyException");
    assertThat(provesNotApplied(Status.INTERNAL.withDescription("committed cluster-wide"), trailers)).isFalse();
  }

  @Test
  void aServerConflictResetsTheIdentity() {
    final Metadata trailers = trailer("com.arcadedb.exception.ConcurrentModificationException");
    assertThat(provesNotApplied(Status.ABORTED.withDescription("conflict"), trailers)).isTrue();
  }

  @Test
  void aDuplicatedKeyResetsTheIdentity() {
    final Metadata trailers = trailer("com.arcadedb.exception.DuplicatedKeyException");
    assertThat(provesNotApplied(Status.ALREADY_EXISTS.withDescription("duplicated key"), trailers)).isTrue();
  }

  @Test
  void aConnectionThatCouldNotBeEstablishedResetsTheIdentity() {
    assertThat(provesNotApplied(Status.UNAVAILABLE.withDescription("refused").withCause(new ConnectException("refused")), null))
        .isTrue();
  }

  private static boolean provesNotApplied(final Status status, final Metadata trailers) {
    return Verdict.of(GrpcClientErrorMapper.toCommitException(status.asRuntimeException(trailers)));
  }

  private static Metadata trailer(final String exceptionClass) {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, exceptionClass);
    return trailers;
  }

  /** Reaches the verdict {@code RemoteGrpcDatabase.commit()} inherits. Never instantiated. */
  private abstract static class Verdict extends RemoteDatabase {
    private Verdict() {
      super("localhost", 0, "db", "root", "password");
    }

    static boolean of(final Throwable failure) {
      return provesCommitNotApplied(failure);
    }
  }
}
