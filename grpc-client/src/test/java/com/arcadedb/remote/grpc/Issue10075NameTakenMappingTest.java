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

import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.remote.RemoteAlreadyExistsException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10075: an admin RPC that finds its name taken answers {@code ALREADY_EXISTS} with no exception-class trailer, the
 * very shape the generic mapping reads as a duplicate key on the data plane. The two must stay apart: a taken name is a
 * {@link RemoteAlreadyExistsException}, a duplicate key (identified by its trailer) is still a
 * {@link DuplicatedKeyException}.
 */
class Issue10075NameTakenMappingTest {

  @Test
  void aTrailerlessAlreadyExistsIsATakenName() {
    final StatusException wire = Status.ALREADY_EXISTS.withDescription("User 'bob' already exists").asException();

    assertThat(GrpcClientErrorMapper.isNameTaken(wire)).isTrue();
    final RemoteAlreadyExistsException mapped = GrpcClientErrorMapper.toNameTakenException(wire);
    assertThat(mapped.getMessage()).isEqualTo("User 'bob' already exists");
    assertThat(mapped.getCause()).isSameAs(wire);
  }

  @Test
  void aTrailerlessAlreadyExistsWithoutADescriptionStillCarriesTheStatusName() {
    assertThat(GrpcClientErrorMapper.toNameTakenException(Status.ALREADY_EXISTS.asException()).getMessage())
        .isEqualTo("ALREADY_EXISTS");
  }

  @Test
  void aDuplicateKeyTrailerIsNotATakenName() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, DuplicatedKeyException.class.getName());
    final StatusException wire = Status.ALREADY_EXISTS.withDescription("dup").asException(trailers);

    assertThat(GrpcClientErrorMapper.isNameTaken(wire)).isFalse();
    // and the generic mapping keeps rebuilding it as the duplicate key it is
    assertThat(GrpcClientErrorMapper.toException(wire)).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void anyOtherTypedAlreadyExistsKeepsItsOwnType() {
    // a later server may name its own conflict in the trailer: that name, not this rule, decides the type
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, "com.arcadedb.server.SomeFutureConflictException");

    assertThat(GrpcClientErrorMapper.isNameTaken(Status.ALREADY_EXISTS.withDescription("x").asException(trailers))).isFalse();
  }

  @Test
  void otherStatusesAreNotATakenName() {
    assertThat(GrpcClientErrorMapper.isNameTaken(Status.NOT_FOUND.withDescription("User 'bob' not found").asException())).isFalse();
    assertThat(GrpcClientErrorMapper.isNameTaken(Status.ABORTED.withDescription("busy").asException())).isFalse();
    assertThat(GrpcClientErrorMapper.isNameTaken(new RuntimeException("not a status"))).isFalse();
  }
}
