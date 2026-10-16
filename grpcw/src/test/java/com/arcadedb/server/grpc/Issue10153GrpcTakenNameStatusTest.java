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

import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10153 on the gRPC data plane: a {@code CREATE} on a taken name was {@code INTERNAL} (a {@code CommandExecutionException})
 * or {@code NOT_FOUND} (a {@code SchemaException} - the opposite of what happened). It is {@code ALREADY_EXISTS} now, named in the
 * class trailer as a {@link SchemaException}, its supertype, so a client that predates {@link AlreadyExistsException} rebuilds a
 * SchemaException rather than reading the status as a duplicate key, and with the kind in a trailer of its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10153GrpcTakenNameStatusTest {

  @Test
  void aTakenNameIsAlreadyExistsWithTheKindTrailer() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new AlreadyExistsException(AlreadyExistsException.Kind.PROPERTY, "Property 'V.p' already exists"), "Command failed");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    final Metadata trailers = sre.getTrailers();
    assertThat(trailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(SchemaException.class.getName());
    assertThat(trailers.get(GrpcErrorMapper.ALREADY_EXISTS_KIND_KEY)).isEqualTo("PROPERTY");
    assertThat(trailers.get(GrpcErrorMapper.DUP_INDEX_KEY)).as("not a duplicate key").isNull();
    assertThat(sre.getStatus().getDescription()).contains("Property 'V.p' already exists");
  }

  @Test
  void aWrappedTakenNameIsNamedAsTheConflictNotAsTheWrapper() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new CommandExecutionException("Error on executing the script", new TypeAlreadyExistsException("V")), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(SchemaException.class.getName());
    assertThat(sre.getTrailers().get(GrpcErrorMapper.ALREADY_EXISTS_KIND_KEY)).isEqualTo("TYPE");
  }

  @Test
  void productionModeKeepsTheLabelOfTheConflict() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new AlreadyExistsException(AlreadyExistsException.Kind.MATERIALIZED_VIEW, "Materialized view 'secret_name' already exists"),
        null, null, true);

    assertThat(sre.getStatus().getDescription()).isEqualTo("Materialized view already exists");
  }

  @Test
  void aMissingTypeStaysNotFound() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new SchemaException("Type 'X' was not found"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.ALREADY_EXISTS_KIND_KEY)).isNull();
  }
}
