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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11314 on gRPC: a value a declared constraint refuses carries the constraint in a trailer, as a taken name carries the
 * kind of object holding it (#10153), so the client rebuilds the {@link ValidationException} with its kind; and a call of an
 * unknown SQL function is the caller's {@code INVALID_ARGUMENT}, where it was the {@code INTERNAL} of a server fault.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11314GrpcValidationKindTest {

  @Test
  void aViolatedConstraintIsInvalidArgumentWithTheKindTrailer() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new CommandExecutionException("Error on executing",
        new ValidationException(ValidationException.Kind.NOT_NULL, "The property 'T.p' cannot be null")), "Command failed");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(ValidationException.class.getName());
    assertThat(sre.getTrailers().get(GrpcErrorMapper.VALIDATION_KIND_KEY)).isEqualTo("NOT_NULL");
  }

  @Test
  void aRefusalOfNoDeclaredConstraintCarriesNoKind() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new ValidationException("refused"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.VALIDATION_KIND_KEY)).isNull();
  }

  @Test
  void anUnknownFunctionIsInvalidArgument() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new UnknownFunctionException("foo", "Unknown function name 'foo'"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(UnknownFunctionException.class.getName());
  }
}
