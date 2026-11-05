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

import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import io.grpc.Metadata;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11314: the gRPC client rebuilds a refused value as the {@link ValidationException} it was on the server, with the
 * constraint the server named in its trailer, and an unknown function as an {@link UnknownFunctionException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11314GrpcClientValidationKindTest {

  @Test
  void theKindTrailerIsRebuilt() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, ValidationException.class.getName());
    trailers.put(GrpcClientErrorMapper.VALIDATION_KIND_KEY, "MAX_LENGTH");

    final RuntimeException rebuilt = GrpcClientErrorMapper.toException(
        Status.INVALID_ARGUMENT.withDescription("The property 'T.p' contains more characters than 3 requested").asRuntimeException(trailers));

    assertThat(rebuilt).isInstanceOf(ValidationException.class).hasMessageContaining("more characters");
    assertThat(((ValidationException) rebuilt).getKind()).isEqualTo(ValidationException.Kind.MAX_LENGTH);
  }

  @Test
  void aServerThatSendsNoKindOrOneThisClientLacksRebuildsNone() {
    for (final String kind : new String[] { null, "SOME_NEWER_KIND" }) {
      final Metadata trailers = new Metadata();
      trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, ValidationException.class.getName());
      if (kind != null)
        trailers.put(GrpcClientErrorMapper.VALIDATION_KIND_KEY, kind);

      final RuntimeException rebuilt = GrpcClientErrorMapper.toException(Status.INVALID_ARGUMENT.withDescription("refused").asRuntimeException(trailers));
      assertThat(rebuilt).isInstanceOf(ValidationException.class);
      assertThat(((ValidationException) rebuilt).getKind()).as("%s", kind).isNull();
    }
  }

  @Test
  void anUnknownFunctionIsRebuiltAsItself() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, UnknownFunctionException.class.getName());

    final RuntimeException rebuilt = GrpcClientErrorMapper.toException(
        Status.INVALID_ARGUMENT.withDescription("Unknown function name 'foo'").asRuntimeException(trailers));

    assertThat(rebuilt).isInstanceOf(UnknownFunctionException.class).hasMessageContaining("foo");
  }
}
