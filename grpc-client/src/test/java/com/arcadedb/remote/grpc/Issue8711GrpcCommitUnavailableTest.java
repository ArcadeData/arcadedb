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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import io.grpc.Metadata;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8711: a gRPC {@code UNAVAILABLE} on commit does not prove the commit never landed, so it must not surface as
 * a {@link NeedRetryException} that {@code transaction()} re-runs. No server is required.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8711GrpcCommitUnavailableTest {

  @Test
  void statusOnlyUnavailableOnCommitIsAnUnknownOutcome() {
    final RuntimeException e = GrpcClientErrorMapper.toCommitException(Status.UNAVAILABLE.withDescription("channel dropped").asRuntimeException());

    assertThat(e).isInstanceOf(TransactionException.class).isNotInstanceOf(NeedRetryException.class);
    assertThat(e.getMessage()).contains("outcome is unknown");
  }

  @Test
  void otherOperationsKeepTheRetryableMapping() {
    assertThat(GrpcClientErrorMapper.toException(Status.UNAVAILABLE.asRuntimeException())).isInstanceOf(NeedRetryException.class);
  }

  @Test
  void aServerClassifiedRetryableErrorOnCommitKeepsItsType() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, "com.arcadedb.exception.ConcurrentModificationException");
    final RuntimeException e = GrpcClientErrorMapper.toCommitException(Status.ABORTED.withDescription("conflict").asRuntimeException(trailers));

    assertThat(e).isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  void aServerClassifiedUnavailableOnCommitKeepsItsType() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, "com.arcadedb.exception.NeedRetryException");
    final RuntimeException e = GrpcClientErrorMapper.toCommitException(Status.UNAVAILABLE.withDescription("refused").asRuntimeException(trailers));

    assertThat(e).isInstanceOf(NeedRetryException.class);
  }
}
