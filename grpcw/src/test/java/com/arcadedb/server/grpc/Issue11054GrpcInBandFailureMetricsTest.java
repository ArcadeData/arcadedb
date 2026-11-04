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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.Status;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11054: {@link GrpcMetricsInterceptor} counted an in-band failure (a response closed with status OK that says
 * {@code success=false}) for one message type only, {@code ExecuteCommandResponse}. A vetoed {@code updateRecord} or
 * {@code deleteRecord} answers exactly that shape, so on a server whose listeners veto every write
 * {@code grpc.errors.total} stayed at zero while {@code grpc.requests.total} climbed. The check now reads the response's
 * own {@code success} field, whatever its type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11054GrpcInBandFailureMetricsTest {

  @Test
  void aVetoedUpdateIsCountedAsAnError() {
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getUpdateRecordMethod(),
        UpdateRecordResponse.newBuilder().setSuccess(false).setUpdated(false).build())).isEqualTo(1.0);
  }

  @Test
  void aVetoedDeleteIsCountedAsAnError() {
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getDeleteRecordMethod(),
        DeleteRecordResponse.newBuilder().setSuccess(false).setDeleted(false).build())).isEqualTo(1.0);
  }

  @Test
  void anInBandCommandFailureIsStillCountedAsAnError() {
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getExecuteCommandMethod(),
        ExecuteCommandResponse.newBuilder().setSuccess(false).build())).isEqualTo(1.0);
  }

  @Test
  void anAppliedWriteIsNotAnError() {
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getUpdateRecordMethod(),
        UpdateRecordResponse.newBuilder().setSuccess(true).setUpdated(true).build())).isZero();
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getDeleteRecordMethod(),
        DeleteRecordResponse.newBuilder().setSuccess(true).setDeleted(true).build())).isZero();
  }

  @Test
  void aCommitOfNoTransactionSaysSuccessAndIsNotAnError() {
    // success=true with committed=false: the call succeeded and had nothing to commit
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getCommitTransactionMethod(),
        CommitTransactionResponse.newBuilder().setSuccess(true).setCommitted(false).build())).isZero();
  }

  @Test
  void aResponseWithoutASuccessFieldIsNotAnError() {
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getExecuteQueryMethod(), ExecuteQueryResponse.getDefaultInstance())).isZero();
  }

  @Test
  void aNonOkStatusIsStillAnErrorWithoutAnyMessage() {
    final GrpcMetricsInterceptor interceptor = new GrpcMetricsInterceptor(new SimpleMeterRegistry());
    final ServerCall<Object, Object> call = start(interceptor, ArcadeDbServiceGrpc.getUpdateRecordMethod());
    call.close(Status.INTERNAL, new Metadata());
    assertThat(interceptor.getErrorCount()).isEqualTo(1.0);
  }

  @Test
  void everyResponseThatCarriesASuccessFlagIsCovered() {
    // The rule the counter follows, stated over the whole protocol: each response type of both services that declares a
    // "success" flag is one the interceptor reads, so a response type added later cannot be left out as these two were
    final FileDescriptor file = ArcadeDbProto.getDescriptor();
    final List<String> flagged = new ArrayList<>();
    for (final ServiceDescriptor service : file.getServices())
      for (final var method : service.getMethods()) {
        final Descriptor output = method.getOutputType();
        if (output.findFieldByName("success") == null)
          continue;
        flagged.add(output.getName());
        assertThat(GrpcMetricsInterceptor.successField(DynamicMessage.getDefaultInstance(output))).as(output.getName()).isNotNull();
      }

    assertThat(flagged).contains("UpdateRecordResponse", "DeleteRecordResponse", "ExecuteCommandResponse");
  }

  private static double errorsAfter(final MethodDescriptor<?, ?> method, final Message response) {
    final GrpcMetricsInterceptor interceptor = new GrpcMetricsInterceptor(new SimpleMeterRegistry());
    final ServerCall<Object, Object> call = start(interceptor, method);
    call.sendMessage(response);
    call.close(Status.OK, new Metadata());
    assertThat(interceptor.getRequestCount()).isEqualTo(1.0);
    return interceptor.getErrorCount();
  }

  /** The call the interceptor hands the handler, around a call that records nothing. */
  @SuppressWarnings("unchecked")
  private static ServerCall<Object, Object> start(final GrpcMetricsInterceptor interceptor, final MethodDescriptor<?, ?> method) {
    final ServerCall<Object, Object>[] wrapped = new ServerCall[1];
    interceptor.interceptCall(new NoOpCall((MethodDescriptor<Object, Object>) method), new Metadata(), (c, h) -> {
      wrapped[0] = c;
      return new ServerCall.Listener<>() {
      };
    });
    return wrapped[0];
  }

  private static final class NoOpCall extends ServerCall<Object, Object> {
    private final MethodDescriptor<Object, Object> method;

    private NoOpCall(final MethodDescriptor<Object, Object> method) {
      this.method = method;
    }

    @Override
    public void request(final int numMessages) {
    }

    @Override
    public void sendHeaders(final Metadata headers) {
    }

    @Override
    public void sendMessage(final Object message) {
    }

    @Override
    public void close(final Status status, final Metadata trailers) {
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public MethodDescriptor<Object, Object> getMethodDescriptor() {
      return method;
    }

    @Override
    public Attributes getAttributes() {
      return Attributes.EMPTY;
    }
  }
}
