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

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Metrics interceptor for gRPC requests using Micrometer.
 * <p>
 * Meters are registered into the shared {@link MeterRegistry} handed in by the gRPC plugin (the
 * server's JVM-wide {@code io.micrometer.core.instrument.Metrics#globalRegistry}), so the same
 * exporters that scrape the rest of the server (Prometheus, OTLP, JMX, the Studio "Server" tab)
 * also see gRPC telemetry. Each meter carries a {@code method} tag (and, for errors/latency, a
 * {@code status} tag) instead of stuffing per-call values into call trailers, so operators can
 * slice request/error/duration by gRPC method and gRPC status code.
 */
class GrpcMetricsInterceptor implements ServerInterceptor {
  private static final String ARCADEDB_MESSAGE_PACKAGE = "com.arcadedb.server.grpc.";

  static final String REQUESTS_METER = "grpc.requests.total";
  static final String ERRORS_METER   = "grpc.errors.total";
  static final String DURATION_METER = "grpc.request.duration";

  private final MeterRegistry meterRegistry;

  GrpcMetricsInterceptor(final MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    final String methodName = call.getMethodDescriptor().getFullMethodName();
    final Timer.Sample sample = Timer.start(meterRegistry);

    Counter.builder(REQUESTS_METER)
        .description("Total number of gRPC requests")
        .tag("method", methodName)
        .register(meterRegistry)
        .increment();

    final ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
      // gRPC may invoke sendMessage and close on different threads, so publish the flag with
      // volatile to guarantee the value written in sendMessage is visible in close.
      private volatile boolean           inBandFailure = false;
      // The response type of a call never changes, so its "success" field is looked up once, at the first message. The
      // calls into a ServerCall are serialised by the gRPC contract, so these two need no publication of their own
      private          boolean           successFieldResolved;
      private          FieldDescriptor   successField;

      @Override
      public void sendMessage(final RespT message) {
        // Some RPCs answer a failure in-band, with success=false in a response closed with status OK: a vetoed
        // updateRecord or deleteRecord does (issue #11054), and executeCommand used to. Without this check each of them
        // would be counted as a success. The check is not a list of message types, which went stale as soon as a second
        // one joined the set: any response whose type declares a top-level boolean "success" field and sends it false is
        // an in-band failure, so an RPC added later is covered without being listed here.
        if (!inBandFailure && message instanceof Message response && isInBandFailure(response))
          inBandFailure = true;

        super.sendMessage(message);
      }

      private boolean isInBandFailure(final Message response) {
        if (!successFieldResolved) {
          successField = successField(response);
          successFieldResolved = true;
        }
        return successField != null && !(Boolean) response.getField(successField);
      }

      @Override
      public void close(final Status status, final Metadata trailers) {
        final String statusCode = status.getCode().toString();

        sample.stop(Timer.builder(DURATION_METER)
            .description("gRPC request duration")
            .tag("method", methodName)
            .tag("status", statusCode)
            .register(meterRegistry));

        if (!status.isOk() || inBandFailure)
          Counter.builder(ERRORS_METER)
              .description("Total number of gRPC errors")
              .tag("method", methodName)
              .tag("status", statusCode)
              .register(meterRegistry)
              .increment();

        super.close(status, trailers);
      }
    };

    return next.startCall(wrappedCall, headers);
  }

  /**
   * The top-level, non-repeated boolean {@code success} field of {@code response}'s type, or {@code null} when it has none. Only the
   * ArcadeDB messages are inspected: {@code getDescriptorForType()} makes protobuf build the message's field-accessor table
   * reflectively, which the native image registers for the ArcadeDB messages only, so asking a foreign response (the gRPC reflection
   * or health services') failed the RPC there with "Generated message class ... missing method".
   */
  static FieldDescriptor successField(final Message response) {
    if (!(response instanceof DynamicMessage) && !response.getClass().getName().startsWith(ARCADEDB_MESSAGE_PACKAGE))
      return null;
    final FieldDescriptor field = response.getDescriptorForType().findFieldByName("success");
    return field != null && !field.isRepeated() && field.getJavaType() == FieldDescriptor.JavaType.BOOLEAN ? field : null;
  }

  /**
   * Current value of the gRPC error counter, summed across all method/status tags. Exposed for
   * testing so that error accounting (including in-band command failures reported as
   * ExecuteCommandResponse.success=false) can be verified without reflection.
   */
  double getErrorCount() {
    return meterRegistry.find(ERRORS_METER).counters().stream().mapToDouble(Counter::count).sum();
  }

  /**
   * Current value of the gRPC request counter, summed across all method tags. Exposed for testing.
   */
  double getRequestCount() {
    return meterRegistry.find(REQUESTS_METER).counters().stream().mapToDouble(Counter::count).sum();
  }
}
