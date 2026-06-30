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

import com.arcadedb.server.ArcadeDBServer;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Metrics interceptor for gRPC requests using Micrometer
 */
class GrpcMetricsInterceptor implements ServerInterceptor {

  private final MeterRegistry meterRegistry;
  private final Counter       requestCounter;
  private final Counter       errorCounter;
  private final Timer         requestTimer;

  public GrpcMetricsInterceptor(ArcadeDBServer server) {
    // Try to get existing meter registry from server, or create a simple one
    this.meterRegistry = new SimpleMeterRegistry(); // In production, integrate with server's meter registry

    this.requestCounter = Counter.builder("grpc.requests.total")
        .description("Total number of gRPC requests")
        .register(meterRegistry);

    this.errorCounter = Counter.builder("grpc.errors.total")
        .description("Total number of gRPC errors")
        .register(meterRegistry);

    this.requestTimer = Timer.builder("grpc.request.duration")
        .description("gRPC request duration")
        .register(meterRegistry);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {

    String methodName = call.getMethodDescriptor().getFullMethodName();
    Timer.Sample sample = Timer.start(meterRegistry);

    requestCounter.increment();

    ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
      // gRPC may invoke sendMessage and close on different threads, so publish the flag with
      // volatile to guarantee the value written in sendMessage is visible in close.
      private volatile boolean inBandFailure = false;

      @Override
      public void sendMessage(RespT message) {
        // executeCommand reports execution failures in-band as ExecuteCommandResponse.success=false
        // while still closing the RPC with status OK (unlike executeQuery/createRecord, which call
        // onError). Without this check every command failure would be counted as a success. Detect
        // the in-band failure here so it is reflected in the error counter at close() time.
        if (message instanceof ExecuteCommandResponse response && !response.getSuccess())
          inBandFailure = true;

        super.sendMessage(message);
      }

      @Override
      public void close(Status status, Metadata trailers) {
        sample.stop(requestTimer);

        if (!status.isOk() || inBandFailure) {
          errorCounter.increment();
        }

        // Add metrics as trailers for observability
        trailers.put(Metadata.Key.of("grpc-metrics-method", Metadata.ASCII_STRING_MARSHALLER), methodName);
        trailers.put(Metadata.Key.of("grpc-metrics-status", Metadata.ASCII_STRING_MARSHALLER), status.getCode().toString());

        super.close(status, trailers);
      }
    };

    return next.startCall(wrappedCall, headers);
  }

  /**
   * Current value of the gRPC error counter. Exposed for testing so that error accounting
   * (including in-band command failures reported as ExecuteCommandResponse.success=false) can be
   * verified without reflection.
   */
  double getErrorCount() {
    return errorCounter.count();
  }

  /**
   * Current value of the gRPC request counter. Exposed for testing.
   */
  double getRequestCount() {
    return requestCounter.count();
  }
}
