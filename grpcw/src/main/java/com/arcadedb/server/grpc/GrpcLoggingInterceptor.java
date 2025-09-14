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

import com.arcadedb.log.LogManager;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.logging.Level;

/**
 * Logging interceptor for gRPC requests with compression detection
 */
class GrpcLoggingInterceptor implements ServerInterceptor {

  private static final Metadata.Key<String> GRPC_ENCODING_KEY        =
      Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> GRPC_ACCEPT_ENCODING_KEY =
      Metadata.Key.of("grpc-accept-encoding", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {

    String methodName = call.getMethodDescriptor().getFullMethodName();
    long startTime = System.currentTimeMillis();

    // Check if client sent compressed request
    String requestEncoding = headers.get(GRPC_ENCODING_KEY);
    String acceptEncoding = headers.get(GRPC_ACCEPT_ENCODING_KEY);
    boolean requestCompressed = requestEncoding != null && !requestEncoding.equals("identity");
    boolean clientAcceptsCompression = acceptEncoding != null && acceptEncoding.contains("gzip");

    LogManager.instance().log(this, Level.FINE, "gRPC call started: %s (request compression: %s, client accepts: %s)",
        methodName,
        requestCompressed ? requestEncoding : "none",
        clientAcceptsCompression ? acceptEncoding : "none");

    ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
      private String responseCompression = "none";

      @Override
      public void sendHeaders(Metadata headers) {
        // Check if we're sending compressed response
        String encoding = headers.get(GRPC_ENCODING_KEY);
        if (encoding != null && !encoding.equals("identity")) {
          responseCompression = encoding;
        }
        super.sendHeaders(headers);
      }

      @Override
      public void close(Status status, Metadata trailers) {
        long duration = System.currentTimeMillis() - startTime;

        // Add compression info to trailers for client visibility
        trailers.put(Metadata.Key.of("x-grpc-compression-used", Metadata.ASCII_STRING_MARSHALLER),
            responseCompression);
        trailers.put(Metadata.Key.of("x-grpc-request-compressed", Metadata.ASCII_STRING_MARSHALLER),
            String.valueOf(requestCompressed));

        if (status.isOk()) {
          LogManager.instance().log(GrpcLoggingInterceptor.this, Level.FINE,
              "gRPC call completed: %s (%sms, req-compression: %s, resp-compression: %s)",
              methodName, duration,
              requestCompressed ? requestEncoding : "none",
              responseCompression);
        } else {
          LogManager.instance()
              .log(GrpcLoggingInterceptor.this, Level.WARNING, "gRPC call failed: %s - %s (%sms)", methodName, status, duration);
        }
        super.close(status, trailers);
      }
    };

    return next.startCall(wrappedCall, headers);
  }
}
