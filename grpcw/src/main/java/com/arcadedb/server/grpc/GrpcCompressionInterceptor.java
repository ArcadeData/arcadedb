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
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.util.logging.Level;

/**
 * Compression interceptor that can force compression based on configuration
 */
class GrpcCompressionInterceptor implements ServerInterceptor {

  // Context key to store compression info
  public static final  Context.Key<CompressionInfo> COMPRESSION_KEY      = Context.key("compression-info");
  private static final Metadata.Key<String>         GRPC_ACCEPT_ENCODING = Metadata.Key.of("grpc-accept-encoding",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String>         GRPC_ENCODING        = Metadata.Key.of("grpc-encoding",
      Metadata.ASCII_STRING_MARSHALLER);
  private final        boolean                      forceCompression;
  private final        String                       compressionType;
  private final        int                          minMessageSizeForCompression;

  public GrpcCompressionInterceptor(boolean forceCompression, String compressionType, int minMessageSizeBytes) {
    this.forceCompression = forceCompression;
    this.compressionType = compressionType != null ? compressionType : "gzip";
    this.minMessageSizeForCompression = minMessageSizeBytes;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {

    String methodName = call.getMethodDescriptor().getFullMethodName();

    // Check if request is compressed
    String requestEncoding = headers.get(Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER));
    boolean requestCompressed = requestEncoding != null && !requestEncoding.equals("identity");

    final boolean clientAcceptsGzip =
        headers.get(GRPC_ACCEPT_ENCODING) != null && headers.get(GRPC_ACCEPT_ENCODING).contains("gzip");

    // Store compression info in context
    CompressionInfo compressionInfo = new CompressionInfo(requestCompressed, requestEncoding);
    Context context = Context.current().withValue(COMPRESSION_KEY, compressionInfo);

    // Wrap the call to control compression
    ServerCall<ReqT, RespT> compressedCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
      private boolean compressionSet = false;

      @Override
      public void sendHeaders(Metadata responseHeaders) {

        if (clientAcceptsGzip && !compressionSet) {

          // Pick the compressor for this call before headers go out
          super.setCompression("gzip");
          setMessageCompression(true);

          LogManager.instance()
              .log(GrpcCompressionInterceptor.this, Level.FINE, "Forced %s compression for method: %s", compressionType,
                  methodName);

          compressionSet = true;
        }

        super.sendHeaders(responseHeaders);
      }

      @Override
      public void sendMessage(RespT message) {

        if (!compressionSet && forceCompression) {

          // Force compression for this response

          if (this.getMethodDescriptor().getType().serverSendsOneMessage()) {
            // For unary calls, we can set compression
            setMessageCompression(true);
            compressionSet = true;

            LogManager.instance()
                .log(GrpcCompressionInterceptor.this, Level.FINE, "Forced %s compression for method: %s", compressionType,
                    methodName);
          }
        }

        super.sendMessage(message);
      }
    };

    return Contexts.interceptCall(context, compressedCall, headers, next);
  }

  /**
   * Helper class to store compression information
   */
  public static class CompressionInfo {
    public final boolean requestCompressed;
    public final String  requestEncoding;

    public CompressionInfo(boolean requestCompressed, String requestEncoding) {
      this.requestCompressed = requestCompressed;
      this.requestEncoding = requestEncoding;
    }
  }
}
