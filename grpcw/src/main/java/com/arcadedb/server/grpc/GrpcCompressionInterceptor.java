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

import io.grpc.*;

import java.util.logging.Level;

/**
 * Compression interceptor that forces server-side response compression using the configured codec, but
 * only when the client advertised support for that codec in its {@code grpc-accept-encoding} header and the
 * codec is registered server-side. This guarantees the client is always able to decode the response.
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

  public GrpcCompressionInterceptor(final boolean forceCompression, final String compressionType) {
    this.forceCompression = forceCompression;
    this.compressionType = compressionType != null ? compressionType : "gzip";
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call, final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    final String methodName = call.getMethodDescriptor().getFullMethodName();

    // Check if request is compressed
    final String requestEncoding = headers.get(GRPC_ENCODING);
    final boolean requestCompressed = requestEncoding != null && !"identity".equals(requestEncoding);

    // Only force compression when the client advertised support for the configured codec, otherwise the
    // response would be undecodable on the client side.
    final boolean clientAcceptsCodec = clientAcceptsEncoding(headers, compressionType);

    // The compressor must also be registered server-side, otherwise gRPC would fail when sending the message.
    final boolean codecAvailable = CompressorRegistry.getDefaultInstance().lookupCompressor(compressionType) != null;

    final boolean compressResponse = forceCompression && clientAcceptsCodec && codecAvailable;

    if (forceCompression && clientAcceptsCodec && !codecAvailable)
      LogManager.instance().log(this, Level.WARNING,
          "gRPC compression codec '%s' is not registered: responses for method '%s' will not be compressed", compressionType,
          methodName);

    // Store compression info in context
    final CompressionInfo compressionInfo = new CompressionInfo(requestCompressed, requestEncoding);
    final Context context = Context.current().withValue(COMPRESSION_KEY, compressionInfo);

    // Wrap the call to select the response compressor before the headers are flushed
    final ServerCall<ReqT, RespT> compressedCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
      @Override
      public void sendHeaders(final Metadata responseHeaders) {

        if (compressResponse) {
          // The compressor must be selected before the response headers are sent out
          super.setCompression(compressionType);
          setMessageCompression(true);

          LogManager.instance()
              .log(GrpcCompressionInterceptor.this, Level.FINE, "Forced %s compression for method: %s", compressionType,
                  methodName);
        }

        super.sendHeaders(responseHeaders);
      }
    };

    return Contexts.interceptCall(context, compressedCall, headers, next);
  }

  private static boolean clientAcceptsEncoding(final Metadata headers, final String encoding) {
    final String acceptEncoding = headers.get(GRPC_ACCEPT_ENCODING);
    if (acceptEncoding == null)
      return false;

    for (final String token : acceptEncoding.split(","))
      if (token.trim().equalsIgnoreCase(encoding))
        return true;

    return false;
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
