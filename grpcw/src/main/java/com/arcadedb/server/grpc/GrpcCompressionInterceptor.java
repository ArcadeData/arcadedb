package com.arcadedb.server.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compression interceptor that can force compression based on configuration
 */
class GrpcCompressionInterceptor implements ServerInterceptor {
  private static final Logger logger = LoggerFactory.getLogger(GrpcCompressionInterceptor.class);

  // Context key to store compression info
  public static final Context.Key<CompressionInfo> COMPRESSION_KEY = Context.key("compression-info");

  private static final Metadata.Key<String> GRPC_ACCEPT_ENCODING = Metadata.Key.of("grpc-accept-encoding",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> GRPC_ENCODING        = Metadata.Key.of("grpc-encoding",
      Metadata.ASCII_STRING_MARSHALLER);

  private final boolean forceCompression;
  private final String  compressionType;

  private final int minMessageSizeForCompression;

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

          logger.debug("Forced {} compression for method: {}", compressionType, methodName);

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

            logger.debug("Forced {} compression for method: {}", compressionType, methodName);
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
