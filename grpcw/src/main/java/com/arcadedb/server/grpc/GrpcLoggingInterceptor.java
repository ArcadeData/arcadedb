package com.arcadedb.server.grpc;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logging interceptor for gRPC requests with compression detection
 */
class GrpcLoggingInterceptor implements ServerInterceptor {
  private static final Logger logger = LoggerFactory.getLogger(GrpcLoggingInterceptor.class);

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

    logger.debug("gRPC call started: {} (request compression: {}, client accepts: {})",
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
          logger.debug("gRPC call completed: {} ({}ms, req-compression: {}, resp-compression: {})",
              methodName, duration,
              requestCompressed ? requestEncoding : "none",
              responseCompression);
        } else {
          logger.warn("gRPC call failed: {} - {} ({}ms)", methodName, status, duration);
        }
        super.close(status, trailers);
      }
    };

    return next.startCall(wrappedCall, headers);
  }
}
