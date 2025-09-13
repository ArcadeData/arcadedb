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
      @Override
      public void close(Status status, Metadata trailers) {
        sample.stop(requestTimer);

        if (!status.isOk()) {
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
}
