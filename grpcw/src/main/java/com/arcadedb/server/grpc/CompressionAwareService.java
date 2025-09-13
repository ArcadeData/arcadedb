package com.arcadedb.server.grpc;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import com.arcadedb.log.LogManager;
import java.util.logging.Level;

/**
 * Enhanced compression-aware service wrapper
 */
class CompressionAwareService {

  /**
   * Force compression for a specific response
   */
  public static <T> void setResponseCompression(StreamObserver<T> responseObserver, String compression) {
    if (responseObserver instanceof ServerCallStreamObserver) {
      ServerCallStreamObserver<T> serverObserver = (ServerCallStreamObserver<T>) responseObserver;
      serverObserver.setCompression(compression);
      LogManager.instance().log(CompressionAwareService.class, Level.FINE, "Set response compression to: %s", compression);
    }
  }

  /**
   * Check if current request was compressed (call from service method)
   */
  public static boolean isCurrentRequestCompressed() {
    GrpcCompressionInterceptor.CompressionInfo info =
        GrpcCompressionInterceptor.COMPRESSION_KEY.get();
    return info != null && info.requestCompressed;
  }

  /**
   * Get current request compression encoding
   */
  public static String getCurrentRequestEncoding() {
    GrpcCompressionInterceptor.CompressionInfo info =
        GrpcCompressionInterceptor.COMPRESSION_KEY.get();
    return info != null ? info.requestEncoding : "identity";
  }

  /**
   * Get compression statistics for monitoring
   */
  public static class CompressionStats {
    private long compressedRequests    = 0;
    private long uncompressedRequests  = 0;
    private long compressedResponses   = 0;
    private long uncompressedResponses = 0;

    public synchronized void recordRequest(boolean compressed) {
      if (compressed) {
        compressedRequests++;
      } else {
        uncompressedRequests++;
      }
    }

    public synchronized void recordResponse(boolean compressed) {
      if (compressed) {
        compressedResponses++;
      } else {
        uncompressedResponses++;
      }
    }

    public synchronized String getStats() {
      long totalRequests = compressedRequests + uncompressedRequests;
      long totalResponses = compressedResponses + uncompressedResponses;

      double reqCompressionRate = totalRequests > 0 ?
          (compressedRequests * 100.0 / totalRequests) : 0;
      double respCompressionRate = totalResponses > 0 ?
          (compressedResponses * 100.0 / totalResponses) : 0;

      return String.format(
          "Compression Stats - Requests: %.1f%% (%d/%d), Responses: %.1f%% (%d/%d)",
          reqCompressionRate, compressedRequests, totalRequests,
          respCompressionRate, compressedResponses, totalResponses
      );
    }
  }
}
