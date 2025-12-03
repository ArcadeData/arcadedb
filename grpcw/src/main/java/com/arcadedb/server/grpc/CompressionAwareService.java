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
