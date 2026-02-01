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

import com.google.protobuf.Timestamp;

/**
 * Utility class for converting between gRPC protobuf types and Java/ArcadeDB types.
 * This class is stateless and all methods are static.
 */
class GrpcTypeConverter {

  private GrpcTypeConverter() {
    // Utility class - prevent instantiation
  }

  /**
   * Convert a protobuf Timestamp to milliseconds since epoch.
   */
  static long tsToMillis(final Timestamp ts) {
    return ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
  }

  /**
   * Convert milliseconds since epoch to a protobuf Timestamp.
   */
  static Timestamp msToTimestamp(final long ms) {
    final long seconds = Math.floorDiv(ms, 1000L);
    final int nanos = (int) Math.floorMod(ms, 1000L) * 1_000_000;
    return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
  }
}
