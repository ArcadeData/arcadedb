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

import io.grpc.Metadata;

/**
 * The packed vector kinds of a {@link GrpcValue}, {@code float_array_value} and {@code double_array_value} (issue #11714),
 * shared by the server's decoders and encoders and the client's {@code ProtoUtils}, so both ends of the wire read and write
 * them the same way. Each is one length-delimited run of 4 or 8 bytes per element, where a {@code list_value} spends a nested
 * {@link GrpcValue} per element: a 768-float vector is 3,075 bytes packed against about 5,400 as a list, and decodes into one
 * primitive array with no per-element object on either end.
 * <p>
 * Both directions are negotiated with {@link #HEADER} ({@code true}), because a peer built from a copy of the proto that
 * predates them reads an unknown oneof member as a value with no kind set, which is {@code null}:
 * <ul>
 * <li>every response of a server that decodes them carries the header, in its headers and in its trailers, and a client sends
 * them only after a response of the server said so, lists otherwise;</li>
 * <li>a response carries them only to a call that sent the header; without it a {@code float[]} or {@code double[]} result
 * stays a {@code list_value}.</li>
 * </ul>
 */
public final class GrpcPackedArrays {

  /**
   * On a request, the caller's consent to {@code float[]} and {@code double[]} results in their packed kinds; on a response, the
   * server's announcement that it decodes them.
   */
  public static final String              HEADER_NAME = "x-arcade-packed-arrays";
  public static final Metadata.Key<String> HEADER      = Metadata.Key.of(HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);

  private GrpcPackedArrays() {
    // Utility class
  }

  public static GrpcValue encode(final float[] values) {
    final GrpcFloatArray.Builder packed = GrpcFloatArray.newBuilder();
    for (final float value : values)
      packed.addValues(value);
    return GrpcValue.newBuilder().setFloatArrayValue(packed).build();
  }

  public static GrpcValue encode(final double[] values) {
    final GrpcDoubleArray.Builder packed = GrpcDoubleArray.newBuilder();
    for (final double value : values)
      packed.addValues(value);
    return GrpcValue.newBuilder().setDoubleArrayValue(packed).build();
  }

  /** The elements read through the primitive accessor, so none is boxed. */
  public static float[] toFloats(final GrpcFloatArray packed) {
    final float[] values = new float[packed.getValuesCount()];
    for (int i = 0; i < values.length; i++)
      values[i] = packed.getValues(i);
    return values;
  }

  /** The elements read through the primitive accessor, so none is boxed. */
  public static double[] toDoubles(final GrpcDoubleArray packed) {
    final double[] values = new double[packed.getValuesCount()];
    for (int i = 0; i < values.length; i++)
      values[i] = packed.getValues(i);
    return values;
  }
}
