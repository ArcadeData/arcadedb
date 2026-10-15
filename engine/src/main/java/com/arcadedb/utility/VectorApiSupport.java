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
package com.arcadedb.utility;

import io.github.jbellis.jvector.vector.VectorizationProvider;

/**
 * Tells whether the Java Vector API ({@code jdk.incubator.vector}) runs as SIMD instructions in this process.
 * <p>
 * On HotSpot, C2 compiles Vector API calls into SIMD instructions. A GraalVM native image does not: every call runs the
 * incubator's pure-Java fallback, which loops over the lanes of arrays it allocates per operation and is several times
 * slower than plain scalar code. Measured on a native image, the jvector graph build of 20k 384-dimension vectors took
 * 178 s with the Vector API and 6.5 s with jvector's scalar provider (issue #10072). Every SIMD code path therefore picks
 * its scalar twin when {@link #isVectorApiCompiled()} is false.
 */
public final class VectorApiSupport {
  /**
   * The system property jvector reads, once, to choose its vectorization provider.
   */
  public static final String JVECTOR_PROVIDER_PROPERTY = "jvector.vectorization_provider";
  /**
   * Set by GraalVM to "buildtime" or "runtime" while native-image code runs, absent on a JVM.
   */
  static final        String NATIVE_IMAGE_PROPERTY     = "org.graalvm.nativeimage.imagecode";
  static final        String JVECTOR_SCALAR_PROVIDER   = "default";

  private VectorApiSupport() {
  }

  public static boolean isNativeImage() {
    return System.getProperty(NATIVE_IMAGE_PROPERTY) != null;
  }

  /**
   * Returns true when Vector API calls are compiled to SIMD instructions, false in a native image. A JVM running without
   * C2 ({@code -Xint}, {@code -XX:TieredStopAtLevel=1}) has the same problem, but jvector already detects that case on its
   * own, and ArcadeDB's SIMD ops are not worth a second probe for a configuration nobody runs in production.
   */
  public static boolean isVectorApiCompiled() {
    return !isNativeImage();
  }

  /**
   * Returns jvector's vectorization provider, after {@link #configureJVectorProvider()} had its chance to pick the scalar
   * one. Every ArcadeDB call site reaches jvector's provider through here, so none of them can resolve it first.
   */
  public static VectorizationProvider jvectorProvider() {
    return JVectorProviderHolder.INSTANCE;
  }

  /**
   * Points jvector at its scalar provider when the Vector API is not compiled and the user has not chosen a provider.
   * jvector resolves its provider once per process, on first use, so this must run before any jvector class is touched:
   * {@link #jvectorProvider()} calls it, and so does {@code LocalDatabase}'s class initialization, which covers jvector
   * classes reached from inside jvector itself. Calling it again is harmless.
   */
  public static void configureJVectorProvider() {
    final String provider = jvectorProviderFor(isNativeImage(), System.getProperty(JVECTOR_PROVIDER_PROPERTY));
    if (provider != null)
      System.setProperty(JVECTOR_PROVIDER_PROPERTY, provider);
  }

  /**
   * Returns the provider to force on jvector, or null to leave its own choice alone.
   *
   * @param nativeImage true when running as a native image
   * @param configured  the provider the user set through {@link #JVECTOR_PROVIDER_PROPERTY}, or null
   */
  static String jvectorProviderFor(final boolean nativeImage, final String configured) {
    if (!nativeImage || (configured != null && !configured.isBlank()))
      return null;
    return JVECTOR_SCALAR_PROVIDER;
  }

  private static final class JVectorProviderHolder {
    private static final VectorizationProvider INSTANCE;

    static {
      configureJVectorProvider();
      INSTANCE = VectorizationProvider.getInstance();
    }
  }
}
