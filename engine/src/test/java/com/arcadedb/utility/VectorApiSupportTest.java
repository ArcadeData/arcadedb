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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10072: a native image runs the Vector API through its pure-Java lane loops, so jvector must fall back to its
 * scalar provider there unless the user chose one.
 */
class VectorApiSupportTest {

  @Test
  void nativeImageWithoutAChoiceGetsTheScalarJVectorProvider() {
    assertThat(VectorApiSupport.jvectorProviderFor(true, null)).isEqualTo("default");
    assertThat(VectorApiSupport.jvectorProviderFor(true, "  ")).isEqualTo("default");
  }

  @Test
  void anExplicitJVectorProviderWinsInANativeImage() {
    assertThat(VectorApiSupport.jvectorProviderFor(true, "panama")).isNull();
    assertThat(VectorApiSupport.jvectorProviderFor(true, "native")).isNull();
  }

  @Test
  void theJvmKeepsJVectorsOwnChoice() {
    assertThat(VectorApiSupport.jvectorProviderFor(false, null)).isNull();
    assertThat(VectorApiSupport.jvectorProviderFor(false, "default")).isNull();
  }

  @Test
  void theTestJvmIsNotANativeImageAndCompilesTheVectorApi() {
    assertThat(VectorApiSupport.isNativeImage()).isFalse();
    assertThat(VectorApiSupport.isVectorApiCompiled()).isTrue();
  }

  @Test
  void configuringOnAJvmLeavesTheJVectorPropertyAlone() {
    final String before = System.getProperty(VectorApiSupport.JVECTOR_PROVIDER_PROPERTY);
    try {
      System.clearProperty(VectorApiSupport.JVECTOR_PROVIDER_PROPERTY);
      VectorApiSupport.configureJVectorProvider();
      assertThat(System.getProperty(VectorApiSupport.JVECTOR_PROVIDER_PROPERTY)).isNull();

      System.setProperty(VectorApiSupport.JVECTOR_PROVIDER_PROPERTY, "panama");
      VectorApiSupport.configureJVectorProvider();
      assertThat(System.getProperty(VectorApiSupport.JVECTOR_PROVIDER_PROPERTY)).isEqualTo("panama");
    } finally {
      if (before == null)
        System.clearProperty(VectorApiSupport.JVECTOR_PROVIDER_PROPERTY);
      else
        System.setProperty(VectorApiSupport.JVECTOR_PROVIDER_PROPERTY, before);
    }
  }

  @Test
  void everyCallSiteGetsJVectorsOwnProvider() {
    assertThat(VectorApiSupport.jvectorProvider()).isSameAs(VectorizationProvider.getInstance());
  }
}
