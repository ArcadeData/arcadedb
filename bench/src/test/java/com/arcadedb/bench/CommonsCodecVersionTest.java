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
package com.arcadedb.bench;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12503: the Meterian scan went red (security 53, minimum 90) on commons-codec 1.19.0, which
 * testcontainers reaches through commons-compress and which is affected by CVE-2026-119078 (Blake3 documentation,
 * affects 1.16.0 up to 1.23.0 excluded). The root pom manages commons-codec to a fixed version; this test pins that
 * management on the classpath of the module the scanner names. When commons-compress or testcontainers ship a fixed
 * commons-codec and the pin in the root pom is dropped, drop this test with it.
 * <p>
 * The version is read from the manifest of the jar that provides the codec classes, never from the
 * {@code META-INF/maven/commons-codec} descriptor: Surefire's own shaded utilities jar carries a copy of that descriptor
 * (of whatever version Surefire was built with) ahead of the project dependencies on the test classpath.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CommonsCodecVersionTest {
  private static final String FIXED_VERSION = "1.23.0";
  private static final String CODEC_CLASS   = "org.apache.commons.codec.binary.Base64";

  @Test
  void commonsCodecOnTheClasspathIsNotAffectedByCve2026119078() throws ClassNotFoundException {
    final Class<?> codec = Class.forName(CODEC_CLASS);
    final String resolved = codec.getPackage().getImplementationVersion();
    final Object location = codec.getProtectionDomain().getCodeSource().getLocation();

    assertThat(resolved).as("the jar %s declares no Implementation-Version", location).isNotNull();
    assertThat(compareVersions(resolved, FIXED_VERSION)).as(
        "commons-codec %s (%s) is on the classpath, CVE-2026-119078 is fixed in %s: manage it in the root pom dependencyManagement", resolved,
        location, FIXED_VERSION).isGreaterThanOrEqualTo(0);
  }

  @Test
  void versionComparisonOrdersNumericComponents() {
    assertThat(compareVersions("1.19.0", "1.23.0")).isNegative();
    assertThat(compareVersions("1.9.0", "1.23.0")).isNegative();
    assertThat(compareVersions("1.11", "1.23.0")).isNegative();
    assertThat(compareVersions("1.23.0", "1.23.0")).isZero();
    assertThat(compareVersions("1.23", "1.23.0")).isZero();
    assertThat(compareVersions("1.23.1", "1.23.0")).isPositive();
    assertThat(compareVersions("2.0.0", "1.23.0")).isPositive();
  }

  private static int compareVersions(final String left, final String right) {
    final String[] a = left.split("\\.");
    final String[] b = right.split("\\.");
    for (int i = 0; i < Math.max(a.length, b.length); i++) {
      final int ai = i < a.length ? Integer.parseInt(a[i]) : 0;
      final int bi = i < b.length ? Integer.parseInt(b[i]) : 0;
      if (ai != bi)
        return Integer.compare(ai, bi);
    }
    return 0;
  }
}
