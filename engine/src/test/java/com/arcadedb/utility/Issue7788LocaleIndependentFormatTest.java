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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7788: {@code StringUtils.format()} (backing the SQL {@code .format()} method) called
 * {@code String.format(format, args)} with no {@link Locale}, so the decimal separator of every {@code %f}/{@code %e}
 * conversion came from the server JVM's default locale instead of being locale-independent like the sibling
 * {@code number.format()} function. A comma-decimal default locale turned {@code %e} output such as
 * {@code "1.234568e+06"} into {@code "1,234568e+06"}, which is no longer parseable as a number downstream.
 * <p>
 * Mutates the JVM default locale for the duration of the test and restores it afterward - tests in this module run
 * sequentially (no {@code junit.jupiter.execution.parallel.enabled}), so this is safe, but it is still process-wide
 * state and must never leak into another test.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7788LocaleIndependentFormatTest {

  private Locale originalDefault;

  @BeforeEach
  void saveDefaultLocale() {
    originalDefault = Locale.getDefault();
  }

  @AfterEach
  void restoreDefaultLocale() {
    Locale.setDefault(originalDefault);
  }

  @Test
  void stringUtilsFormatIgnoresTheJvmDefaultLocale() {
    Locale.setDefault(Locale.GERMANY);
    try {
      assertThat(StringUtils.format("format", "%,.2f", 1234567.891)).isEqualTo("1,234,567.89");
      assertThat(StringUtils.format("format", "%e", 1234567.891)).isEqualTo("1.234568e+06");
    } finally {
      Locale.setDefault(originalDefault);
    }
  }

  @Test
  void stringUtilsFormatIsConsistentAcrossLocales() {
    final String rootResult;
    Locale.setDefault(Locale.US);
    try {
      rootResult = StringUtils.format("format", "%,.2f", 1234567.891);
    } finally {
      Locale.setDefault(originalDefault);
    }

    Locale.setDefault(Locale.GERMANY);
    final String germanResult;
    try {
      germanResult = StringUtils.format("format", "%,.2f", 1234567.891);
    } finally {
      Locale.setDefault(originalDefault);
    }

    assertThat(germanResult).isEqualTo(rootResult);
  }
}
