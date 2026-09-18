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
package com.arcadedb.function.text;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7788: {@code text.format()} called {@code String.format(format, args)} with no
 * {@link Locale}, so its numeric output depended on the server JVM's default locale instead of being
 * locale-independent, unlike {@code number.format()}.
 * <p>
 * Mutates the JVM default locale for the duration of the test and restores it afterward - tests in this module run
 * sequentially, but this is still process-wide state.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7788TextFormatLocaleTest {

  private final TextFormat function = new TextFormat();

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
  void formatIgnoresTheJvmDefaultLocale() {
    Locale.setDefault(Locale.GERMANY);
    try {
      assertThat(function.execute(new Object[] { "%,.2f", 1234567.891 }, null)).isEqualTo("1,234,567.89");
    } finally {
      Locale.setDefault(originalDefault);
    }
  }

  @Test
  void formatIsConsistentAcrossLocales() {
    Locale.setDefault(Locale.US);
    final Object usResult;
    try {
      usResult = function.execute(new Object[] { "%,.2f", 1234567.891 }, null);
    } finally {
      Locale.setDefault(originalDefault);
    }

    Locale.setDefault(Locale.GERMANY);
    final Object germanResult;
    try {
      germanResult = function.execute(new Object[] { "%,.2f", 1234567.891 }, null);
    } finally {
      Locale.setDefault(originalDefault);
    }

    assertThat(germanResult).isEqualTo(usResult);
  }
}
