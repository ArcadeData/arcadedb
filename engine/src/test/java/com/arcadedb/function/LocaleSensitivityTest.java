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
package com.arcadedb.function;

import com.arcadedb.function.convert.ConvertToBoolean;
import com.arcadedb.function.convert.ToBooleanFunction;
import com.arcadedb.function.date.AbstractDateFunction;
import com.arcadedb.function.date.DateField;
import com.arcadedb.function.math.RoundFunction;
import com.arcadedb.function.text.NormalizeFunction;
import com.arcadedb.function.text.ToLowerFunction;
import com.arcadedb.function.text.ToUpperFunction;
import com.arcadedb.function.util.UtilCompress;
import com.arcadedb.function.vector.VectorDistanceFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for locale-sensitive toLowerCase/toUpperCase.
 * On Turkish locale (tr_TR), "I".toLowerCase() produces dotless-i (u0131)
 * and "i".toUpperCase() produces dotted-I (u0130), breaking keyword comparisons
 * in any function that uses the JVM default locale.
 * All internal keyword parsing must use Locale.ROOT.
 */
class LocaleSensitivityTest {

  private Locale originalLocale;

  @BeforeEach
  void setTurkishLocale() {
    originalLocale = Locale.getDefault();
    Locale.setDefault(Locale.forLanguageTag("tr-TR"));
  }

  @AfterEach
  void restoreLocale() {
    Locale.setDefault(originalLocale);
  }

  // ========= ToLowerFunction =========

  @Test
  void toLowerFunctionInfoUnderTurkishLocale() {
    // "INFO".toLowerCase(tr_TR) -> "ınfo" (dotless-i); Locale.ROOT gives "info"
    final ToLowerFunction fn = new ToLowerFunction();
    assertThat(fn.execute(new Object[]{"INFO"}, null)).isEqualTo("info");
  }

  @Test
  void toLowerFunctionCapitalIUnderTurkishLocale() {
    // "I".toLowerCase(tr_TR) -> "ı" (ı); Locale.ROOT gives "i"
    final ToLowerFunction fn = new ToLowerFunction();
    assertThat(fn.execute(new Object[]{"I"}, null)).isEqualTo("i");
  }

  // ========= ToUpperFunction =========

  @Test
  void toUpperFunctionInfoUnderTurkishLocale() {
    // "info".toUpperCase(tr_TR) -> "İNFO" (dotted-I); Locale.ROOT gives "INFO"
    final ToUpperFunction fn = new ToUpperFunction();
    assertThat(fn.execute(new Object[]{"info"}, null)).isEqualTo("INFO");
  }

  @Test
  void toUpperFunctionLowercaseIUnderTurkishLocale() {
    // "i".toUpperCase(tr_TR) -> "İ" (İ); Locale.ROOT gives "I"
    final ToUpperFunction fn = new ToUpperFunction();
    assertThat(fn.execute(new Object[]{"i"}, null)).isEqualTo("I");
  }

  // ========= ToBooleanFunction =========

  @Test
  void toBooleanFunctionUpperCaseUnderTurkishLocale() {
    final ToBooleanFunction fn = new ToBooleanFunction();
    assertThat(fn.execute(new Object[]{"TRUE"}, null)).isEqualTo(Boolean.TRUE);
    assertThat(fn.execute(new Object[]{"FALSE"}, null)).isEqualTo(Boolean.FALSE);
  }

  // ========= ConvertToBoolean =========

  @Test
  void convertToBooleanUpperCaseUnderTurkishLocale() {
    final ConvertToBoolean fn = new ConvertToBoolean();
    assertThat(fn.execute(new Object[]{"TRUE"}, null)).isEqualTo(Boolean.TRUE);
    assertThat(fn.execute(new Object[]{"YES"}, null)).isEqualTo(Boolean.TRUE);
    assertThat(fn.execute(new Object[]{"FALSE"}, null)).isEqualTo(Boolean.FALSE);
    assertThat(fn.execute(new Object[]{"NO"}, null)).isEqualTo(Boolean.FALSE);
  }

  // ========= UtilCompress =========

  @Test
  void utilCompressUpperCaseGzipUnderTurkishLocale() {
    // "GZIP".toLowerCase(tr_TR) -> "gzıp" (dotless-i at position 2)
    // then switch("gzıp") misses the "gzip" case and falls to default -> throws
    final UtilCompress fn = new UtilCompress();
    final Object result = fn.execute(new Object[]{"hello", "GZIP"}, null);
    assertThat(result).isNotNull().isInstanceOf(String.class);
  }

  @Test
  void utilCompressUpperCaseDeflateUnderTurkishLocale() {
    final UtilCompress fn = new UtilCompress();
    final Object result = fn.execute(new Object[]{"hello", "DEFLATE"}, null);
    assertThat(result).isNotNull().isInstanceOf(String.class);
  }

  // ========= RoundFunction =========

  @Test
  void roundFunctionUpperCaseModeUnderTurkishLocale() {
    final RoundFunction fn = new RoundFunction();
    assertThat(fn.execute(new Object[]{2.5, 0, "half_up"}, null)).isEqualTo(3.0);
    assertThat(fn.execute(new Object[]{2.5, 0, "HALF_UP"}, null)).isEqualTo(3.0);
    assertThat(fn.execute(new Object[]{2.4, 0, "half_down"}, null)).isEqualTo(2.0);
  }

  // ========= AbstractDateFunction.unitToMillis =========

  @Test
  void unitToMillisUpperCaseUnderTurkishLocale() {
    // "MILLISECONDS" contains 'I' -> toLowerCase(tr_TR) gives "mıllıseconds" != "milliseconds"
    // "MINUTES" contains 'I' -> gives "mınutes" != "minutes"
    final AbstractDateFunction fn = new AbstractDateFunction() {
      @Override
      protected String getSimpleName() {
        return "test";
      }

      @Override
      public int getMinArgs() {
        return 1;
      }

      @Override
      public int getMaxArgs() {
        return 1;
      }

      @Override
      public Object execute(final Object[] args, final CommandContext ctx) {
        return unitToMillis(args[0] != null ? args[0].toString() : null);
      }
    };

    assertThat(fn.execute(new Object[]{"MILLISECONDS"}, null)).isEqualTo(1L);
    assertThat(fn.execute(new Object[]{"MINUTES"}, null)).isEqualTo(60_000L);
    assertThat(fn.execute(new Object[]{"ms"}, null)).isEqualTo(1L);
    assertThat(fn.execute(new Object[]{"SECONDS"}, null)).isEqualTo(1_000L);
    assertThat(fn.execute(new Object[]{"HOURS"}, null)).isEqualTo(3_600_000L);
    assertThat(fn.execute(new Object[]{"DAYS"}, null)).isEqualTo(86_400_000L);
  }

  // ========= DateField =========

  @Test
  void dateFieldUpperCaseFieldUnderTurkishLocale() {
    // "MINUTE".toLowerCase(tr_TR) -> "mınute" (dotless-i) != "minute"
    // "MILLISECOND".toLowerCase(tr_TR) -> "mıllısecond" != "millisecond"
    final DateField fn = new DateField();
    final long timestampMs = 1_700_000_000_000L;
    assertThat(fn.execute(new Object[]{timestampMs, "MINUTE"}, null)).isNotNull();
    assertThat(fn.execute(new Object[]{timestampMs, "MILLISECOND"}, null)).isNotNull();
    assertThat(fn.execute(new Object[]{timestampMs, "MILLISECONDS"}, null)).isNotNull();
    assertThat(fn.execute(new Object[]{timestampMs, "MILLIS"}, null)).isNotNull();
  }

  // ========= NormalizeFunction =========

  @Test
  void normalizeFunctionLowerCaseFormUnderTurkishLocale() {
    // The standard forms (NFC, NFD, NFKC, NFKD) contain no 'i', so Turkish locale does not break them.
    // Still verify that lowercase form names work correctly via Locale.ROOT toUpperCase.
    final NormalizeFunction fn = new NormalizeFunction();
    final String composed = "é"; // é NFC
    assertThat(fn.execute(new Object[]{composed, "nfc"}, null)).isEqualTo(composed);
    assertThat(fn.execute(new Object[]{composed, "nfd"}, null)).isNotNull();
    assertThat(fn.execute(new Object[]{composed, "nfkc"}, null)).isEqualTo(composed);
    assertThat(fn.execute(new Object[]{composed, "nfkd"}, null)).isNotNull();
  }

  // ========= VectorDistanceFunction =========

  @Test
  void vectorDistanceFunctionLowerCaseMetricUnderTurkishLocale() {
    // "euclidean".toUpperCase(tr_TR) -> "EUCLİDEAN" (dotted-İ) != "EUCLIDEAN"
    final VectorDistanceFunction fn = new VectorDistanceFunction();
    final float[] a = {3.0f, 0.0f};
    final float[] b = {0.0f, 4.0f};
    assertThat(fn.execute(new Object[]{a, b, "euclidean"}, null)).isEqualTo(5.0);
    assertThat(fn.execute(new Object[]{a, b, "EUCLIDEAN"}, null)).isEqualTo(5.0);
    assertThat(fn.execute(new Object[]{a, b, "cosine"}, null)).isNotNull();
    assertThat(fn.execute(new Object[]{a, b, "manhattan"}, null)).isNotNull();
  }
}
