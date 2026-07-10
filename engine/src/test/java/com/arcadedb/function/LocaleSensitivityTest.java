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
import com.arcadedb.function.node.AbstractNodeFunction;
import com.arcadedb.function.sql.geo.SQLFunctionGeoDistance;
import com.arcadedb.function.sql.time.SQLFunctionTimeBucket;
import com.arcadedb.function.sql.vector.SQLFunctionVectorMultiScore;
import com.arcadedb.function.sql.vector.SQLFunctionVectorScoreTransform;
import com.arcadedb.function.text.NormalizeFunction;
import com.arcadedb.function.text.ToLowerFunction;
import com.arcadedb.function.text.ToUpperFunction;
import com.arcadedb.function.util.UtilCompress;
import com.arcadedb.function.util.UtilDecompress;
import com.arcadedb.function.vector.VectorDistanceFunction;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for locale-sensitive toLowerCase/toUpperCase.
 * On Turkish locale (tr_TR), "I".toLowerCase() produces dotless-i (u0131)
 * and "i".toUpperCase() produces dotted-I (u0130), breaking keyword comparisons
 * in any function that uses the JVM default locale.
 * All internal keyword parsing must use Locale.ROOT.
 */
@Isolated
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
    // "I".toLowerCase(tr_TR) -> "ı"; Locale.ROOT gives "i"
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
    // "i".toUpperCase(tr_TR) -> "İ"; Locale.ROOT gives "I"
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
    // "GZIP".toLowerCase(tr_TR) -> "gzıp" (dotless-i at I); switch misses "gzip"
    final UtilCompress fn = new UtilCompress();
    final Object resultUpper = fn.execute(new Object[]{"hello", "GZIP"}, null);
    final Object resultLower = fn.execute(new Object[]{"hello", "gzip"}, null);
    assertThat(resultUpper).isEqualTo(resultLower);
  }

  @Test
  void utilCompressUpperCaseDeflateUnderTurkishLocale() {
    final UtilCompress fn = new UtilCompress();
    final Object resultUpper = fn.execute(new Object[]{"hello", "DEFLATE"}, null);
    final Object resultLower = fn.execute(new Object[]{"hello", "deflate"}, null);
    assertThat(resultUpper).isEqualTo(resultLower);
  }

  // ========= UtilDecompress =========

  @Test
  void utilDecompressUpperCaseGzipUnderTurkishLocale() {
    // Same bug as UtilCompress - "GZIP".toLowerCase(tr_TR) -> "gzıp"
    final UtilCompress compress = new UtilCompress();
    final UtilDecompress decompress = new UtilDecompress();
    final String compressed = (String) compress.execute(new Object[]{"hello world", "gzip"}, null);
    final Object resultUpper = decompress.execute(new Object[]{compressed, "GZIP"}, null);
    final Object resultLower = decompress.execute(new Object[]{compressed, "gzip"}, null);
    assertThat(resultUpper).isEqualTo(resultLower).isEqualTo("hello world");
  }

  // ========= RoundFunction =========

  @Test
  void roundFunctionCeilingModeUnderTurkishLocale() {
    // "ceiling".toUpperCase(tr_TR) -> "CEİLİNG" (i->İ); Locale.ROOT gives "CEILING"
    final RoundFunction fn = new RoundFunction();
    assertThat(fn.execute(new Object[]{2.1, 0, "ceiling"}, null)).isEqualTo(3.0);
    assertThat(fn.execute(new Object[]{2.1, 0, "CEILING"}, null)).isEqualTo(3.0);
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
    // Use a timestamp with a specific millisecond value to enable exact assertions
    final long timestampMs = 1_700_000_000_123L; // ...000.123 ms
    assertThat(fn.execute(new Object[]{timestampMs, "MINUTE"}, null)).isNotNull();
    assertThat(fn.execute(new Object[]{timestampMs, "MILLISECOND"}, null)).isEqualTo(123L);
    assertThat(fn.execute(new Object[]{timestampMs, "MILLISECONDS"}, null)).isEqualTo(123L);
    assertThat(fn.execute(new Object[]{timestampMs, "MILLIS"}, null)).isEqualTo(123L);
  }

  // ========= AbstractNodeFunction.parseDirection =========

  @Test
  void parsedDirectionInUnderTurkishLocale() {
    // "IN".toLowerCase(tr_TR) -> "ın" (dotless-i) != "in"; breaks graph traversal direction matching
    final AbstractNodeFunction fn = new AbstractNodeFunction() {
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
        return parseDirection(args[0] != null ? args[0].toString() : null);
      }
    };

    assertThat(fn.execute(new Object[]{"IN"}, null)).isEqualTo(Vertex.DIRECTION.IN);
    assertThat(fn.execute(new Object[]{"in"}, null)).isEqualTo(Vertex.DIRECTION.IN);
    assertThat(fn.execute(new Object[]{"INCOMING"}, null)).isEqualTo(Vertex.DIRECTION.IN);
    assertThat(fn.execute(new Object[]{"OUT"}, null)).isEqualTo(Vertex.DIRECTION.OUT);
    assertThat(fn.execute(new Object[]{"BOTH"}, null)).isEqualTo(Vertex.DIRECTION.BOTH);
  }

  // ========= NormalizeFunction =========

  @Test
  void normalizeFunctionWorksUnderTurkishLocale() {
    // Standard form names (NFC, NFD, NFKC, NFKD) contain no 'i'; verify Locale.ROOT toUpperCase consistency
    final NormalizeFunction fn = new NormalizeFunction();
    final String composed = "é"; // é NFC
    assertThat(fn.execute(new Object[]{composed, "nfc"}, null)).isEqualTo(composed);
    assertThat(fn.execute(new Object[]{composed, "nfd"}, null)).isNotNull();
    assertThat(fn.execute(new Object[]{composed, "nfkc"}, null)).isEqualTo(composed);
    assertThat(fn.execute(new Object[]{composed, "nfkd"}, null)).isNotNull();
  }

  // ========= FunctionRegistry name normalization =========

  @Test
  void functionRegistryNormalizeApocNameUnderTurkishLocale() {
    // "NODEINFO".toLowerCase(tr_TR) -> "nodeınfo" (I->ı) != "nodeinfo"
    // normalizeApocName must use Locale.ROOT for case folding
    assertThat(FunctionRegistry.normalizeApocName("NODEINFO")).isEqualTo("nodeinfo");
    assertThat(FunctionRegistry.normalizeApocName("TOINTEGER")).isEqualTo("tointeger");
    assertThat(FunctionRegistry.normalizeApocName("TYPEIN")).isEqualTo("typein");
  }

  // ========= SQLFunctionGeoDistance =========

  @Test
  void geoDistanceUpperCaseMiUnderTurkishLocale() {
    // "MI".toLowerCase(tr_TR) -> "mı" (dotless-i) != "mi"; same for "NMI" -> "nmı"
    final SQLFunctionGeoDistance fn = new SQLFunctionGeoDistance();
    // Two identical WKT points → distance = 0 regardless of unit
    final String p = "POINT(0 0)";
    final Object resultMI = fn.execute(null, null, null, new Object[]{p, p, "MI"}, null);
    final Object resultMi = fn.execute(null, null, null, new Object[]{p, p, "mi"}, null);
    assertThat(resultMI).isEqualTo(resultMi);
    final Object resultNMI = fn.execute(null, null, null, new Object[]{p, p, "NMI"}, null);
    final Object resultNmi = fn.execute(null, null, null, new Object[]{p, p, "nmi"}, null);
    assertThat(resultNMI).isEqualTo(resultNmi);
  }

  // ========= SQLFunctionTimeBucket =========

  @Test
  void timeBucketParseIntervalUpperCaseUnderTurkishLocale() {
    // Single-letter units ('m','s','h','d','w') are not affected by tr_TR,
    // but uppercase variants must work correctly for any locale
    assertThat(SQLFunctionTimeBucket.parseInterval("5M")).isEqualTo(5 * 60_000L);
    assertThat(SQLFunctionTimeBucket.parseInterval("10S")).isEqualTo(10 * 1000L);
    assertThat(SQLFunctionTimeBucket.parseInterval("2H")).isEqualTo(2 * 3_600_000L);
  }

  // ========= SQLFunctionVectorScoreTransform =========

  @Test
  void vectorScoreTransformSigmoidUnderTurkishLocale() {
    // "sigmoid".toUpperCase(tr_TR) -> "SİGMOİD" (two i's become İ) != "SIGMOID"
    final SQLFunctionVectorScoreTransform fn = new SQLFunctionVectorScoreTransform();
    final Object resultLower = fn.execute(null, null, null, new Object[]{0.5f, "sigmoid"}, null);
    final Object resultUpper = fn.execute(null, null, null, new Object[]{0.5f, "SIGMOID"}, null);
    assertThat(resultLower).isEqualTo(resultUpper);
    assertThat(resultLower).isNotNull();
  }

  // ========= SQLFunctionVectorMultiScore =========

  @Test
  void multiVectorScoreMinMethodUnderTurkishLocale() {
    // "min".toUpperCase(tr_TR) -> "MİN" (i -> İ) != "MIN"
    final SQLFunctionVectorMultiScore fn = new SQLFunctionVectorMultiScore();
    final List<Double> scores = List.of(0.9, 0.7, 0.8);
    final Object resultLower = fn.execute(null, null, null, new Object[]{scores, "min"}, null);
    final Object resultUpper = fn.execute(null, null, null, new Object[]{scores, "MIN"}, null);
    assertThat(resultLower).isEqualTo(resultUpper);
  }

  // ========= VectorDistanceFunction =========

  @Test
  void vectorDistanceFunctionLowerCaseMetricUnderTurkishLocale() {
    // "euclidean".toUpperCase(tr_TR) -> "EUCLİDEAN" (i->İ) != "EUCLIDEAN"
    final VectorDistanceFunction fn = new VectorDistanceFunction();
    final float[] a = {3.0f, 0.0f};
    final float[] b = {0.0f, 4.0f};
    assertThat(fn.execute(new Object[]{a, b, "euclidean"}, null)).isEqualTo(5.0);
    assertThat(fn.execute(new Object[]{a, b, "EUCLIDEAN"}, null)).isEqualTo(5.0);
    assertThat(fn.execute(new Object[]{a, b, "cosine"}, null)).isEqualTo(fn.execute(new Object[]{a, b, "COSINE"}, null));
    assertThat(fn.execute(new Object[]{a, b, "manhattan"}, null)).isEqualTo(fn.execute(new Object[]{a, b, "MANHATTAN"}, null));
  }
}
