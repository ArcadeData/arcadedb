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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.text.SubstringFunction;
import com.arcadedb.query.sql.executor.ResultSet;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5629: an explicit {@code null} written in an optional argument position was read as
 * "argument omitted, use the default", so {@code normalize('x', null)} normalized as NFC while {@code normalize(null)}
 * answered {@code null}. The same absent value meant two different things depending on the position it landed in.
 *
 * <p>The settled rule is that omitting an argument selects the default, while writing {@code null} there propagates.
 * Neo4j documents that reading for every optional argument it defines one for - {@code round()} ("returns null if any of
 * its input parameters are null"), {@code replace()}'s limit and {@code btrim()}'s trim character - and nine of
 * ArcadeDB's own optional-argument functions already behaved that way.
 *
 * <p>Each function is covered twice on purpose: once for the explicit {@code null}, and once for the omitted argument.
 * The second half is what keeps the rule from being over-applied into "the default no longer works".
 */
class CypherOptionalArgumentNullIssue5629Test extends TestHelper {

  // ===================== the five functions named in the issue =====================

  @Test
  void normalizePropagatesAnExplicitNullNormalForm() {
    // 'Å' (Angstrom sign) composes to 'Å' under NFC, so a defaulted-to-NFC result is distinguishable from a
    // propagated null and from an untouched input.
    assertThat(single("RETURN normalize('Å', null) AS r")).isNull();
    // Omitting the argument still selects NFC.
    assertThat(single("RETURN normalize('Å') AS r")).isEqualTo("Å");
  }

  @Test
  void isNormalizedPropagatesAnExplicitNullNormalForm() {
    assertThat(single("RETURN isNormalized('Å', null) AS r")).isNull();
    assertThat(single("RETURN isNormalized('Å') AS r")).isEqualTo(false);
  }

  @Test
  void formatPropagatesAnExplicitNullPattern() {
    assertThat(single("RETURN format(date('1984-10-11'), null) AS r")).isNull();
    // Omitting the pattern still answers the ISO string.
    assertThat(single("RETURN format(date('1984-10-11')) AS r")).isEqualTo("1984-10-11");
  }

  @Test
  void roundPropagatesAnExplicitNullRoundingMode() {
    assertThat(single("RETURN round(3.14159, 2, null) AS r")).isNull();
    // Omitting the mode still selects HALF_UP.
    assertThat(single("RETURN round(3.14159, 2) AS r")).isEqualTo(3.14d);
  }

  @Test
  void vectorDistancePropagatesAnExplicitNullMetric() {
    // The issue names this one as vector_distance(), which in Cypher is a grammar rule whose metric is a keyword, so an
    // explicit null cannot be written there at all. VectorDistanceFunction - the class the issue points at - is reached
    // from Cypher under the name vector.distance(), where the metric is an ordinary expression and the null is
    // expressible.
    assertThat(single("RETURN vector.distance([1.0, 2.0, 3.0], [4.0, 5.0, 6.0], null) AS r")).isNull();
    // Omitting the metric still selects EUCLIDEAN.
    final Object euclidean = single("RETURN vector.distance([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]) AS r");
    assertThat(((Number) euclidean).doubleValue()).isCloseTo(5.196, Offset.offset(0.01));
  }

  // ===================== the same defect in the truncate family =====================
  //
  // Not named in the issue, but found by auditing every function whose getMinArgs() differs from getMaxArgs(). The
  // optional adjustment map was tested with `args[2] instanceof Map`, and null is not a Map, so an explicit null was
  // silently dropped and the plain truncated value came back. Fixing only the five named functions is the per-function
  // drift the issue asked to avoid.

  @Test
  void dateTruncatePropagatesAnExplicitNullAdjustmentMap() {
    assertThat(single("RETURN date.truncate('year', date('1984-10-11'), null) AS r")).isNull();
    assertThat(single("RETURN date.truncate('year', date('1984-10-11')) AS r")).hasToString("1984-01-01");
  }

  @Test
  void datetimeTruncatePropagatesAnExplicitNullAdjustmentMap() {
    assertThat(single("RETURN datetime.truncate('year', datetime('1984-10-11T12:31:14Z'), null) AS r")).isNull();
    assertThat(single("RETURN datetime.truncate('year', datetime('1984-10-11T12:31:14Z')) AS r")).hasToString("1984-01-01T00:00Z");
  }

  @Test
  void localdatetimeTruncatePropagatesAnExplicitNullAdjustmentMap() {
    assertThat(single("RETURN localdatetime.truncate('year', localdatetime('1984-10-11T12:31:14'), null) AS r")).isNull();
    assertThat(single("RETURN localdatetime.truncate('year', localdatetime('1984-10-11T12:31:14')) AS r")).hasToString("1984-01-01T00:00");
  }

  @Test
  void timeTruncatePropagatesAnExplicitNullAdjustmentMap() {
    assertThat(single("RETURN time.truncate('hour', time('12:31:14Z'), null) AS r")).isNull();
    assertThat(single("RETURN time.truncate('hour', time('12:31:14Z')) AS r")).hasToString("12:00Z");
  }

  @Test
  void localtimeTruncatePropagatesAnExplicitNullAdjustmentMap() {
    assertThat(single("RETURN localtime.truncate('hour', localtime('12:31:14'), null) AS r")).isNull();
    assertThat(single("RETURN localtime.truncate('hour', localtime('12:31:14')) AS r")).hasToString("12:00");
  }

  @Test
  void theCypherSubstringAlreadyPropagatesAnExplicitNullLength() {
    // Cypher's substring() resolves to CypherSubstringFunction, which issue #5193 had already settled this exact way,
    // citing Neo4j. That precedent is why the rule this issue writes down is a codification rather than a new decision.
    assertThat(single("RETURN substring('hello', 1, null) AS r")).isNull();
    assertThat(single("RETURN substring('hello', 1) AS r")).isEqualTo("ello");
    assertThat(single("RETURN substring('hello', 1, 3) AS r")).isEqualTo("ell");
  }

  @Test
  void theOtherSubstringImplementationAgreesWithIt() {
    // com.arcadedb.function.text.SubstringFunction is a second implementation of the same function that no factory
    // currently registers, so it is exercised directly. It read an explicit null length as "no length given" and ran to
    // the end of the string - the divergence that would surface the day it is wired up.
    final SubstringFunction fn = new SubstringFunction();
    assertThat(fn.execute(new Object[] { "hello", 1, null }, null)).isNull();
    assertThat(fn.execute(new Object[] { "hello", 1 }, null)).isEqualTo("ello");
    assertThat(fn.execute(new Object[] { "hello", 1, 3 }, null)).isEqualTo("ell");
  }

  // ===================== the functions that already propagated stay that way =====================
  //
  // Nine of the twenty-two optional-argument functions were already right. Pinning them here is what stops the
  // convention drifting back one function at a time, which is how normalize() and isNormalized() came to disagree in
  // the first place (issue #5602).

  @Test
  void theFunctionsThatAlreadyPropagatedStillDo() {
    assertThat(single("RETURN ltrim('  hello', null) AS r")).isNull();
    assertThat(single("RETURN rtrim('hello  ', null) AS r")).isNull();
    assertThat(single("RETURN point({x: 1.0, y: null}) AS r")).isNull();
    assertThat(single("RETURN date(null) AS r")).isNull();
    assertThat(single("RETURN datetime(null) AS r")).isNull();
    assertThat(single("RETURN localdatetime(null) AS r")).isNull();
    assertThat(single("RETURN time(null) AS r")).isNull();
    assertThat(single("RETURN localtime(null) AS r")).isNull();
  }

  @Test
  void omittingTheArgumentIsStillDistinctFromWritingNull() {
    // ltrim's trim character is the clearest pair: omitted strips whitespace, explicit null propagates.
    assertThat(single("RETURN ltrim('  hello') AS r")).isEqualTo("hello");
    assertThat(single("RETURN ltrim('xxhello', 'x') AS r")).isEqualTo("hello");
    assertThat(single("RETURN ltrim('xxhello', null) AS r")).isNull();
  }

  // ===================== propagation must not swallow a genuine client error =====================

  @Test
  void anUnusableModeIsStillReportedWhenAnotherArgumentIsNull() {
    // round() validates every argument before null propagation decides the answer, so that an unusable mode is reported
    // even when the value is null (issue #5484). Propagating the explicit null mode must not reorder that.
    assertThatThrownBy(() -> consume("RETURN round(null, 2, 'SIDEWAYS') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("round()")
        .hasMessageContaining("SIDEWAYS");
  }

  @Test
  void aBadTruncateUnitIsStillReportedWhenTheAdjustmentMapIsNull() {
    // Same ordering question as round(): propagating the explicit null must not short-circuit past the checks on the
    // arguments before it, or a wrong query would come back looking like a successful one.
    // The unit is checked deep in TemporalUtil, which still raises a raw IllegalArgumentException rather than a
    // client-facing one - a separate defect, so this asserts only that the check is reached, not the class it uses.
    assertThatThrownBy(() -> consume("RETURN date.truncate('fortnight', date('1984-10-11'), null) AS r"))
        .hasMessageContaining("fortnight");
    assertThatThrownBy(() -> consume("RETURN date.truncate('year', 'not a date', null) AS r"))
        .isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void anUnknownNormalFormIsStillRejected() {
    assertThatThrownBy(() -> consume("RETURN normalize('x', 'NFQ') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("normalize")
        .hasMessageContaining("NFQ");
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
