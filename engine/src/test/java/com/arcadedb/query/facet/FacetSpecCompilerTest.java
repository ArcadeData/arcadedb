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
package com.arcadedb.query.facet;

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.parser.FacetItem;
import com.arcadedb.query.sql.parser.FacetsClause;
import com.arcadedb.query.sql.parser.SelectStatement;
import org.junit.jupiter.api.Test;

import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FacetSpecCompilerTest {

  private static FacetsClause clause(final String facets) {
    return ((SelectStatement) new SQLAntlrParser(null).parse("SELECT FROM A FACETS " + facets)).getFacets();
  }

  private static List<FacetSpec> compile(final String facets, final Map<String, Object> params) {
    final CommandContext ctx = new BasicCommandContext();
    ctx.setInputParameters(params);
    return FacetSpecCompiler.compile(clause(facets), ctx);
  }

  private static FacetSpec only(final String facets) {
    final List<FacetSpec> specs = compile(facets, Map.of());
    assertThat(specs).hasSize(1);
    return specs.getFirst();
  }

  @Test
  void defaults() {
    final TermsFacetSpec t = (TermsFacetSpec) only("brand TERMS");
    assertThat(t.top()).isEqualTo(10);
    assertThat(t.minCount()).isEqualTo(1);
    assertThat(t.order()).isEqualTo(FacetOrder.COUNT_DESC);
    assertThat(t.property()).isEqualTo("brand");
    assertThat(t.alias()).isEqualTo("brand");
    assertThat(t.expression()).isNull();
    assertThat(t.missingBucket()).isFalse();
    assertThat(t.include()).isNull();
  }

  @Test
  void topAboveMaxIsRefused() {
    assertThatThrownBy(() -> only("brand TERMS TOP 1001")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("arcadedb.columnar.topMax").hasMessageContaining("1000");
    assertThat(((TermsFacetSpec) only("brand TERMS TOP 1000")).top()).isEqualTo(1000);
  }

  @Test
  void orderByVariants() {
    assertThat(((TermsFacetSpec) only("b TERMS ORDER BY VALUE")).order()).isEqualTo(FacetOrder.VALUE_ASC);
    assertThat(((TermsFacetSpec) only("b TERMS ORDER BY VALUE DESC")).order()).isEqualTo(FacetOrder.VALUE_DESC);
    assertThat(((TermsFacetSpec) only("b TERMS ORDER BY COUNT ASC")).order()).isEqualTo(FacetOrder.COUNT_ASC);
    assertThat(((TermsFacetSpec) only("b TERMS ORDER BY COUNT")).order()).isEqualTo(FacetOrder.COUNT_DESC);
  }

  @Test
  void calendarUnits() {
    assertThat(FacetSpecCompiler.parseInterval(true, "1M").calendarUnit()).isEqualTo(ChronoUnit.MONTHS);
    assertThat(FacetSpecCompiler.parseInterval(true, "1m").calendarUnit()).isEqualTo(ChronoUnit.MINUTES);
    assertThat(FacetSpecCompiler.parseInterval(true, "1h").calendarUnit()).isEqualTo(ChronoUnit.HOURS);
    assertThat(FacetSpecCompiler.parseInterval(true, "1d").calendarUnit()).isEqualTo(ChronoUnit.DAYS);
    assertThat(FacetSpecCompiler.parseInterval(true, "1w").calendarUnit()).isEqualTo(ChronoUnit.WEEKS);
    assertThat(FacetSpecCompiler.parseInterval(true, "1q").calendarUnit()).isEqualTo(IsoFields.QUARTER_YEARS);
    assertThat(FacetSpecCompiler.parseInterval(true, "1y").calendarUnit()).isEqualTo(ChronoUnit.YEARS);
    assertThat(FacetSpecCompiler.parseInterval(true, "1y").calendar()).isTrue();
  }

  @Test
  void calendarRefusals() {
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(true, "2M")).hasMessageContaining("CALENDAR intervals take a multiplier of 1");
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(true, "1x")).isInstanceOf(CommandSQLParsingException.class);
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(true, 5)).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void fixedMonthIsRefused() {
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(false, "1M")).hasMessageContaining("use CALENDAR '1M'");
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(false, "1q")).hasMessageContaining("use CALENDAR '1q'");
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(false, "1y")).hasMessageContaining("use CALENDAR '1y'");
  }

  @Test
  void fixedUsesTimeBucketUnits() {
    final HistogramInterval interval = FacetSpecCompiler.parseInterval(false, "90m");
    assertThat(interval.calendar()).isFalse();
    assertThat(interval.fixedMillis()).isEqualTo(5_400_000L);
    assertThat(FacetSpecCompiler.parseInterval(false, "1w").fixedMillis()).isEqualTo(7 * 86_400_000L);
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(false, "5x")).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void fixedNumberForNumeric() {
    assertThat(FacetSpecCompiler.parseInterval(false, 100).fixedNumber()).isEqualTo(100d);
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(false, 0)).isInstanceOf(CommandSQLParsingException.class);
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(false, -2.5)).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void rangeDefaultKeys() {
    final RangeFacetSpec spec = (RangeFacetSpec) only("s: size RANGES ([0, 10), [10, *), [*, 5) AS 'tiny') DEFAULT 'other'");
    assertThat(spec.ranges()).extracting(RangeBound::key).containsExactly("0-10", "10-*", "tiny");
    assertThat(spec.ranges().get(1).to()).isNull();
    assertThat(spec.ranges().get(0).from()).isEqualTo(0);
    assertThat(spec.defaultLabel()).isEqualTo("other");
  }

  @Test
  void emptyRangeIsRefused() {
    assertThatThrownBy(() -> only("s: size RANGES ([10, 10))")).hasMessageContaining("empty range [10, 10)");
    assertThatThrownBy(() -> only("s: size RANGES ([10, 9))")).hasMessageContaining("empty range [10, 9)");
    assertThat(((RangeFacetSpec) only("s: size RANGES ([9, 10))")).ranges()).hasSize(1);
  }

  @Test
  void unknownZoneIsRefused() {
    assertThatThrownBy(() -> only("p: d HISTOGRAM CALENDAR '1d' TIMEZONE 'Mars/Base'")).hasMessageContaining("Mars/Base");
    final HistogramFacetSpec ok = (HistogramFacetSpec) only("p: d HISTOGRAM CALENDAR '1d' TIMEZONE 'Europe/Rome' MIN_COUNT 0");
    assertThat(ok.timezone()).isEqualTo("Europe/Rome");
    assertThat(ok.minCount()).isZero();
    assertThat(ok.interval().calendarUnit()).isEqualTo(ChronoUnit.DAYS);
  }

  @Test
  void fixedTimezoneFollowsTimeBucketRules() {
    assertThat(((HistogramFacetSpec) only("p: d HISTOGRAM FIXED '1d' TIMEZONE '+02:00'")).timezone()).isEqualTo("+02:00");
    assertThatThrownBy(() -> only("p: d HISTOGRAM FIXED '1d' TIMEZONE 'Europe/Rome'")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Europe/Rome");
  }

  @Test
  void histogramBounds() {
    final HistogramFacetSpec spec = (HistogramFacetSpec) only("h: n HISTOGRAM FIXED 10 BOUNDS [0, 100]");
    assertThat(spec.boundsFrom()).isEqualTo(0);
    assertThat(spec.boundsTo()).isEqualTo(100);
    assertThat(spec.interval().fixedNumber()).isEqualTo(10d);
  }

  @Test
  void unknownKindIsRefused() {
    final FacetsClause c = clause("t: tags TERMS");
    c.items.getFirst().kind = FacetItem.Kind.HIERARCHY;
    assertThatThrownBy(() -> FacetSpecCompiler.compile(c, new BasicCommandContext())).isInstanceOf(CommandSQLParsingException.class)
        .hasMessage("FACETS t: HIERARCHY facets are not supported");
  }

  @Test
  void parameterInIncludeAndBoundsResolves() {
    final List<FacetSpec> specs = compile(
        "b TERMS INCLUDE [:v, 'x'] MISSING, s: size RANGES ([:lo, :hi)), h: n HISTOGRAM FIXED 5 BOUNDS [:lo, :hi]",
        Map.of("v", "Nike", "lo", 5, "hi", 50));
    final TermsFacetSpec terms = (TermsFacetSpec) specs.get(0);
    assertThat(terms.include()).containsExactly("Nike", "x");
    assertThat(terms.missingBucket()).isTrue();
    final RangeBound bound = ((RangeFacetSpec) specs.get(1)).ranges().getFirst();
    assertThat(bound.from()).isEqualTo(5);
    assertThat(bound.to()).isEqualTo(50);
    assertThat(bound.key()).isEqualTo("5-50");
    assertThat(((HistogramFacetSpec) specs.get(2)).boundsTo()).isEqualTo(50);
  }

  @Test
  void expressionFacetHasNullProperty() {
    final TermsFacetSpec spec = (TermsFacetSpec) only("x: brand.toLowerCase() TERMS");
    assertThat(spec.property()).isNull();
    assertThat(spec.expression()).isNotNull();
    assertThat(spec.alias()).isEqualTo("x");
  }

  @Test
  void dottedPathIsAProperty() {
    final TermsFacetSpec spec = (TermsFacetSpec) only("a.b TERMS");
    assertThat(spec.property()).isEqualTo("a.b");
    assertThat(spec.expression()).isNull();
  }

  @Test
  void filterIsCarried() {
    final TermsFacetSpec spec = (TermsFacetSpec) only("brand TERMS FILTER brand IN ['Nike']");
    assertThat(spec.filter()).isNotNull();
  }

  @Test
  void integralBoundsAbove2Pow53AreNotEqual() {
    assertThat(((RangeFacetSpec) only("s: size RANGES ([9007199254740992, 9007199254740993))")).ranges()).hasSize(1);
    assertThatThrownBy(() -> only("s: size RANGES ([9007199254740993, 9007199254740993))")).hasMessageContaining("empty range");
    assertThatThrownBy(() -> only("s: size RANGES ([9007199254740993, 9007199254740992))")).hasMessageContaining("empty range");
  }

  @Test
  void mixedLongAndDoubleBounds() {
    assertThatThrownBy(() -> only("s: size RANGES ([1, 0.5))")).hasMessageContaining("empty range");
    assertThat(((RangeFacetSpec) only("s: size RANGES ([0.5, 1))")).ranges()).hasSize(1);
  }

  @Test
  void positionalParameters() {
    final List<FacetSpec> specs = compile("b TERMS INCLUDE [?, 'x'], s: size RANGES ([?, ?))", Map.of("0", "Nike", "1", 5, "2", 50));
    assertThat(((TermsFacetSpec) specs.get(0)).include()).containsExactly("Nike", "x");
    final RangeBound bound = ((RangeFacetSpec) specs.get(1)).ranges().getFirst();
    assertThat(bound.from()).isEqualTo(5);
    assertThat(bound.to()).isEqualTo(50);
  }

  @Test
  void parameterBoundToNullIsRefused() {
    assertThatThrownBy(() -> compile("s: size RANGES ([:lo, 10))", Map.of())).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("bound to null");
    assertThatThrownBy(() -> compile("h: n HISTOGRAM FIXED 5 BOUNDS [:lo, 10]", Map.of())).hasMessageContaining("bound to null");
  }

  @Test
  void timezoneWithOffsetOnFixedIsRefused() {
    assertThatThrownBy(() -> only("p: d HISTOGRAM FIXED '1d' TIMEZONE '+02:00' OFFSET '1h'")).hasMessageContaining("only one of");
  }

  @Test
  void offsetIsValidated() {
    assertThat(((HistogramFacetSpec) only("p: d HISTOGRAM FIXED '1d' OFFSET '-8h'")).offset()).isEqualTo("-8h");
    assertThat(((HistogramFacetSpec) only("p: d HISTOGRAM CALENDAR '1d' OFFSET '2h'")).offset()).isEqualTo("2h");
    assertThatThrownBy(() -> only("p: d HISTOGRAM FIXED '1d' OFFSET 'zzz'")).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void numericHistogramTakesANumberOffsetAndNoTimezone() {
    assertThat(((HistogramFacetSpec) only("h: n HISTOGRAM FIXED 100 OFFSET '+50'")).offset()).isEqualTo("+50");
    assertThat(((HistogramFacetSpec) only("h: n HISTOGRAM FIXED 0.5 OFFSET '-0.25'")).offset()).isEqualTo("-0.25");
    assertThatThrownBy(() -> only("h: n HISTOGRAM FIXED 100 OFFSET '1h'")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("OFFSET of a numeric histogram must be a number");
    assertThatThrownBy(() -> only("h: n HISTOGRAM FIXED 100 TIMEZONE 'UTC'")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("TIMEZONE applies only to time histograms");
  }

  @Test
  void fixedUppercaseYearAndQuarterGetCalendarHint() {
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(false, "1Y")).hasMessageContaining("use CALENDAR");
    assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(false, "1Q")).hasMessageContaining("use CALENDAR");
  }

  @Test
  void duplicateRangeKeysAreRefused() {
    assertThatThrownBy(() -> only("s: size RANGES ([0, 10), [0, 10))")).hasMessageContaining("duplicate range key '0-10'");
    assertThatThrownBy(() -> only("s: size RANGES ([0, 10) AS 'a', [10, 20) AS 'a')")).hasMessageContaining("duplicate range key 'a'");
  }

  @Test
  void tooManyBucketsFromBoundsIsRefused() {
    assertThatThrownBy(() -> only("h: n HISTOGRAM FIXED 1 BOUNDS [0, 200000]")).hasMessageContaining("100000 buckets");
    assertThat(((HistogramFacetSpec) only("h: n HISTOGRAM FIXED 1 BOUNDS [0, 100000]")).boundsTo()).isEqualTo(100000);
  }
}
