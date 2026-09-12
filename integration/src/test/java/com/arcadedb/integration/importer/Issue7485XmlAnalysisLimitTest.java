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
package com.arcadedb.integration.importer;

import com.arcadedb.integration.importer.format.XMLImporterFormat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7485: {@code XMLImporterFormat.analyze()} read its entries cap from the generic options map under the
 * misspelled, default-less key {@code analyzingLimitEntries} instead of the real {@code ImporterSettings} field
 * {@code analysisLimitEntries} - the one with a default of 10000 that {@code CSVImporterFormat.analyze()} already
 * honours - so:
 * <ul>
 *   <li>{@code -analysisLimitEntries 500}, the documented flag, did nothing on an XML source;</li>
 *   <li>with neither flag passed, XML schema analysis was UNBOUNDED, walking the entire file before {@code load()}
 *   walks it a second time;</li>
 *   <li>there was no XML equivalent of {@code -analysisLimitBytes} at all.</li>
 * </ul>
 * The fix reads {@code settings.analysisLimitEntries} (falling back to the old {@code analyzingLimitEntries} key
 * only when a caller still passes it explicitly) and adds the same {@code -analysisLimitBytes} guard
 * {@code CSVImporterFormat.analyze()} already has.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7485XmlAnalysisLimitTest {

  private static final int OBJECTS = 6;

  @Test
  void theDefaultAnalysisLimitEntriesCapsXmlSchemaAnalysis() throws Exception {
    final ImporterSettings settings = new ImporterSettings();
    settings.analysisLimitEntries = 3;

    final AnalyzedSchema analyzedSchema = new AnalyzedSchema(100);
    new XMLImporterFormat().analyze(AnalyzedEntity.EntityType.DOCUMENT, parserOf(distinctlyPropertiedXml()), settings,
        analyzedSchema);

    assertThat(analyzedSchema.getEntity("item").getProperties())
        .as("-analysisLimitEntries - the real, defaulted setting, not the misspelled 'analyzingLimitEntries' - must "
            + "cap XML schema analysis the same way it already caps CSV's")
        .hasSize(3);
  }

  @Test
  void withNoLimitSetAtAllAnalysisIsStillUnbounded() throws Exception {
    final AnalyzedSchema analyzedSchema = new AnalyzedSchema(100);
    final ImporterSettings settings = new ImporterSettings();
    // ImporterSettings' OWN DEFAULT (10000) IS FAR ABOVE OBJECTS, SO THE WHOLE SOURCE IS STILL ANALYZED - THIS IS
    // NOT "NO CAP", IT IS "A GENEROUS DEFAULT CAP", EXACTLY LIKE analysisLimitBytes/analysisLimitEntries FOR CSV.
    new XMLImporterFormat().analyze(AnalyzedEntity.EntityType.DOCUMENT, parserOf(distinctlyPropertiedXml()), settings,
        analyzedSchema);

    assertThat(analyzedSchema.getEntity("item").getProperties()).hasSize(OBJECTS);
  }

  /**
   * The deprecated alias some script out there may already pass: still honoured, and still takes priority when
   * explicitly set (mirroring how a caller who set BOTH old and new flags almost certainly means the one they
   * bothered to pass).
   */
  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 4 })
  void theDeprecatedAnalyzingLimitEntriesAliasStillWorks(final int limit) throws Exception {
    final ImporterSettings settings = new ImporterSettings();
    settings.options.put("analyzingLimitEntries", String.valueOf(limit));

    final AnalyzedSchema analyzedSchema = new AnalyzedSchema(100);
    new XMLImporterFormat().analyze(AnalyzedEntity.EntityType.DOCUMENT, parserOf(distinctlyPropertiedXml()), settings,
        analyzedSchema);

    assertThat(analyzedSchema.getEntity("item").getProperties()).hasSize(limit);
  }

  /**
   * Both flags passed together: the deprecated one still wins (a caller who bothered to pass it almost certainly
   * means it), and it is read with the same range as the field it overrides - {@code getLongValue}, not
   * {@code getIntValue} - so a caller cannot silently lose precision by using the deprecated spelling.
   */
  @Test
  void whenBothFlagsAreSetTheDeprecatedAliasWinsAtFullLongRange() throws Exception {
    final ImporterSettings settings = new ImporterSettings();
    settings.parseParameter("analysisLimitEntries", "5");
    settings.options.put("analyzingLimitEntries", "2");

    final AnalyzedSchema analyzedSchema = new AnalyzedSchema(100);
    new XMLImporterFormat().analyze(AnalyzedEntity.EntityType.DOCUMENT, parserOf(distinctlyPropertiedXml()), settings,
        analyzedSchema);

    assertThat(analyzedSchema.getEntity("item").getProperties())
        .as("the deprecated -analyzingLimitEntries (2) must win over -analysisLimitEntries (5) when both are set")
        .hasSize(2);
  }

  @Test
  void analysisLimitBytesCapsXmlSchemaAnalysisToo() throws Exception {
    final String xml = distinctlyPropertiedXml();
    // A byte budget that lands partway through the source: strictly less than the whole thing, but enough to reach
    // past the opening <root> and at least the first object.
    final int budget = xml.indexOf("<item p2") ;

    final ImporterSettings settings = new ImporterSettings();
    settings.analysisLimitBytes = budget;

    final AnalyzedSchema analyzedSchema = new AnalyzedSchema(100);
    new XMLImporterFormat().analyze(AnalyzedEntity.EntityType.DOCUMENT, parserOf(xml), settings, analyzedSchema);

    assertThat(analyzedSchema.getEntity("item").getProperties().size())
        .as("-analysisLimitBytes must stop analysis before the byte budget's worth of the source has all been read, "
            + "which for a source with more than one object means fewer objects analyzed than the whole file has")
        .isPositive().isLessThan(OBJECTS);
  }

  // -----------------------------------------------------------------------------------------------------------

  private static String distinctlyPropertiedXml() {
    final StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<root>\n");
    for (int i = 1; i <= OBJECTS; ++i)
      xml.append("  <item p").append(i).append("=\"v\"/>\n");
    return xml.append("</root>").toString();
  }

  private static Parser parserOf(final String content) throws Exception {
    final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    return new Parser(new Source("test.xml", new ByteArrayInputStream(bytes), bytes.length, false, null, null), 0);
  }
}
