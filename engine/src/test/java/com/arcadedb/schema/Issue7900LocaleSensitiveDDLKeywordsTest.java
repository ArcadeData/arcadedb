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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7900: SQL DDL normalised its keyword ARGUMENTS with the no-argument
 * {@code String.toUpperCase()}, which uses the JVM default locale.
 * <p>
 * In the Turkish, Azeri and Lithuanian locales {@code 'i'} upper-cases to the DOTTED {@code 'İ'} (U+0130), not to
 * {@code 'I'}, so a keyword the user wrote in lower case stopped matching the constant it is compared against. Two
 * live consequences, both on the same binary with nothing changed but the default locale:
 * <ol>
 *   <li>{@code CREATE INDEX ... (n COLLATE ci) UNIQUE} produced {@code "Cİ"}, which
 *       {@link IndexMetadata#isCaseInsensitive} never recognises, so the index was built case-SENSITIVE with no
 *       warning: the unique constraint stopped firing and an indexed lookup answered fewer rows than the same
 *       predicate without the index. And because the collation list is PERSISTED, moving the database back to a
 *       normal-locale server did NOT repair an index already built.</li>
 *   <li>{@code NULL_STRATEGY index} could not be run at all - {@code IllegalArgumentException: No enum constant
 *       ... NULL_STRATEGY.İNDEX}.</li>
 * </ol>
 * Both work in UPPER case, which is what hid this: SQL keywords are case-insensitive everywhere else in the
 * dialect.
 * <p>
 * {@code @Isolated} because {@link Locale#setDefault} is process-wide, the same reason
 * {@code com.arcadedb.function.LocaleSensitivityTest} carries it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Isolated
class Issue7900LocaleSensitiveDDLKeywordsTest extends TestHelper {
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

  /**
   * The silent one: a declared case-insensitive UNIQUE constraint that simply stops being enforced. The lower-case
   * keyword must behave exactly as the upper-case one does.
   */
  @Test
  void lowerCaseCollateCiStillBuildsACaseInsensitiveIndex() {
    // the locale really is the hostile one, or this test proves nothing
    assertThat("ci".toUpperCase()).isNotEqualTo("CI");

    database.command("sql", "create document type LowerCi");
    database.command("sql", "create property LowerCi.n STRING");
    database.command("sql", "create index on LowerCi (n collate ci) unique");

    final TypeIndex index = database.getSchema().getType("LowerCi").getIndexesByProperties("n").iterator().next();
    for (final IndexInternal sub : index.getIndexesOnBuckets())
      assertThat(sub.getMetadata().isCaseInsensitive(0))
          .as("COLLATE ci must reach the index as the CI collation whatever the server's locale")
          .isTrue();

    database.transaction(() -> database.command("sql", "insert into LowerCi set n = 'Alpha'"));

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "insert into LowerCi set n = 'alpha'")))
        .as("the UNIQUE constraint must still fire on a case-insensitive duplicate")
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(database.query("sql", "select from LowerCi where n = 'ALPHA'").stream().count())
        .as("an indexed lookup must not answer fewer rows than the same predicate without the index")
        .isEqualTo(1);
  }

  /** The upper-case spelling, which always worked: it must keep working and must normalise to the same thing. */
  @Test
  void upperCaseCollateCiIsUnchanged() {
    database.command("sql", "create document type UpperCi");
    database.command("sql", "create property UpperCi.n STRING");
    database.command("sql", "create index on UpperCi (n collate CI) unique");

    final TypeIndex index = database.getSchema().getType("UpperCi").getIndexesByProperties("n").iterator().next();
    for (final IndexInternal sub : index.getIndexesOnBuckets())
      assertThat(sub.getMetadata().isCaseInsensitive(0)).isTrue();
  }

  /** The loud one: a DDL statement that could not be run at all under a hostile locale. */
  @Test
  void lowerCaseNullStrategyParsesAndRuns() {
    database.command("sql", "create document type NullStrat");
    database.command("sql", "create property NullStrat.other STRING");

    database.command("sql", "create index on NullStrat (other) notunique null_strategy index");

    assertThat(database.getSchema().getType("NullStrat").getIndexesByProperties("other")).isNotEmpty();
  }

  /** A lower-case vector similarity: same family, same `i`, one of the sites the issue listed as waiting. */
  @Test
  void lowerCaseVectorSimilarityIsRecognised() {
    final LSMVectorIndexMetadata metadata = new LSMVectorIndexMetadata("V", new String[] { "vec" }, 0);
    metadata.setSimilarity("cosine");
    metadata.setQuantization("binary");

    assertThat(metadata.similarityFunction.name()).isEqualTo("COSINE");
    assertThat(metadata.quantizationType.name()).isEqualTo("BINARY");
  }

  /**
   * The better half of the fix: a collation this engine does not implement is REFUSED rather than stored. That
   * covers every other way a wrong keyword can arrive - a typo included - not only the locale one, and it is what
   * makes the failure impossible to persist silently.
   */
  @Test
  void anUnknownCollationIsRefusedRatherThanStored() {
    assertThatThrownBy(() -> IndexMetadata.normalizeCollation("nosuchcollation"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nosuchcollation");

    database.command("sql", "create document type BadCollate");
    database.command("sql", "create property BadCollate.n STRING");

    // reported as a PARSING error, the same classification the METADATA clause gets, so a client mistake in the
    // statement answers 400 rather than escaping as a bare IllegalArgumentException (PR #7942 review)
    assertThatThrownBy(() -> database.command("sql", "create index on BadCollate (n collate nosuch) unique"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("COLLATE")
        .hasMessageContaining("nosuch");
  }

  @Test
  void normalizeCollationFoldsWithTheRootLocale() {
    assertThat(IndexMetadata.normalizeCollation("ci")).isEqualTo(IndexMetadata.COLLATION_CI);
    assertThat(IndexMetadata.normalizeCollation("Ci")).isEqualTo(IndexMetadata.COLLATION_CI);
    assertThat(IndexMetadata.normalizeCollation(" default ")).isEqualTo(IndexMetadata.COLLATION_DEFAULT);
    assertThat(IndexMetadata.normalizeCollation(null)).isEqualTo(IndexMetadata.COLLATION_DEFAULT);
  }

  /**
   * {@code ILIKE} folded its two sides with DIFFERENT locales - the left with {@code Locale.ENGLISH}, the right
   * with the JVM default - so under tr_TR a pattern carrying an {@code I} stopped matching the value it was
   * compared against.
   */
  @Test
  void ilikeFoldsBothSidesTheSameWay() {
    database.command("sql", "create document type IlikeDocs");
    database.transaction(() -> database.command("sql", "insert into IlikeDocs set n = 'INDEX'"));

    assertThat(database.select().fromType("IlikeDocs").where().property("n").ilike().value("index").documents().toList())
        .as("both sides of ILIKE must fold with the same locale")
        .hasSize(1);
  }

  /** The shape of the keyword list this test exercises, kept beside it so a new one is obvious to add. */
  @Test
  void theLocaleUnderTestReallyBreaksTheseKeywords() {
    for (final String keyword : List.of("ci", "index", "cosine", "insert", "binary"))
      assertThat(keyword.toUpperCase())
          .as("'%s' must upper-case DIFFERENTLY under the test locale, or this class asserts nothing", keyword)
          .isNotEqualTo(keyword.toUpperCase(Locale.ROOT));
  }
}
