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
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7703: a rendered {@code CREATE TIMESERIES TYPE} must name a codec its caller NAMED, even when that codec
 * is what this build's default table would have resolved anyway.
 * <p>
 * The renderer used to omit the clause on "equals the default", because a {@link ColumnDefinition} always carries a
 * codec and that was the only way to ask whether one had been chosen. That is the right answer for
 * {@code withTag}/{@code withField}, which never named one: pinning the resolved codec into the DDL would freeze
 * today's table into every rendered statement. It is the wrong answer for a RESTORE. A {@code .jsonl} export
 * records {@code "compression"} per column precisely because the codec is NOT re-derivable (issue #5475), and
 * {@code JsonlImporterFormat.createTimeSeriesType} reads it back for the same reason; rendered through the DDL path
 * into a build whose default for that {@code (type, role)} pair has since moved, the omission silently substitutes
 * the NEW default for the exported codec, and the restored type is not the exported one.
 * <p>
 * {@code ColumnDefinition.legacyCodecFor} is the proof the table moves: it exists because it already has.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7703ExplicitCodecSurvivesDefaultTableChangeTest extends TestHelper {

  private TimeSeriesTypeBuilder builder(final String name) {
    return database.getSchema().buildTimeSeriesType().withName(name).withTimestamp("ts");
  }

  /**
   * The defect itself: a column that named the codec the default table happens to return today renders WITH the
   * clause, so the receiving build applies it rather than re-deriving it.
   */
  @Test
  void aNamedCodecThatEqualsTodaysDefaultStillRenders() {
    final TimeSeriesCodec todaysDefault =
        ColumnDefinition.defaultCodecFor(Type.LONG, ColumnDefinition.ColumnRole.FIELD);

    final String sql = builder("NamedDefaultCodec")
        .withColumn(new ColumnDefinition("v", Type.LONG, ColumnDefinition.ColumnRole.FIELD, todaysDefault))
        .toSQL().getFirst();

    assertThat(sql)
        .as("the codec was NAMED, so the statement must carry it instead of leaving it to the receiver's table")
        .contains("`v` LONG CODEC " + todaysDefault.name());

    database.command("sql", sql);
    assertThat(((TimeSeriesType) database.getSchema().getType("NamedDefaultCodec")).getTsColumn("v")
        .getCompressionHint()).isEqualTo(todaysDefault);
  }

  /**
   * The other half, unchanged: a column built by {@code withTag}/{@code withField}/{@code withTimestamp} named
   * nothing, so the statement names nothing and the receiver resolves its own default. This is what stops the fix
   * from freezing the default table into every create a user writes.
   */
  @Test
  void aDerivedCodecStillRendersWithoutAClause() {
    final String sql = builder("DerivedCodecs").withField("v", Type.DOUBLE).withTag("host", Type.STRING)
        .toSQL().getFirst();

    assertThat(sql).doesNotContain("CODEC");
  }

  /**
   * The flag is a property of the column, not of the renderer, so every hop a definition makes carries it: the two
   * constructors are the whole distinction, and the 4-argument one is what the importer, the SQL parser's
   * {@code CODEC} arm and {@code LocalTimeSeriesType.fromJSON} all use.
   */
  @Test
  void theConstructorIsWhatDecidesWhetherACodecWasNamed() {
    assertThat(new ColumnDefinition("v", Type.LONG, ColumnDefinition.ColumnRole.FIELD).isExplicitCodec())
        .as("derived from the default table")
        .isFalse();
    assertThat(new ColumnDefinition("v", Type.LONG, ColumnDefinition.ColumnRole.FIELD, TimeSeriesCodec.DICTIONARY)
        .isExplicitCodec())
        .as("named, and different from the default")
        .isTrue();
    assertThat(new ColumnDefinition("v", Type.LONG, ColumnDefinition.ColumnRole.FIELD,
        ColumnDefinition.defaultCodecFor(Type.LONG, ColumnDefinition.ColumnRole.FIELD)).isExplicitCodec())
        .as("named, and identical to the default - the case the old 'differs from the default' test could not see")
        .isTrue();
  }

  /**
   * A {@code null} codec is "none named" rather than a stored null: the column resolves the default table and is
   * indistinguishable from the 3-argument one, so no read path has to cope with a null codec and no renderer emits
   * a clause for a codec nobody chose.
   */
  @Test
  void aNullCodecIsNoneNamedRatherThanAStoredNull() {
    final ColumnDefinition column = new ColumnDefinition("v", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, null);

    assertThat(column.isExplicitCodec()).isFalse();
    assertThat(column.getCompressionHint())
        .isEqualTo(ColumnDefinition.defaultCodecFor(Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  /**
   * The timestamp column goes through the same rule, on the path {@code renderCreate} gives it of its own:
   * {@code withTimestamp} names nothing, {@code withColumn} does.
   */
  @Test
  void theTimestampColumnFollowsTheSameRule() {
    final TimeSeriesCodec todaysDefault =
        ColumnDefinition.defaultCodecFor(Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP);

    final String named = database.getSchema().buildTimeSeriesType().withName("NamedTsCodec")
        .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP, todaysDefault))
        .withField("v", Type.DOUBLE)
        .toSQL().getFirst();

    assertThat(named).contains("TIMESTAMP `ts` CODEC " + todaysDefault.name());
    assertThat(builder("DerivedTsCodec").withField("v", Type.DOUBLE).toSQL().getFirst()).doesNotContain("CODEC");
  }
}
