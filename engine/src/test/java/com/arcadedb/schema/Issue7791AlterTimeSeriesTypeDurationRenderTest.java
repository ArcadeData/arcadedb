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
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7791: {@code AlterTimeSeriesTypeStatement.toString()} rendered a downsampling tier as a bare millisecond
 * count - {@code tier.afterMs()}/{@code tier.granularityMs()} appended directly - instead of through
 * {@code renderDuration()} like the identical sibling {@code CreateTimeSeriesTypeStatement} does. The grammar rule
 * {@code downsamplingTierClause} requires a {@code tsTimeUnit} after each {@code INTEGER_LITERAL}, so the rendered
 * ALTER was not valid SQL at all: printing it and parsing it back threw a syntax error.
 * <p>
 * {@code renderDuration()} is now shared on {@link Statement}, next to {@code appendWithSettings}, so both DDL
 * statements that carry a duration in their AST render it the same way and cannot drift apart again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7791AlterTimeSeriesTypeDurationRenderTest extends TestHelper {

  @Test
  void theAlterStatementPrintsBackToSQLThatParsesToAnEqualStatement() {
    database.command("sql", "CREATE TIMESERIES TYPE SensorDDL TIMESTAMP ts FIELDS (v DOUBLE)");

    final String original = "ALTER TIMESERIES TYPE SensorDDL ADD DOWNSAMPLING POLICY "
        + "AFTER 7 DAYS GRANULARITY 1 HOURS AFTER 30 DAYS GRANULARITY 1 DAYS";

    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final Statement parsed = parser.parse(original);
    final String printed = parsed.toString();

    // The bare-millisecond bug rendered "AFTER 604800000 GRANULARITY 3600000 ...", which parses as neither DAYS
    // nor HOURS nor anything else: the grammar requires a unit, so it threw before this assertion could even run.
    assertThat(printed).isEqualTo(original);

    final Statement reparsed = parser.parse(printed);
    assertThat(reparsed).isEqualTo(parsed);
  }

  @Test
  void aTierWithADurationThatIsNotAWholeNumberOfTheLargestUnitStillRendersAndReparses() {
    database.command("sql", "CREATE TIMESERIES TYPE SensorOddDuration TIMESTAMP ts FIELDS (v DOUBLE)");

    // 90 minutes is not a whole number of hours, so renderDuration() falls back to the next-smaller unit that
    // divides it evenly (MINUTES), exactly as CreateTimeSeriesTypeStatement already does for RETENTION/
    // COMPACTION_INTERVAL.
    final String original = "ALTER TIMESERIES TYPE SensorOddDuration ADD DOWNSAMPLING POLICY "
        + "AFTER 90 MINUTES GRANULARITY 5 MINUTES";

    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final Statement parsed = parser.parse(original);
    final String printed = parsed.toString();

    assertThat(parser.parse(printed)).isEqualTo(parsed);
  }

  @Test
  void theExecutedAlterAppliesTheSameTiersTheRoundTrippedStatementWouldReparseTo() {
    database.command("sql", "CREATE TIMESERIES TYPE SensorApplied TIMESTAMP ts FIELDS (v DOUBLE)");
    database.command("sql", "ALTER TIMESERIES TYPE SensorApplied ADD DOWNSAMPLING POLICY "
        + "AFTER 7 DAYS GRANULARITY 1 HOURS AFTER 30 DAYS GRANULARITY 1 DAYS");

    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("SensorApplied");
    assertThat(type.getDownsamplingTiers()).containsExactly(
        new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
        new DownsamplingTier(30L * 86_400_000L, 86_400_000L));
  }
}
