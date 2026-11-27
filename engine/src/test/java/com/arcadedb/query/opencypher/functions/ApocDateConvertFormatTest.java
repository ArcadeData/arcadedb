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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * apoc.date.convertFormat(temporal, currentFormat, convertTo): every expected value is what Neo4j 5.26 with APOC core 5.26.33
 * answers for the same call. {@code @neo4j/graphql} uses it to render a DateTime aggregate.
 */
class ApocDateConvertFormatTest extends TestHelper {
  @Test
  void convertsBetweenNamedFormats() {
    assertThat(eval("apoc.date.convertFormat('2021-01-01T10:00:00Z', 'iso_zoned_date_time', 'iso_offset_date_time')")).isEqualTo("2021-01-01T10:00:00Z");
    // the zone id of a zoned value is dropped by the offset format
    assertThat(eval("apoc.date.convertFormat('2021-01-01T10:00:00+01:00[Europe/Berlin]', 'iso_zoned_date_time', 'iso_offset_date_time')"))
        .isEqualTo("2021-01-01T10:00:00+01:00");
    assertThat(eval("apoc.date.convertFormat('2021-01-01T10:00:00.123456789Z', 'ISO_ZONED_DATE_TIME', 'iso_offset_date_time')"))
        .isEqualTo("2021-01-01T10:00:00.123456789Z");
    assertThat(eval("apoc.date.convertFormat('2021-01-01', 'iso_local_date', 'basic_iso_date')")).isEqualTo("20210101");
  }

  @Test
  void convertsPatterns() {
    assertThat(eval("apoc.date.convertFormat('2021-01-01', 'yyyy-MM-dd', 'dd/MM/yyyy')")).isEqualTo("01/01/2021");
    assertThat(eval("apoc.date.convertFormat('2021-01-01T10:00:00Z', 'iso_zoned_date_time', 'HH:mm')")).isEqualTo("10:00");
  }

  @Test
  void theThirdArgumentDefaultsToTheLocalDate() {
    assertThat(eval("apoc.date.convertFormat('2021-01-01T10:00:00Z', 'iso_zoned_date_time')")).isEqualTo("2021-01-01");
  }

  @Test
  void nullAndEmptyInputAreNull() {
    assertThat(eval("apoc.date.convertFormat(null, 'iso_zoned_date_time', 'iso_offset_date_time')")).isNull();
    assertThat(eval("apoc.date.convertFormat('', 'iso_zoned_date_time', 'iso_offset_date_time')")).isNull();
  }

  @Test
  void anInputOrAFormatNothingReadsIsAClientError() {
    assertThatThrownBy(() -> eval("apoc.date.convertFormat('garbage', 'iso_zoned_date_time', 'iso_offset_date_time')"))
        .hasMessageContaining("could not be parsed");
    assertThatThrownBy(() -> eval("apoc.date.convertFormat('2021-01-01', 'nope', 'iso_offset_date_time')"))
        .hasMessageContaining("Unknown pattern letter");
    assertThat(ErrorCategory.of(catchError("apoc.date.convertFormat('garbage', 'iso_zoned_date_time', 'iso_offset_date_time')")))
        .isEqualTo(ErrorCategory.VALIDATION);
  }

  @Test
  void aDateTimeAggregateRendersAsTheLibraryAsksForIt() {
    // the shape @neo4j/graphql generates for min/max over a DateTime property
    assertThat(eval("apoc.date.convertFormat(toString(max(datetime('2021-01-01T10:00:00+01:00[Europe/Berlin]'))), 'iso_zoned_date_time', 'iso_offset_date_time')"))
        .isEqualTo("2021-01-01T10:00:00+01:00");
  }

  private Object eval(final String expression) {
    try (final ResultSet rs = database.query("opencypher", "RETURN " + expression + " AS r")) {
      return rs.next().getProperty("r");
    }
  }

  private Throwable catchError(final String expression) {
    try {
      eval(expression);
    } catch (final Throwable t) {
      return t;
    }
    throw new AssertionError("no error");
  }
}
