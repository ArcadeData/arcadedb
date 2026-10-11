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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9648: the index compared a ZONED_DATETIME (and an OFFSET_TIME) by its instant while the row filter compared it by
 * {@code equals()}, which also requires the zone, so creating or dropping an index changed what a query returned. Both now
 * follow openCypher, as {@code CypherDateTime} already did (#8300): the same instant in two zones is two values, ordered by
 * instant, then offset, then zone name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9648ZonedTemporalIndexEqualityTest extends TestHelper {

  private static final ZonedDateTime STORED  = ZonedDateTime.parse("2026-01-01T00:30+02:00");
  private static final ZonedDateTime SAME_UTC = ZonedDateTime.parse("2025-12-31T22:30Z");

  private void load(final String type, final Type propertyType, final String algorithm, final Object... values) {
    database.getSchema().createDocumentType(type).createProperty("z", propertyType);
    if (algorithm != null)
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.valueOf(algorithm), false, type, "z");
    database.transaction(() -> {
      for (final Object value : values)
        database.newDocument(type).set("z", value).save();
    });
  }

  private long count(final String type, final Object value) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE z = ?", value)) {
      return rs.next().<Long>getProperty("c");
    }
  }

  private List<Object> sorted(final String type, final String where, final Object value) {
    final List<Object> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT z FROM " + type + " WHERE " + where + " ORDER BY z", value)) {
      rs.stream().forEach(r -> result.add(r.getProperty("z")));
    }
    return result;
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
      LSM_TREE
      HASH
      """)
  void zonedDateTimeEqualityIsTheSameWithAndWithoutIndex(final String algorithm) {
    assertThat(BinaryComparator.equals(STORED, SAME_UTC)).isFalse();

    load("Indexed", Type.ZONED_DATETIME, algorithm, STORED);
    load("Plain", Type.ZONED_DATETIME, null, STORED);

    for (final Object probe : new Object[] { STORED, SAME_UTC, STORED.toOffsetDateTime() }) {
      final long expected = count("Plain", probe);
      assertThat(count("Indexed", probe)).as("probe " + probe).isEqualTo(expected);
    }
    assertThat(count("Plain", STORED)).isEqualTo(1L);
    assertThat(count("Plain", SAME_UTC)).isZero();
    // the OffsetDateTime of the same zone is the stored value, to the filter as to the index
    assertThat(count("Plain", STORED.toOffsetDateTime())).isEqualTo(1L);

    assertThat(database.getSchema().getIndexByName("Indexed[z]").get(new Object[] { SAME_UTC }).hasNext()).isFalse();
    assertThat(database.getSchema().getIndexByName("Indexed[z]").get(new Object[] { STORED }).hasNext()).isTrue();
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
      LSM_TREE
      HASH
      """)
  void offsetTimeEqualityIsTheSameWithAndWithoutIndex(final String algorithm) {
    // 01:30+01:00 and 00:30Z are the same time of day in two offsets
    final OffsetTime stored = OffsetTime.of(1, 30, 0, 0, ZoneOffset.ofHours(1));
    final OffsetTime sameUtc = OffsetTime.of(0, 30, 0, 0, ZoneOffset.UTC);
    load("Indexed", Type.OFFSET_TIME, algorithm, stored);
    load("Plain", Type.OFFSET_TIME, null, stored);

    for (final Object probe : new Object[] { stored, sameUtc })
      assertThat(count("Indexed", probe)).as("probe " + probe).isEqualTo(count("Plain", probe));
    assertThat(count("Indexed", stored)).isEqualTo(1L);
    assertThat(count("Indexed", sameUtc)).isZero();
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
      LSM_TREE
      HASH
      """)
  void sameInstantInTwoZonesIsTwoKeys(final String algorithm) {
    load("Indexed", Type.ZONED_DATETIME, algorithm, STORED, SAME_UTC, ZonedDateTime.parse("2025-12-31T23:30+01:00[Europe/Paris]"));
    load("Plain", Type.ZONED_DATETIME, null, STORED, SAME_UTC, ZonedDateTime.parse("2025-12-31T23:30+01:00[Europe/Paris]"));

    assertThat(count("Indexed", STORED)).isEqualTo(1L);
    assertThat(count("Indexed", SAME_UTC)).isEqualTo(1L);
    assertThat(count("Indexed", ZonedDateTime.parse("2025-12-31T23:30+01:00[Europe/Paris]"))).isEqualTo(1L);
    // one instant, three zones: none is the fixed +01:00 offset without the region
    assertThat(count("Indexed", ZonedDateTime.parse("2025-12-31T23:30+01:00"))).isZero();
  }

  @Test
  void rangeOrderIsInstantThenOffsetThenZone() {
    final ZonedDateTime west = ZonedDateTime.parse("2025-12-31T17:30-05:00");
    final ZonedDateTime later = ZonedDateTime.parse("2026-01-01T10:00Z");
    load("Indexed", Type.ZONED_DATETIME, "LSM_TREE", later, STORED, SAME_UTC, west);
    load("Plain", Type.ZONED_DATETIME, null, later, STORED, SAME_UTC, west);

    // the same instant, west to east, then the later instant
    final List<Object> expected = List.of(west, SAME_UTC, STORED, later);
    assertThat(sorted("Plain", "z >= ?", west)).isEqualTo(expected);
    assertThat(sorted("Indexed", "z >= ?", west)).isEqualTo(expected);
    // a bound in one zone of the shared instant keeps the zones east of it and drops those west of it, with or without index
    assertThat(sorted("Indexed", "z > ?", SAME_UTC)).isEqualTo(sorted("Plain", "z > ?", SAME_UTC)).isEqualTo(List.of(STORED, later));
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
      LSM_TREE
      HASH
      """)
  void inTransactionAndPersistedKeysAgree(final String algorithm) {
    // the pending entries of a transaction are keyed by the untyped compareTo, the persisted ones by the typed comparator:
    // both must tell the zones of one instant apart
    database.getSchema().createDocumentType("Tx").createProperty("z", Type.ZONED_DATETIME);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.valueOf(algorithm), false, "Tx", "z");
    final ZonedDateTime west = ZonedDateTime.parse("2025-12-31T17:30-05:00");

    database.begin();
    for (final ZonedDateTime value : List.of(STORED, SAME_UTC, west))
      database.newDocument("Tx").set("z", value).save();
    final long[] pending = { count("Tx", STORED), count("Tx", SAME_UTC), count("Tx", west) };
    database.commit();

    assertThat(pending).containsExactly(1L, 1L, 1L);
    assertThat(new long[] { count("Tx", STORED), count("Tx", SAME_UTC), count("Tx", west) }).containsExactly(1L, 1L, 1L);
    if ("LSM_TREE".equals(algorithm))
      assertThat(sorted("Tx", "z >= ?", west)).isEqualTo(List.of(west, SAME_UTC, STORED));
  }

  @Test
  void offsetDateTimeComparesAsItsZonedForm() {
    final OffsetDateTime offset = STORED.toOffsetDateTime();
    assertThat(BinaryComparator.equals(offset, STORED)).isTrue();
    assertThat(BinaryComparator.equals(STORED, offset)).isTrue();
    assertThat(BinaryComparator.equals(offset, SAME_UTC)).isFalse();
  }
}
