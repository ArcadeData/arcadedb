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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * The pg_type answers a PostgreSQL client uses to find out what the OIDs it was handed in RowDescription
 * mean (issue #5290).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresTypeCatalogTest {

  @Test
  void enumerationReturnsOneRowPerTypeThisProtocolCanProduce() {
    // This query used to be on an ignore-list and answered with zero rows, so a client that builds its
    // whole OID-to-name map up front built an empty one.
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve("SELECT oid, typname FROM pg_type");

    assertThat(rows).hasSize(PostgresType.values().length);
    assertThat(rows).allSatisfy(row -> assertThat(row.keySet()).containsExactly("oid", "typname"));
    assertThat(rows).anySatisfy(row -> {
      assertThat(row.get("oid")).isEqualTo(PostgresType.TEXT.code);
      assertThat(row.get("typname")).isEqualTo("text");
    });
    assertThat(rows).anySatisfy(row -> {
      assertThat(row.get("oid")).isEqualTo(PostgresType.ARRAY_TEXT.code);
      assertThat(row.get("typname")).isEqualTo("_text");
    });
  }

  @Test
  void enumerationIsOrderedByOidSoACachedAnswerNeverReshuffles() {
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve("SELECT oid FROM pg_type");

    int previous = Integer.MIN_VALUE;
    for (final Map<String, Object> row : rows) {
      final int oid = (Integer) row.get("oid");
      assertThat(oid).isGreaterThan(previous);
      previous = oid;
    }
  }

  @Test
  void projectionOrderIsTheClientsOrderBecauseADataRowIsReadPositionally() {
    assertThat(PostgresTypeCatalog.resolve("SELECT typname, oid FROM pg_type").get(0).keySet())
        .containsExactly("typname", "oid");
    assertThat(PostgresTypeCatalog.resolve("SELECT oid, typname FROM pg_type").get(0).keySet())
        .containsExactly("oid", "typname");
  }

  @Test
  void projectionAliasNamesTheColumn() {
    assertThat(PostgresTypeCatalog.resolve("SELECT t.oid AS type_oid, t.typname FROM pg_type t").get(0).keySet())
        .containsExactly("type_oid", "typname");
  }

  @Test
  void starProjectsEveryColumnTheCatalogKnows() {
    final Map<String, Object> row = PostgresTypeCatalog.resolve("SELECT * FROM pg_type").get(0);
    assertThat(row.keySet()).contains("oid", "typname", "typelem", "typarray", "typdelim", "typtype", "typcategory",
        "typlen", "typinput", "typnotnull", "typbasetype", "typnamespace", "typrelid");
  }

  @Test
  void arrayAndScalarRowsDescribeEachOther() {
    final Map<String, Object> byOid = PostgresTypeCatalog.resolve("SELECT oid, typelem, typarray, typcategory FROM pg_type")
        .stream().filter(row -> row.get("oid").equals(PostgresType.ARRAY_TEXT.code)).findFirst().orElseThrow();
    assertThat(byOid.get("typelem")).isEqualTo(PostgresType.TEXT.code);
    assertThat(byOid.get("typarray")).isEqualTo(0);   // an array type is not itself the element of one
    assertThat(byOid.get("typcategory")).isEqualTo("A");

    final Map<String, Object> scalar = PostgresTypeCatalog.resolve("SELECT oid, typelem, typarray, typcategory FROM pg_type")
        .stream().filter(row -> row.get("oid").equals(PostgresType.TEXT.code)).findFirst().orElseThrow();
    assertThat(scalar.get("typelem")).isEqualTo(0);
    assertThat(scalar.get("typarray")).isEqualTo(PostgresType.ARRAY_TEXT.code);
    assertThat(scalar.get("typcategory")).isEqualTo("S");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // A filter this catalog does not understand is not guessed at.
      "SELECT oid FROM pg_type WHERE typtype = 'b' AND typlen > 4",
      "SELECT oid FROM pg_type ORDER BY oid",
      // A column this catalog cannot produce is declined whole rather than answered with a hole in it.
      "SELECT oid, typcollation FROM pg_type",
      // Not pg_type at all.
      "SELECT oid FROM pg_class" })
  void shapesThisCatalogCannotAnswerAreDeclined(final String query) {
    assertThat(PostgresTypeCatalog.resolve(query)).isNull();
  }

  @Test
  void aFilterOnOidSelectsThatTypesOwnRow() {
    // Every column the client projected comes back, the OID it filtered on included. The fixed handful of
    // columns this used to answer with never carried "oid", so a client that asked for it got a row missing
    // the very column it selected by.
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
        "SELECT oid, typname, typelem FROM pg_type WHERE oid = 1007");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly(entry("oid", 1007), entry("typname", "_int4"),
        entry("typelem", PostgresType.INTEGER.code));
  }

  @Test
  void aFilterOnTypeNameSelectsThatTypesOwnRow() {
    for (final PostgresType type : PostgresType.values()) {
      final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
          "SELECT oid, typname FROM pg_type WHERE typname = '" + type.typeName + "'");
      assertThat(rows).as(type.name()).hasSize(1);
      assertThat(rows.get(0)).as(type.name()).containsEntry("oid", type.code).containsEntry("typname", type.typeName);
    }
  }

  @Test
  void aFilterNamingATypeThisProtocolCannotProduceMeansNoRowRatherThanNoAnswer() {
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type WHERE typname = 'hstore'")).isEmpty();
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type WHERE oid = 987654")).isEmpty();
  }

  @Test
  void theDriverSelfJoinDescribesTheElementOfTheArray() {
    // The shape pgjdbc sends. Here - and only here - the projected columns describe a different row from
    // the one the filter selects, which is what the "t.typelem = e.oid" correlation says.
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
        "SELECT e.typdelim, e.typname FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly(entry("typdelim", ","), entry("typname", "int4"));
  }

  @Test
  void theDriverSelfJoinAnswersOnlyWhatItProjected() {
    // Its WHERE clause names both oid and typelem; reading the columns off the whole query rather than off
    // the projection would answer with fields nobody asked for.
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
        "SELECT e.typdelim FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = 1009 AND t.typelem = e.oid");

    assertThat(rows.get(0)).containsExactly(entry("typdelim", ","));
  }

  @Test
  void theDriverSelfJoinOnAnUnknownOidStillFallsBackToTextSoADriverGetsAnAnswerItCanUse() {
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve(
        "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 987654 AND t.typelem = e.oid");

    assertThat(rows.get(0)).containsEntry("typname", "text");
  }

  @Test
  void theDriverSelfJoinOnAScalarSelectsNothingBecauseAScalarHasNoElement() {
    assertThat(PostgresTypeCatalog.resolve(
        "SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 23 AND t.typelem = e.oid")).isEmpty();
  }

  @Test
  void inputFunctionsAreSpelledTheWayPostgresSpellsThem() {
    // Synthesising <name> + "in" is right for most types but not for the temporal ones, json or numeric, which
    // PostgreSQL spells with an underscore.
    final Map<Integer, Object> byOid = new HashMap<>();
    for (final Map<String, Object> row : PostgresTypeCatalog.resolve("SELECT oid, typinput FROM pg_type"))
      byOid.put((Integer) row.get("oid"), row.get("typinput"));

    assertThat(byOid).containsEntry(PostgresType.INTEGER.code, "int4in");
    assertThat(byOid).containsEntry(PostgresType.BOOLEAN.code, "boolin");
    assertThat(byOid).containsEntry(PostgresType.DATE.code, "date_in");
    assertThat(byOid).containsEntry(PostgresType.TIMESTAMP.code, "timestamp_in");
    assertThat(byOid).containsEntry(PostgresType.JSON.code, "json_in");
    assertThat(byOid).containsEntry(PostgresType.NUMERIC.code, "numeric_in");
    assertThat(byOid).containsEntry(PostgresType.ARRAY_TEXT.code, "array_in");
  }

  @Test
  void numericIsCategorisedAsNumberLikeEveryOtherNumericType() {
    // issue #6447: NUMERIC used to fall through category()'s default "U" (user-defined) arm, the same bucket
    // real PostgreSQL uses for json - wrong for a type PostgreSQL itself files under "N" alongside int4/float8.
    final Map<String, Object> row = PostgresTypeCatalog.resolve("SELECT oid, typcategory FROM pg_type")
        .stream().filter(r -> r.get("oid").equals(PostgresType.NUMERIC.code)).findFirst().orElseThrow();
    assertThat(row.get("typcategory")).isEqualTo("N");
  }

  @Test
  void anExplicitlyEmptyQuotedAliasIsAColumnNamedEmpty() {
    assertThat(PostgresTypeCatalog.resolve("SELECT oid AS \"\" FROM pg_type").get(0).keySet()).containsExactly("");
  }

  @Test
  void everyTypeCarriesTheNamePostgresGivesItsOid() {
    // The names are what a client resolves the announced OID against, so they have to be PostgreSQL's own.
    assertThat(PostgresType.ARRAY_TEXT.typeName).isEqualTo("_text");
    assertThat(PostgresType.ARRAY_INT.typeName).isEqualTo("_int4");
    assertThat(PostgresType.JSON.typeName).isEqualTo("json");
    // 1002 is "char"[]; 1003, which this used to claim, is name[] - a type ArcadeDB never produces.
    assertThat(PostgresType.ARRAY_CHAR.code).isEqualTo(1002);
    assertThat(PostgresType.ARRAY_CHAR.typeName).isEqualTo("_char");
    assertThat(PostgresType.ARRAY_CHAR.elementCode).isEqualTo(PostgresType.CHAR.code);
  }
}
