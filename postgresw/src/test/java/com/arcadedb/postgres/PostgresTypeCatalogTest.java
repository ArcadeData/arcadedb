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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
    final List<Map<String, Object>> rows = PostgresTypeCatalog.enumerate("SELECT oid, typname FROM pg_type");

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
    final List<Map<String, Object>> rows = PostgresTypeCatalog.enumerate("SELECT oid FROM pg_type");

    int previous = Integer.MIN_VALUE;
    for (final Map<String, Object> row : rows) {
      final int oid = (Integer) row.get("oid");
      assertThat(oid).isGreaterThan(previous);
      previous = oid;
    }
  }

  @Test
  void projectionOrderIsTheClientsOrderBecauseADataRowIsReadPositionally() {
    assertThat(PostgresTypeCatalog.enumerate("SELECT typname, oid FROM pg_type").get(0).keySet())
        .containsExactly("typname", "oid");
    assertThat(PostgresTypeCatalog.enumerate("SELECT oid, typname FROM pg_type").get(0).keySet())
        .containsExactly("oid", "typname");
  }

  @Test
  void projectionAliasNamesTheColumn() {
    assertThat(PostgresTypeCatalog.enumerate("SELECT t.oid AS type_oid, t.typname FROM pg_type t").get(0).keySet())
        .containsExactly("type_oid", "typname");
  }

  @Test
  void starProjectsEveryColumnTheCatalogKnows() {
    final Map<String, Object> row = PostgresTypeCatalog.enumerate("SELECT * FROM pg_type").get(0);
    assertThat(row.keySet()).contains("oid", "typname", "typelem", "typarray", "typdelim", "typtype", "typcategory",
        "typlen", "typinput", "typnotnull", "typbasetype", "typnamespace", "typrelid");
  }

  @Test
  void arrayAndScalarRowsDescribeEachOther() {
    final Map<String, Object> byOid = PostgresTypeCatalog.enumerate("SELECT oid, typelem, typarray, typcategory FROM pg_type")
        .stream().filter(row -> row.get("oid").equals(PostgresType.ARRAY_TEXT.code)).findFirst().orElseThrow();
    assertThat(byOid.get("typelem")).isEqualTo(PostgresType.TEXT.code);
    assertThat(byOid.get("typarray")).isEqualTo(0);   // an array type is not itself the element of one
    assertThat(byOid.get("typcategory")).isEqualTo("A");

    final Map<String, Object> scalar = PostgresTypeCatalog.enumerate("SELECT oid, typelem, typarray, typcategory FROM pg_type")
        .stream().filter(row -> row.get("oid").equals(PostgresType.TEXT.code)).findFirst().orElseThrow();
    assertThat(scalar.get("typelem")).isEqualTo(0);
    assertThat(scalar.get("typarray")).isEqualTo(PostgresType.ARRAY_TEXT.code);
    assertThat(scalar.get("typcategory")).isEqualTo("S");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // A filter is a different question; the OID/name lookups answer those.
      "SELECT oid FROM pg_type WHERE typname = 'int4'",
      "SELECT oid FROM pg_type ORDER BY oid",
      // A column this catalog cannot produce is declined whole rather than answered with a hole in it.
      "SELECT oid, typcollation FROM pg_type",
      // Not pg_type at all.
      "SELECT oid FROM pg_class" })
  void shapesThisCatalogCannotAnswerAreDeclined(final String query) {
    assertThat(PostgresTypeCatalog.enumerate(query)).isNull();
  }

  @Test
  void oidLookupDescribesTheElementOfTheArrayTheClientAskedAbout() {
    // The shape pgjdbc sends: SELECT e.typdelim, e.typname FROM pg_type t, pg_type e
    //                         WHERE t.oid = 1007 AND t.typelem = e.oid
    final Map<String, Object> row = PostgresTypeCatalog.lookupByOid(
        "SELECT e.typdelim, e.typname FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid");

    assertThat(row).containsEntry("typdelim", ",").containsEntry("typname", "int4");
  }

  @Test
  void oidLookupReportsTheElementOid() {
    assertThat(PostgresTypeCatalog.lookupByOid("SELECT typelem FROM pg_type WHERE oid = 1009"))
        .containsEntry("typelem", PostgresType.TEXT.code);
  }

  @Test
  void aScalarOidHasNoElementToReport() {
    // It used to be answered with text/25, so a client asking about int4 was told its element was text.
    assertThat(PostgresTypeCatalog.lookupByOid("SELECT typelem FROM pg_type WHERE oid = 23")).isNull();
  }

  @Test
  void anUnknownOidStillFallsBackToTextSoADriverGetsAnAnswerItCanUse() {
    assertThat(PostgresTypeCatalog.lookupByOid("SELECT typelem, typname FROM pg_type WHERE oid = 987654"))
        .containsEntry("typelem", PostgresType.TEXT.code).containsEntry("typname", "text");
  }

  @Test
  void nameLookupAnswersEveryTypeThisProtocolProduces() {
    for (final PostgresType type : PostgresType.values()) {
      final Map<String, Object> row = PostgresTypeCatalog.lookupByName("SELECT oid FROM pg_type WHERE typname = '"
          + type.typeName + "'");
      assertThat(row).as(type.name()).containsEntry("oid", type.code).containsEntry("typname", type.typeName);
    }
  }

  @Test
  void nameLookupOfATypeThisProtocolCannotProduceMeansNoRowRatherThanNoAnswer() {
    assertThat(PostgresTypeCatalog.lookupByName("SELECT oid FROM pg_type WHERE typname = 'hstore'")).isEmpty();
  }

  @Test
  void aQueryWithNoFilterIsNotALookup() {
    assertThat(PostgresTypeCatalog.lookupByOid("SELECT oid, typname FROM pg_type")).isNull();
    assertThat(PostgresTypeCatalog.lookupByName("SELECT oid, typname FROM pg_type")).isNull();
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
