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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The pg_type attributes of the date/time and jsonb types added by #11960 were derived from generic rules
 * that filed time, timetz and timestamptz under the user-defined category, interval too, preferred none of
 * them, gave timetz and interval INT alignment and spelled their I/O functions without the underscore
 * PostgreSQL uses (issue #12313). Comparing every row found two more in the same rules: {@code char} filed as a
 * string type instead of PostgreSQL's internal-use category Z, and {@code oid} not preferred within N.
 * <p>
 * The expected rows below were read from a stock PostgreSQL 16 server with
 * {@code SELECT oid, typname, typlen, typbyval, typcategory, typispreferred, typalign, typstorage, typinput,
 * typoutput, typreceive, typsend FROM pg_type}.
 */
class Issue12313PgTypeTemporalAttributesTest {

  private static final String COLUMNS = "oid, typname, typlen, typbyval, typcategory, typispreferred, typalign, typstorage, "
      + "typinput, typoutput, typreceive, typsend";

  /** PostgreSQL 16, one line per type this protocol produces, columns in {@link #COLUMNS} order. */
  private static final List<String> POSTGRESQL_16 = List.of(//
      "16|bool|1|t|B|t|c|p|boolin|boolout|boolrecv|boolsend",
      "17|bytea|-1|f|U|f|i|x|byteain|byteaout|bytearecv|byteasend",
      "18|char|1|t|Z|f|c|p|charin|charout|charrecv|charsend",
      "19|name|64|f|S|f|c|p|namein|nameout|namerecv|namesend",
      "20|int8|8|t|N|f|d|p|int8in|int8out|int8recv|int8send",
      "21|int2|2|t|N|f|s|p|int2in|int2out|int2recv|int2send",
      "22|int2vector|-1|f|A|f|i|p|int2vectorin|int2vectorout|int2vectorrecv|int2vectorsend",
      "23|int4|4|t|N|f|i|p|int4in|int4out|int4recv|int4send",
      "25|text|-1|f|S|t|i|x|textin|textout|textrecv|textsend",
      "26|oid|4|t|N|t|i|p|oidin|oidout|oidrecv|oidsend",
      "114|json|-1|f|U|f|i|x|json_in|json_out|json_recv|json_send",
      "199|_json|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "700|float4|4|t|N|f|i|p|float4in|float4out|float4recv|float4send",
      "701|float8|8|t|N|t|d|p|float8in|float8out|float8recv|float8send",
      "1000|_bool|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1001|_bytea|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1002|_char|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1005|_int2|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1007|_int4|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1009|_text|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1014|_bpchar|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1015|_varchar|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1016|_int8|-1|f|A|f|d|x|array_in|array_out|array_recv|array_send",
      "1021|_float4|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1022|_float8|-1|f|A|f|d|x|array_in|array_out|array_recv|array_send",
      "1042|bpchar|-1|f|S|f|i|x|bpcharin|bpcharout|bpcharrecv|bpcharsend",
      "1043|varchar|-1|f|S|f|i|x|varcharin|varcharout|varcharrecv|varcharsend",
      "1082|date|4|t|D|f|i|p|date_in|date_out|date_recv|date_send",
      "1083|time|8|t|D|f|d|p|time_in|time_out|time_recv|time_send",
      "1114|timestamp|8|t|D|f|d|p|timestamp_in|timestamp_out|timestamp_recv|timestamp_send",
      "1115|_timestamp|-1|f|A|f|d|x|array_in|array_out|array_recv|array_send",
      "1182|_date|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1184|timestamptz|8|t|D|t|d|p|timestamptz_in|timestamptz_out|timestamptz_recv|timestamptz_send",
      "1186|interval|16|f|T|t|d|p|interval_in|interval_out|interval_recv|interval_send",
      "1231|_numeric|-1|f|A|f|i|x|array_in|array_out|array_recv|array_send",
      "1266|timetz|12|f|D|f|d|p|timetz_in|timetz_out|timetz_recv|timetz_send",
      "1700|numeric|-1|f|N|f|i|m|numeric_in|numeric_out|numeric_recv|numeric_send",
      "3802|jsonb|-1|f|U|f|i|x|jsonb_in|jsonb_out|jsonb_recv|jsonb_send");

  @Test
  void theRowsTheIssueNamesAnswerWhatPostgreSQL16Answers() {
    // The query the issue gives, answered by the catalog a wire session reaches.
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/Issue12313PgTypeTemporalAttributesTest");
    if (factory.exists())
      factory.open().drop();
    final Database database = factory.create();
    final List<Map<String, Object>> rows;
    try {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(
          "SELECT oid, typname, typlen, typbyval, typcategory, typispreferred, typalign, typstorage, typinput FROM pg_type "
              + "WHERE oid IN (1083, 1114, 1184, 1186, 1266, 3802)", database, "root");
      assertThat(answer).isNotNull();
      rows = answer.rows;
    } finally {
      database.drop();
    }

    assertThat(rows).extracting(Issue12313PgTypeTemporalAttributesTest::line).containsExactlyInAnyOrder(//
        "1083|time|8|t|D|f|d|p|time_in",
        "1114|timestamp|8|t|D|f|d|p|timestamp_in",
        "1184|timestamptz|8|t|D|t|d|p|timestamptz_in",
        "1186|interval|16|f|T|t|d|p|interval_in",
        "1266|timetz|12|f|D|f|d|p|timetz_in",
        "3802|jsonb|-1|f|U|f|i|x|jsonb_in");
  }

  @Test
  void theDateTimeCategoryHasOnePreferredTypeAndItIsTimestamptz() {
    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve("SELECT typname, typcategory, typispreferred FROM pg_type");

    assertThat(rows).filteredOn(row -> "D".equals(row.get("typcategory")) && Boolean.TRUE.equals(row.get("typispreferred")))
        .extracting(row -> row.get("typname")).containsExactly("timestamptz");
    assertThat(rows).filteredOn(row -> "T".equals(row.get("typcategory")) && Boolean.TRUE.equals(row.get("typispreferred")))
        .extracting(row -> row.get("typname")).containsExactly("interval");
  }

  @Test
  void everyTypeThisProtocolProducesIsDescribedAsPostgreSQL16DescribesIt() {
    // Reaches the authority: every PostgresType must have its PostgreSQL 16 row here, or this census has a
    // hole a new enum constant could slip through.
    final Map<Integer, String> expected = POSTGRESQL_16.stream()
        .collect(Collectors.toMap(line -> Integer.parseInt(line.substring(0, line.indexOf('|'))), line -> line, (a, b) -> a,
            LinkedHashMap::new));
    final List<Integer> produced = Arrays.stream(PostgresType.values()).map(type -> type.code).toList();
    assertThat(expected.keySet()).as("the PostgreSQL 16 table must hold a row for every PostgresType").containsAll(produced);

    final List<Map<String, Object>> rows = PostgresTypeCatalog.resolve("SELECT " + COLUMNS + " FROM pg_type");
    assertThat(rows).hasSize(PostgresType.values().length);

    final List<String> actual = rows.stream().map(Issue12313PgTypeTemporalAttributesTest::line).toList();
    assertThat(actual).containsExactlyInAnyOrderElementsOf(
        expected.entrySet().stream().filter(e -> produced.contains(e.getKey())).map(Map.Entry::getValue).toList());
  }

  private static String line(final Map<String, Object> row) {
    return row.values().stream().map(value -> value instanceof Boolean b ? (b ? "t" : "f") : String.valueOf(value))
        .collect(Collectors.joining("|"));
  }
}
