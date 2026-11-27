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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code apoc.convert.toJson}, {@code apoc.util.*} hash and compression functions and {@code apoc.coll.flatten} answer
 * what APOC 5.26 answers for the same arguments.
 */
class CypherApocConvertUtilFunctionsTest {

  /**
   * Issue #12108: toJson always answers JSON (quoted strings and temporals, APOC's node / relationship / path layout),
   * the hashes hash the concatenated string forms of the list elements, compress answers a byte array and takes APOC's
   * config map, and flatten(list, true) flattens every level. Every expected value is the answer of Neo4j 5.26 with APOC
   * 5.26.33, the element ids being the RIDs where Neo4j has its numeric ids.
   */
  @Nested
  class Issue12108ApocConvertUtilFlatten extends TestHelper {

    @Test
    void toJsonQuotesStringsAndTemporals() {
      assertThat(value("RETURN apoc.convert.toJson('x') AS r")).isEqualTo("\"x\"");
      assertThat(value("RETURN apoc.convert.toJson($s) AS r", Map.of("s", "a\"b\\\n\t\r\b\f\u0001\u001f/é"))).isEqualTo(
          "\"a\\\"b\\\\\\n\\t\\r\\b\\f\\u0001\\u001F/é\"");
      assertThat(value("RETURN apoc.convert.toJson('') AS r")).isEqualTo("\"\"");
      assertThat(value("RETURN apoc.convert.toJson(date('2020-01-02')) AS r")).isEqualTo("\"2020-01-02\"");
      assertThat(value("RETURN apoc.convert.toJson(datetime('2020-01-02T03:04:05Z')) AS r")).isEqualTo(
          "\"2020-01-02T03:04:05Z\"");
      assertThat(value("RETURN apoc.convert.toJson(datetime('2020-01-02T03:04:05.123+02:00')) AS r")).isEqualTo(
          "\"2020-01-02T03:04:05.123+02:00\"");
      assertThat(value("RETURN apoc.convert.toJson(localdatetime('2020-01-02T03:04:05.123')) AS r")).isEqualTo(
          "\"2020-01-02T03:04:05.123\"");
      assertThat(value("RETURN apoc.convert.toJson(time('03:04:05+01:00')) AS r")).isEqualTo("\"03:04:05+01:00\"");
      assertThat(value("RETURN apoc.convert.toJson(localtime('03:04')) AS r")).isEqualTo("\"03:04\"");
      assertThat(value("RETURN apoc.convert.toJson(duration('P1DT2H')) AS r")).isEqualTo("\"P1DT2H\"");
      assertThat(value("RETURN apoc.convert.toJson({a: date('2020-01-02')}) AS r")).isEqualTo("{\"a\":\"2020-01-02\"}");
      assertThat(value("RETURN apoc.convert.toJson([date('2020-01-02')]) AS r")).isEqualTo("[\"2020-01-02\"]");
    }

    @Test
    void toJsonWritesNumbersBooleansListsAndMaps() {
      assertThat(value("RETURN apoc.convert.toJson(1) AS r")).isEqualTo("1");
      assertThat(value("RETURN apoc.convert.toJson(1.5) AS r")).isEqualTo("1.5");
      assertThat(value("RETURN apoc.convert.toJson(1e20) AS r")).isEqualTo("1.0E20");
      assertThat(value("RETURN apoc.convert.toJson($f) AS r", Map.of("f", Double.NaN))).isEqualTo("\"NaN\"");
      assertThat(value("RETURN apoc.convert.toJson(true) AS r")).isEqualTo("true");
      assertThat(value("RETURN apoc.convert.toJson(null) AS r")).isEqualTo("null");
      assertThat(value("RETURN apoc.convert.toJson([1, 'a', null, {b: 2}]) AS r")).isEqualTo("[1,\"a\",null,{\"b\":2}]");
      assertThat(value("RETURN apoc.convert.toJson({a: 1, b: [1, 2], c: null, d: 'x'}) AS r")).isEqualTo(
          "{\"a\":1,\"b\":[1,2],\"c\":null,\"d\":\"x\"}");
      assertThat(value("RETURN apoc.convert.toJson({}) AS r")).isEqualTo("{}");
      assertThat(value("RETURN apoc.convert.toJson([]) AS r")).isEqualTo("[]");
      assertThat(value("RETURN apoc.convert.toJson(apoc.util.compress('ab', {compression: 'NONE'})) AS r")).isEqualTo("[97,98]");
    }

    @Test
    void toJsonWritesNodesRelationshipsAndPathsInApocLayout() {
      database.command("opencypher", "CREATE (a:P {uid: 1, name: 'a'})-[:LIKES {w: 5.0}]->(b:P {uid: 2, name: 'b'})");
      final Result row = single("MATCH (a:P {uid: 1})-[r:LIKES]->(b:P {uid: 2}) RETURN a, r, b");
      final String a = ((Identifiable) row.getProperty("a")).getIdentity().toString();
      final String r = ((Identifiable) row.getProperty("r")).getIdentity().toString();
      final String b = ((Identifiable) row.getProperty("b")).getIdentity().toString();

      final String nodeA = "{\"id\":\"" + a + "\",\"type\":\"node\",\"labels\":[\"P\"],\"properties\":{\"uid\":1,\"name\":\"a\"}}";
      final String nodeB = "{\"id\":\"" + b + "\",\"type\":\"node\",\"labels\":[\"P\"],\"properties\":{\"uid\":2,\"name\":\"b\"}}";
      assertThat(value("MATCH (a:P {uid: 1}) RETURN apoc.convert.toJson(a) AS r")).isEqualTo(nodeA);
      assertThat(value("MATCH (a:P {uid: 1}) RETURN apoc.convert.toJson({n: a}) AS r")).isEqualTo("{\"n\":" + nodeA + "}");
      assertThat(value("MATCH (a:P {uid: 1}) RETURN apoc.convert.toJson([a]) AS r")).isEqualTo("[" + nodeA + "]");

      assertThat(value("MATCH ()-[r:LIKES]->() RETURN apoc.convert.toJson(r) AS r")).isEqualTo(
          "{\"id\":\"" + r + "\",\"type\":\"relationship\",\"label\":\"LIKES\",\"start\":" + nodeA + ",\"end\":" + nodeB
              + ",\"properties\":{\"w\":5.0}}");

      // inside a path APOC writes the node and relationship keys in another order
      final String pathA = "{\"id\":\"" + a + "\",\"type\":\"node\",\"properties\":{\"uid\":1,\"name\":\"a\"},\"labels\":[\"P\"]}";
      final String pathB = "{\"id\":\"" + b + "\",\"type\":\"node\",\"properties\":{\"uid\":2,\"name\":\"b\"},\"labels\":[\"P\"]}";
      assertThat(value("MATCH p = (:P {uid: 1})-[:LIKES]->(:P) RETURN apoc.convert.toJson(p) AS r")).isEqualTo(
          "[" + pathA + ",{\"start\":" + pathA + ",\"end\":" + pathB + ",\"id\":\"" + r
              + "\",\"label\":\"LIKES\",\"type\":\"relationship\",\"properties\":{\"w\":5.0}}," + pathB + "]");
      assertThat(value("MATCH p = (:P {uid: 1}) RETURN apoc.convert.toJson(p) AS r")).isEqualTo("[" + pathA + "]");
    }

    @Test
    void toJsonLeavesOutEmptyLabelsAndProperties() {
      database.command("opencypher", "CREATE (n)");
      final Result row = single("MATCH (n) RETURN n, apoc.convert.toJson(n) AS r");
      assertThat(row.<String>getProperty("r")).isEqualTo(
          "{\"id\":\"" + ((Identifiable) row.getProperty("n")).getIdentity() + "\",\"type\":\"node\"}");
    }

    @Test
    void hashesHashTheConcatenatedValues() {
      assertThat(value("RETURN apoc.util.md5(['a']) AS r")).isEqualTo("0cc175b9c0f1b6a831c399e269772661");
      assertThat(value("RETURN apoc.util.md5(['a', 'b']) AS r")).isEqualTo("187ef4436122d1cc2f40dc2b92f0eba0");
      assertThat(value("RETURN apoc.util.md5(['a', null, 'b']) AS r")).isEqualTo("187ef4436122d1cc2f40dc2b92f0eba0");
      assertThat(value("RETURN apoc.util.md5([]) AS r")).isEqualTo("d41d8cd98f00b204e9800998ecf8427e");
      assertThat(value("RETURN apoc.util.md5([null]) AS r")).isEqualTo("d41d8cd98f00b204e9800998ecf8427e");
      assertThat(value("RETURN apoc.util.md5([1, 2, 3]) AS r")).isEqualTo("202cb962ac59075b964b07152d234b70");
      assertThat(value("RETURN apoc.util.md5([1.5]) AS r")).isEqualTo("6008647277c4454cecd97d33c069f0ca");
      assertThat(value("RETURN apoc.util.md5([2, 2.0, '2']) AS r")).isEqualTo("c9f8393fccb39e4cbecec96c3bf4c1e8");
      assertThat(value("RETURN apoc.util.md5([1, 'a', true, null]) AS r")).isEqualTo("52f837ab73ded7ca978e43ab79de6424");
      assertThat(value("RETURN apoc.util.md5([{a: 1}]) AS r")).isEqualTo("055524e8a3618ce3fa9b3bd7d233be87");
      assertThat(value("RETURN apoc.util.md5([[1, 2], [3, [4, 5]]]) AS r")).isEqualTo("4fcac029e2e0f2c9099de72151b99ad4");
      assertThat(value("RETURN apoc.util.md5([date('2020-01-02'), date('2019-05-06')]) AS r")).isEqualTo(
          "5b2054e5d58fbf9a3dcff490f22a7ad9");
      assertThat(value("RETURN apoc.util.sha1(['a']) AS r")).isEqualTo("86f7e437faa5a7fce15d1ddcb9eaeaea377667b8");
      assertThat(value("RETURN apoc.util.sha256(['a', 1]) AS r")).isEqualTo(
          "f55ff16f66f43360266b95db6f8fec01d76031054306ae4a4b380598f6cfd114");
      assertThat(value("RETURN apoc.util.sha512([]) AS r")).isEqualTo(
          "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");
    }

    @Test
    void compressAnswersBytesAndReadsTheConfigMap() {
      assertThat(hex(value("RETURN apoc.util.compress('hello') AS r"))).isEqualTo(
          "1f8b08000000000000ffcb48cdc9c9070086a6103605000000");
      assertThat(hex(value("RETURN apoc.util.compress('hello', {}) AS r"))).isEqualTo(
          "1f8b08000000000000ffcb48cdc9c9070086a6103605000000");
      assertThat(hex(value("RETURN apoc.util.compress('hello', {compression: 'GZIP'}) AS r"))).isEqualTo(
          "1f8b08000000000000ffcb48cdc9c9070086a6103605000000");
      assertThat(hex(value("RETURN apoc.util.compress('hello', {compression: 'DEFLATE'}) AS r"))).isEqualTo(
          "789ccb48cdc9c90700062c0215");
      assertThat(hex(value("RETURN apoc.util.compress('hello', {compression: 'NONE'}) AS r"))).isEqualTo("68656c6c6f");
      assertThat(hex(value("RETURN apoc.util.compress('héllo', {compression: 'NONE', charset: 'ISO-8859-1'}) AS r"))).isEqualTo(
          "68e96c6c6f");
      assertThat(hex(value("RETURN apoc.util.compress('', {compression: 'NONE'}) AS r"))).isEmpty();
    }

    @Test
    void decompressReadsWhatCompressWrote() {
      assertThat(value("RETURN apoc.util.decompress(apoc.util.compress('hello')) AS r")).isEqualTo("hello");
      assertThat(value("RETURN apoc.util.decompress(apoc.util.compress('hello'), {}) AS r")).isEqualTo("hello");
      assertThat(value(
          "RETURN apoc.util.decompress(apoc.util.compress('hello', {compression: 'DEFLATE'}), {compression: 'DEFLATE'}) AS r"))
          .isEqualTo("hello");
      assertThat(
          value("RETURN apoc.util.decompress(apoc.util.compress('hello', {compression: 'NONE'}), {compression: 'NONE'}) AS r"))
          .isEqualTo("hello");
      assertThat(value("RETURN apoc.util.decompress(apoc.util.compress('héllo', {charset: 'UTF-16'}), {charset: 'UTF-16'}) AS r"))
          .isEqualTo("héllo");
    }

    @Test
    void compressionCodecsTheEngineDoesNotShipAreRefused() {
      assertThatThrownBy(() -> value("RETURN apoc.util.compress('hello', {compression: 'BZIP2'}) AS r"))
          .hasMessageContaining("BZIP2").hasMessageContaining("not available");
    }

    @Test
    void flattenTrueFlattensEveryLevel() {
      assertThat(value("RETURN apoc.coll.flatten([1, [2, [3, [4]]]], true) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
      assertThat(value("RETURN apoc.coll.flatten([1, [2, [3, [4]]], []], true) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
      assertThat(value("RETURN apoc.coll.flatten([1, [2, [3, [4]]]], false) AS r")).isEqualTo(
          List.of(1L, 2L, List.of(3L, List.of(4L))));
      assertThat(value("RETURN apoc.coll.flatten([1, [2, [3, [4]]]]) AS r")).isEqualTo(List.of(1L, 2L, List.of(3L, List.of(4L))));
      assertThat(value("RETURN apoc.coll.flatten([[1, [2]], [3]], false) AS r")).isEqualTo(List.of(1L, List.of(2L), 3L));
    }

    @Test
    void flattenOfNullIsTheEmptyList() {
      assertThat(value("RETURN apoc.coll.flatten(null) AS r")).isEqualTo(List.of());
      assertThat(value("RETURN apoc.coll.flatten(null, true) AS r")).isEqualTo(List.of());
    }

    private Object value(final String query) {
      return value(query, Map.of());
    }

    private Object value(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        return rs.next().getProperty("r");
      }
    }

    private Result single(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return rs.next();
      }
    }

    private static String hex(final Object bytes) {
      return HexFormat.of().formatHex((byte[]) bytes);
    }
  }
}
