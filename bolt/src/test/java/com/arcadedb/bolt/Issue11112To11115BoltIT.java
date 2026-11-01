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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.types.Point;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issues #11112, #11113, #11114 and #11115 as the reporter met them, over Bolt with the Neo4j driver. Each expected value is
 * what Neo4j 5.26 answers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11112To11115BoltIT extends BaseBoltServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  /**
   * #11112: a map literal around a map projection keeps its own keys and its other entries.
   */
  @Test
  void aMapLiteralAroundAMapProjection() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE (a:Qa {k: 1})<-[:QR {y: 1}]-(:Qb {t: 'x'})").consume();

      assertThat(single(session, "MATCH (n:Qa) RETURN { a: n { .k }, b: 1 } AS m").get("m").asMap())
          .isEqualTo(Map.of("a", Map.of("k", 1L), "b", 1L));
      assertThat(single(session,
          "MATCH (n:Qa) RETURN n { c: { edges: [(n)<-[e0:QR]-(n0:Qb) | { node: n0 { .t }, p: e0 { .y } }] } } AS m").get("m")
          .asMap()).isEqualTo(Map.of("c", Map.of("edges", List.of(Map.of("node", Map.of("t", "x"), "p", Map.of("y", 1L))))));
    }
  }

  /**
   * #11113: the crs name is Neo4j's lower-case spelling, and a point still travels as a native Bolt Point.
   */
  @Test
  void theCrsNameIsLowerCase() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final Record row = single(session, """
          RETURN point({longitude: 1, latitude: 2, height: 3}).crs AS geo3d, point({longitude: 1.5, latitude: 2.5}).crs AS geo,
                 point({x: 1, y: 2, z: 3}).crs AS cart3d, point({x: 1, y: 2}).crs AS cart,
                 point({longitude: 1, latitude: 2, height: 3}) AS p""");
      assertThat(row.get("geo3d").asString()).isEqualTo("wgs-84-3d");
      assertThat(row.get("geo").asString()).isEqualTo("wgs-84");
      assertThat(row.get("cart3d").asString()).isEqualTo("cartesian-3d");
      assertThat(row.get("cart").asString()).isEqualTo("cartesian");

      final Point point = row.get("p").asPoint();
      assertThat(point.srid()).isEqualTo(4979);
      assertThat(point.z()).isEqualTo(3.0);

      // a point sent by the driver comes back with the same names
      final Record echo = single(session, "RETURN $p.crs AS crs", Map.of("p", point));
      assertThat(echo.get("crs").asString()).isEqualTo("wgs-84-3d");
    }
  }

  /**
   * #11114: apoc.util.validate takes APOC's three arguments and raises when the predicate is TRUE.
   */
  @Test
  void apocUtilValidate() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      assertThat(single(session, "CALL apoc.util.validate(false, 'bad %s', ['x']) RETURN 1 AS ok").get("ok").asInt()).isEqualTo(1);

      final Throwable error = catchThrowable(() -> session.run("CALL apoc.util.validate(true, 'bad %s', ['x']) RETURN 1 AS ok").list());
      assertThat(error).isInstanceOf(ClientException.class);
      assertThat(error.getMessage()).contains("bad x");
    }
  }

  /**
   * #11115: an unknown namespaced function is the same SyntaxError an unknown plain function is, named as written.
   */
  @Test
  void anUnknownNamespacedFunctionIsASyntaxError() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final ClientException namespaced = unknown(session, "RETURN apoc.coll.sortMulti([{a: 1}], ['a']) AS s");
      assertThat(namespaced.code()).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
      assertThat(namespaced.getMessage()).contains("Unknown function 'apoc.coll.sortMulti'");

      final ClientException plain = unknown(session, "RETURN noSuchFunction(1) AS s");
      assertThat(plain.code()).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
      assertThat(plain.getMessage()).contains("Unknown function 'noSuchFunction'");

      // the session is still usable afterwards
      assertThat(single(session, "RETURN 1 AS one").get("one").asInt()).isEqualTo(1);
    }
  }

  private static ClientException unknown(final Session session, final String query) {
    final Throwable error = catchThrowable(() -> session.run(query).list());
    assertThat(error).as(query).isInstanceOf(ClientException.class);
    return (ClientException) error;
  }

  private static Record single(final Session session, final String query) {
    return single(session, query, Map.of());
  }

  private static Record single(final Session session, final String query, final Map<String, Object> parameters) {
    final List<Record> rows = session.run(query, parameters).list();
    assertThat(rows).as(query).hasSize(1);
    return rows.getFirst();
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
