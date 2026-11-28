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
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Point;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13570: a WGS-84 point sent by the driver as a Bolt Point parameter is the same value {@code point()} builds, so it
 * is equal to a stored or constructed WGS-84 point. Each expected value is what Neo4j 5.26 answers.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public class Issue13570BoltPointParameterEqualityIT extends BaseBoltServerTest {
  private static final Value GEO_2D = Values.point(4326, 8.5, 47.5);
  private static final Value GEO_3D = Values.point(4979, 8.5, 47.5, 27.0);
  private static final Value CAR_2D = Values.point(7203, 10, 20);
  private static final Value CAR_3D = Values.point(9157, 10, 20, 30);

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

  @Test
  void aPointParameterMatchesTheStoredPoint() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("""
          CREATE (:T13570 {geo2d: point({latitude: 47.5, longitude: 8.5}),
                           geo3d: point({latitude: 47.5, longitude: 8.5, height: 27.0}),
                           car2d: point({x: 10, y: 20}),
                           car3d: point({x: 10, y: 20, z: 30})})""").consume();

      assertThat(count(session, "MATCH (n:T13570) WHERE n.geo2d = $p RETURN count(n) AS c", GEO_2D)).isEqualTo(1L);
      assertThat(count(session, "MATCH (n:T13570) WHERE n.geo3d = $p RETURN count(n) AS c", GEO_3D)).isEqualTo(1L);
      assertThat(count(session, "MATCH (n:T13570) WHERE n.car2d = $p RETURN count(n) AS c", CAR_2D)).isEqualTo(1L);
      assertThat(count(session, "MATCH (n:T13570) WHERE n.car3d = $p RETURN count(n) AS c", CAR_3D)).isEqualTo(1L);

      // the other places a point is compared as a value
      assertThat(count(session, "MATCH (n:T13570) WHERE n.geo2d IN [$p] RETURN count(n) AS c", GEO_2D)).isEqualTo(1L);
      assertThat(count(session, "MATCH (n:T13570) UNWIND [n.geo2d, $p] AS p RETURN count(DISTINCT p) AS c", GEO_2D)).isEqualTo(1L);

      // a different point of the same crs still does not match
      assertThat(count(session, "MATCH (n:T13570) WHERE n.geo2d = $p RETURN count(n) AS c", Values.point(4326, 8.5, 47.6)))
          .isEqualTo(0L);
      assertThat(count(session, "MATCH (n:T13570) WHERE n.geo2d = $p RETURN count(n) AS c", Values.point(7203, 8.5, 47.5)))
          .isEqualTo(0L);
    }
  }

  @Test
  void aPointParameterEqualsTheConstructedPoint() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      assertThat(single(session, "RETURN point({latitude: 47.5, longitude: 8.5}) = $p AS eq", Map.of("p", GEO_2D)).get("eq")
          .asBoolean()).isTrue();
      assertThat(single(session, "RETURN point({latitude: 47.5, longitude: 8.5, height: 27.0}) = $p AS eq", Map.of("p", GEO_3D))
          .get("eq").asBoolean()).isTrue();
      assertThat(single(session, "RETURN point({x: 8.5, y: 47.5, srid: 4326}) = $p AS eq", Map.of("p", GEO_2D)).get("eq")
          .asBoolean()).isTrue();
      // a longitude past the antimeridian is the wrapped one, as point() stores it
      assertThat(single(session, "RETURN point({latitude: 47.5, longitude: 181}) = $p AS eq",
          Map.of("p", Values.point(4326, 181, 47.5))).get("eq").asBoolean()).isTrue();
    }
  }

  @Test
  void aStoredPointParameterMatchesAConstructedPoint() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE (:P13570 {geo: $p})", Map.of("p", GEO_2D)).consume();

      assertThat(single(session, "MATCH (n:P13570) WHERE n.geo = point({latitude: 47.5, longitude: 8.5}) RETURN count(n) AS c")
          .get("c").asLong()).isEqualTo(1L);
      assertThat(single(session, "MATCH (n:P13570) RETURN n.geo.latitude AS lat, n.geo.longitude AS lon").asMap())
          .isEqualTo(Map.of("lat", 47.5, "lon", 8.5));
    }
  }

  @Test
  void aPointParameterStillComesBackAsANativePoint() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      for (final Value sent : List.of(GEO_2D, GEO_3D, CAR_2D, CAR_3D)) {
        final Point echo = single(session, "RETURN $p AS p", Map.of("p", sent)).get("p").asPoint();
        assertThat(echo).isEqualTo(sent.asPoint());
      }
    }
  }

  private static long count(final Session session, final String query, final Value point) {
    return single(session, query, Map.of("p", point)).get("c").asLong();
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
