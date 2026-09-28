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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8436: an array bound to {@code IN} and to {@code CONTAINSANY} must answer alike whether it is passed as a
 * positional or as a named parameter, and a scalar property on the left of {@code CONTAINSANY}/{@code CONTAINSALL}
 * must behave as the one-element collection it stands for rather than being silently false for every record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class InContainsAnyArrayParameterTest extends TestHelper {
  private static final List<String> IDS = List.of("u1", "u2");

  @BeforeEach
  void setUpData() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET uuid = 'u1'");
      database.command("sql", "INSERT INTO T SET uuid = 'u2'");
      database.command("sql", "INSERT INTO T SET uuid = 'u3'");
    });
  }

  @Test
  void inAcceptsPositionalAndNamedArray() {
    assertThat(uuids("SELECT uuid FROM T WHERE uuid IN ?", IDS)).containsExactly("u1", "u2");
    assertThat(uuids("SELECT uuid FROM T WHERE uuid IN (?)", IDS)).containsExactly("u1", "u2");
    assertThat(uuids("SELECT uuid FROM T WHERE uuid IN ?", (Object) IDS.toArray())).containsExactly("u1", "u2");
    assertThat(uuids("SELECT uuid FROM T WHERE uuid IN :ids", Map.of("ids", IDS))).containsExactly("u1", "u2");
  }

  @Test
  void containsAnyAcceptsPositionalAndNamedArrayOnScalarProperty() {
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSANY ?", IDS)).containsExactly("u1", "u2");
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSANY :ids", Map.of("ids", IDS))).containsExactly("u1", "u2");
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSANY ['u3', 'x']")).containsExactly("u3");
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSANY 'u2'")).containsExactly("u2");
  }

  @Test
  void containsAllOnScalarProperty() {
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSALL ['u1']")).containsExactly("u1");
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSALL ?", IDS)).isEmpty();
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSALL 'u3'")).containsExactly("u3");
  }

  @Test
  void containsAnyStillMatchesCollectionsAndMissingValuesStayFalse() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET uuid = ['u7', 'u8']");
      database.command("sql", "INSERT INTO T SET other = 'x'");
    });
    assertThat(database.query("sql", "SELECT FROM T WHERE uuid CONTAINSANY ['u8']").stream().count()).isEqualTo(1);
    assertThat(database.query("sql", "SELECT FROM T WHERE uuid CONTAINSANY [null]").stream().count()).isZero();
  }

  /**
   * A JSON array can reach the engine as a plain Java array rather than a List (PR #8475): both binding styles and both
   * operators must expand it exactly like a List.
   */
  @Test
  void javaArrayParameterBehavesLikeList() {
    final String[] ids = IDS.toArray(new String[0]);
    assertThat(uuids("SELECT uuid FROM T WHERE uuid IN :ids", Map.of("ids", ids))).containsExactly("u1", "u2");
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSANY ?", (Object) ids)).containsExactly("u1", "u2");
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSANY :ids", Map.of("ids", ids))).containsExactly("u1", "u2");
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSALL ?", (Object) new String[] { "u3" })).containsExactly("u3");
  }

  /**
   * The same answers when the scalar property is indexed, so the planner may resolve the condition through the index
   * instead of evaluating it record by record.
   */
  @Test
  void indexedScalarPropertyAnswersAlike() {
    database.command("sql", "CREATE PROPERTY T.uuid STRING");
    database.command("sql", "CREATE INDEX ON T (uuid) NOTUNIQUE");
    final String[] ids = IDS.toArray(new String[0]);
    for (final Object bound : new Object[] { IDS, ids }) {
      assertThat(uuids("SELECT uuid FROM T WHERE uuid IN ?", bound)).containsExactly("u1", "u2");
      assertThat(uuids("SELECT uuid FROM T WHERE uuid IN :ids", Map.of("ids", bound))).containsExactly("u1", "u2");
      assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSANY ?", bound)).containsExactly("u1", "u2");
      assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSANY :ids", Map.of("ids", bound))).containsExactly("u1", "u2");
      assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSALL ?", bound)).isEmpty();
    }
    assertThat(uuids("SELECT uuid FROM T WHERE uuid CONTAINSALL ['u1']")).containsExactly("u1");
  }

  private List<String> uuids(final String query, final Object... args) {
    try (final ResultSet rs = database.query("sql", query, args)) {
      return rs.stream().map(r -> r.<String>getProperty("uuid")).sorted().collect(Collectors.toList());
    }
  }

  private List<String> uuids(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", query, params)) {
      return rs.stream().map(r -> r.<String>getProperty("uuid")).sorted().collect(Collectors.toList());
    }
  }
}
