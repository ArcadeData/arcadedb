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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9804: the remote driver removed every key of {@link Property#METADATA_PROPERTIES} from a projection
 * row that carried a {@code @props} hint, so {@code SELECT @rid FROM Person} (a LINK column, which always emits the hint) answered a
 * row with no columns at all, and a Gremlin {@code elementMap()} row lost the {@code @rid} and {@code @type} the flat-map fix named
 * on purpose. A row without {@code @cat} is a projection: its column names belong to the query, and the only key the driver
 * consumes is the hint itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9804RemoteProjectionMetadataNamesTest {
  private RemoteDatabase database;

  @BeforeEach
  void setUp() {
    database = new RemoteDatabase("localhost", 2480, "testdb", "root", "test", new ContextConfiguration()) {
      @Override
      void requestClusterConfiguration() {
        // NO HTTP CALLS IN A UNIT TEST
      }
    };
  }

  @AfterEach
  void tearDown() {
    if (database.isOpen())
      database.close();
  }

  @Test
  void aRidColumnSurvivesAsTheOnlyColumnOfAProjection() {
    // What the server sends for "select @rid, name from Person": the hint exists only because of the @rid column (a LINK)
    final Result result = database.json2Result(new JSONObject("{\"@rid\":\"#1:0\",\"name\":\"Jay\",\"@props\":\"@rid:11\"}"));

    assertThat(result.getPropertyNames()).containsExactly("@rid", "name");
    assertThat(result.<Object>getProperty("@rid")).hasToString("#1:0");
    assertThat(result.<String>getProperty("name")).isEqualTo("Jay");
    assertThat(result.isElement()).isFalse();
  }

  @Test
  void aGremlinElementMapRowKeepsItsFlatMapNames() {
    final Result result = database.json2Result(
        new JSONObject("{\"@rid\":\"#1:0\",\"@type\":\"Person\",\"name\":\"Jay\",\"age\":42,\"@props\":\"age:3\"}"));

    assertThat(result.getPropertyNames()).containsExactly("@rid", "@type", "name", "age");
    assertThat(result.<String>getProperty("@type")).isEqualTo("Person");
    assertThat(result.<Object>getProperty("age")).isEqualTo(42L);
  }

  @Test
  void everyMetadataNameIsAColumnNameOfAProjection() {
    // The claim, not the case: @props is the hint the driver consumes, @cat would make the row a record, and every other name
    // of the metadata vocabulary is an ordinary column on a row the server did not mark as a record
    final Set<String> names = new TreeSet<>(Property.METADATA_PROPERTIES);
    names.remove(Property.PROPERTY_TYPES_PROPERTY);
    names.remove(Property.CAT_PROPERTY);
    assertThat(names).containsExactlyInAnyOrder("@rid", "@type", "@in", "@out");

    for (final String name : names) {
      final JSONObject row = new JSONObject();
      row.put(name, "#1:0");
      row.put("n", 7L);
      // "n:3" is LONG: the hint is what arms the branch that used to delete the column
      row.put(Property.PROPERTY_TYPES_PROPERTY, "n:3");

      final Result result = database.json2Result(row);

      assertThat(result.getPropertyNames()).as("columns of a projection selecting %s", name).containsExactly(name, "n");
      assertThat(result.<Object>getProperty(name)).as(name).isEqualTo("#1:0");
      assertThat(result.<Object>getProperty("n")).as("typed by its hint").isEqualTo(7L);
      assertThat(result.getPropertyNames()).as("the hint is consumed, never a column")
          .doesNotContain(Property.PROPERTY_TYPES_PROPERTY);
    }
  }

  @Test
  void aProjectionOfAnEdgeKeepsItsInAndOutColumns() {
    final Result result = database.json2Result(
        new JSONObject("{\"@out\":\"#1:0\",\"@in\":\"#2:0\",\"w\":2.5,\"@props\":\"@out:11,@in:11\"}"));

    assertThat(result.getPropertyNames()).containsExactly("@out", "@in", "w");
  }

  @Test
  void aProjectionNamedLikeTheRidStillGetsItsTypedValues() {
    final Result result = database.json2Result(new JSONObject("{\"@rid\":\"#1:0\",\"big\":9,\"@props\":\"@rid:11,big:3\"}"));

    assertThat(List.copyOf(result.getPropertyNames())).containsExactly("@rid", "big");
    assertThat(result.<Object>getProperty("big")).isEqualTo(9L);
  }
}
