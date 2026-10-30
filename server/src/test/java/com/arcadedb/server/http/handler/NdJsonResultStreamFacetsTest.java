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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.facet.DistinctValue;
import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.facet.FacetResultsSamples;
import com.arcadedb.query.facet.FacetStats;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The facets of the ndjson encoding ride in the {@code stats} trailer (final review B-M2): no event a client that predates them does
 * not know, and the trailer of any other statement is the plain triple.
 */
class NdJsonResultStreamFacetsTest {

  @Test
  void facetsRideInTheStatsTrailer() throws IOException {
    final FacetResults facets = FacetResultsSamples.allKinds();
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final NdJsonResultStream stream = new NdJsonResultStream(bytes)) {
      stream.writeRecord(new JSONObject().put("n", 1));
      stream.writeStats(3, 1, false, facets.toJSON());
    }

    final String[] lines = bytes.toString(StandardCharsets.UTF_8).split("\n");
    assertThat(lines).hasSize(2);
    assertThat(new JSONObject(lines[0]).keySet()).containsExactly("record");
    final JSONObject trailer = new JSONObject(lines[1]);
    assertThat(trailer.keySet()).containsExactly("stats");
    final JSONObject stats = trailer.getJSONObject("stats");
    assertThat(stats.keySet()).containsExactly("limit", "returned", "truncated", "facets");
    assertThat(stats.getInt("limit")).isEqualTo(3);
    assertThat(stats.getInt("returned")).isEqualTo(1);

    // exactly the facets given, and they parse back into the same results
    assertThat(stats.getJSONObject("facets").toString()).isEqualTo(facets.toJSON().toString());
    assertThat(FacetResults.fromJSON(stats.getJSONObject("facets")).toJSON().toString()).isEqualTo(facets.toJSON().toString());
  }

  /**
   * Plan 5 Task 14: the plan 5 kinds ride in the trailer exactly as the buffered member writes them ({@code facetsJson}): hierarchy
   * attributes, DISTINCT {@code value}/{@code exact}, STATS with a date-time extreme in the database's format, a FLOAT sum with its own
   * type hint and a NaN average by its name, and the cursor of a paged facet.
   */
  @Test
  void plan5KindsRideInTheTrailerAsTheBufferedMemberWritesThem() throws IOException {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("t", FacetResult.of("hierarchy", "index", List.of(FacetBucket.term("t0/t1", 4L)
        .withAttributes(new JSONObject().put("parent", "t0").put("depth", 1))), 0L, 0L).withValueType(Type.STRING));
    map.put("d", FacetResult.ofDistinct("index", new DistinctValue(9L, true), 0L));
    map.put("p", FacetResult.ofStats("index", new FacetStats(2L, 0L, LocalDateTime.of(2020, 1, 1, 0, 0), LocalDateTime.of(2024, 2, 29, 10, 30, 15),
        null, null)).withValueType(Type.DATETIME));
    map.put("r", FacetResult.ofStats("index", new FacetStats(2L, 0L, 0.5f, 1.0f, 1.5f, Double.NaN)).withValueType(Type.FLOAT));
    map.put("b", FacetResult.of("terms", "index", List.of(FacetBucket.term("c", 1L)), 4L, 0L).withAfter("c").withValueType(Type.STRING));
    final FacetResults facets = new FacetResults(true, 0L, 0L, map);

    final String path = "./target/databases/" + getClass().getSimpleName();
    try (final DatabaseFactory factory = new DatabaseFactory(path)) {
      if (factory.exists())
        factory.open().drop();
      final Database database = factory.create();
      try {
        final JSONObject buffered = AbstractQueryHandler.facetsJson(database, facets, true);
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (final NdJsonResultStream stream = new NdJsonResultStream(bytes)) {
          stream.writeStats(-1, 0, false, AbstractQueryHandler.facetsJson(database, facets, true));
        }
        final JSONObject trailer = new JSONObject(bytes.toString(StandardCharsets.UTF_8).trim()).getJSONObject("stats").getJSONObject("facets");
        assertThat(trailer.toString()).isEqualTo(new JSONObject(buffered.toString()).toString());

        assertThat(trailer.getJSONObject("t").getJSONArray("buckets").getJSONObject(0).getString("parent")).isEqualTo("t0");
        assertThat(trailer.getJSONObject("t").getJSONArray("buckets").getJSONObject(0).getInt("depth")).isEqualTo(1);
        assertThat(trailer.getJSONObject("d").getLong("value")).isEqualTo(9L);
        assertThat(trailer.getJSONObject("d").getBoolean("exact")).isTrue();
        assertThat(trailer.getJSONObject("p").getString("max")).isEqualTo("2024-02-29 10:30:15.000");
        assertThat(trailer.getJSONObject("p").isNull("sum")).isTrue();
        assertThat(trailer.getJSONObject("r").getString("@props")).isEqualTo("value:" + Type.FLOAT.getId() + ",sum:" + Type.FLOAT.getId());
        assertThat(trailer.getJSONObject("r").getString("avg")).isEqualTo("NaN");
        assertThat(trailer.getJSONObject("b").getString("after")).isEqualTo("c");
      } finally {
        database.drop();
      }
    }
  }

  @Test
  void anyOtherStatementKeepsThePlainTrailer() throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final NdJsonResultStream stream = new NdJsonResultStream(bytes)) {
      stream.writeStats(3, 0, false, null);
    }
    assertThat(new JSONObject(bytes.toString(StandardCharsets.UTF_8).trim()).getJSONObject("stats").keySet())
        .containsExactly("limit", "returned", "truncated");
  }
}
