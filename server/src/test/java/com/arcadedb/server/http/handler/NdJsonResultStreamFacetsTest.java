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

import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.facet.FacetResultsSamples;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
