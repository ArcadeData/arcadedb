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

/** The {@code facets} line of the ndjson encoding: one single-key line between the records and the {@code stats} trailer. */
class NdJsonResultStreamFacetsTest {

  @Test
  void facetsLineSitsBetweenRecordsAndStats() throws IOException {
    final FacetResults facets = FacetResultsSamples.allKinds();
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final NdJsonResultStream stream = new NdJsonResultStream(bytes)) {
      stream.writeRecord(new JSONObject().put("n", 1));
      stream.writeFacets(facets.toJSON());
      stream.writeStats(3, 1, false);
    }

    final String[] lines = bytes.toString(StandardCharsets.UTF_8).split("\n");
    assertThat(lines).hasSize(3);
    final JSONObject record = new JSONObject(lines[0]);
    final JSONObject facetsLine = new JSONObject(lines[1]);
    final JSONObject stats = new JSONObject(lines[2]);
    assertThat(record.keySet()).containsExactly("record");
    assertThat(facetsLine.keySet()).containsExactly("facets");
    assertThat(stats.keySet()).containsExactly("stats");

    // exactly FacetResults.toJSON(), and it parses back into the same results
    assertThat(facetsLine.getJSONObject("facets").toString()).isEqualTo(facets.toJSON().toString());
    assertThat(FacetResults.fromJSON(facetsLine.getJSONObject("facets")).toJSON().toString()).isEqualTo(facets.toJSON().toString());
  }
}
