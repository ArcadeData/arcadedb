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
package com.arcadedb.integration.importer;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class FastTextVectorImportTest extends TestHelper
{
  @Test
  void vectorNeighborsQuery() {
        database.command("sql", "import database file://src/test/resources/cc.en.300.small.vec.gz "  //
                + "with distanceFunction = cosine, m = 16, beamWidth = 100, " //
                + "vertexType = Word, vectorProperty = vector, idProperty = name" //
        );
        assertThat(database.countType("Word", true)).isEqualTo(1000);

        // Verify LSMVector index was created
        final var index = database.getSchema().getIndexByName("Word[vector]");
        assertThat(index).isNotNull();

        // Verify we can query the data
        final ResultSet rs = database.query("sql", "SELECT FROM Word LIMIT 10");

        final AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
            final Result record = rs.next();
            assertThat(record).isNotNull();
            total.incrementAndGet();
        }
        rs.close();

        assertThat(total.get()).isEqualTo(10);
    }

  @Test
  void parsingLimitEntries() {
        database.command("sql", "import database file://src/test/resources/cc.en.300.small.vec.gz "  //
                + "with distanceFunction = cosine, m = 16, beamWidth = 100, " //
                + "vertexType = Word, vectorProperty = vector, idProperty = name, "
                + "parsingLimitEntries = 101"
        );

        // The header is skipped, so we expect 100 entries
        assertThat(database.countType("Word", true)).isEqualTo(100);
    }

  @Test
  void vectorNeighborsFunction() {
        database.command("sql", "import database file://src/test/resources/cc.en.300.small.vec.gz "  //
                + "with distanceFunction = cosine, m = 16, beamWidth = 100, " //
                + "vertexType = Word, vectorProperty = vector, idProperty = name" //
        );
        assertThat(database.countType("Word", true)).isEqualTo(1000);

        // Test vectorNeighbors with a word key
        final ResultSet rs = database.query("sql",
            "SELECT vectorNeighbors('Word[vector]', 'the', 5) as neighbors");

        assertThat(rs.hasNext()).isTrue();
        final Result result = rs.next();
        final Object neighbors = result.getProperty("neighbors");
        assertThat(neighbors).isNotNull();
        assertThat(neighbors).isInstanceOf(List.class);

        final List<?> neighborsList = (List<?>) neighbors;
        assertThat(neighborsList).hasSizeLessThanOrEqualTo(5);

        // Verify each neighbor has vertex and distance
        for (Object neighbor : neighborsList) {
            assertThat(neighbor).isInstanceOf(Map.class);
            final Map<String, Object> neighborMap = (Map<String, Object>) neighbor;
            assertThat(neighborMap).containsKey("vertex");
            assertThat(neighborMap).containsKey("distance");
            assertThat(neighborMap.get("distance")).isInstanceOf(Number.class);
        }

        rs.close();

        // Test vectorNeighbors with a vector array
        final ResultSet rs2 = database.query("sql", "SELECT vector FROM Word WHERE name = 'the' LIMIT 1");
        assertThat(rs2.hasNext()).isTrue();
        final float[] queryVector = rs2.next().getProperty("vector");
        rs2.close();

        final ResultSet rs3 = database.query("sql",
            "SELECT vectorNeighbors('Word[vector]', ?, 3) as neighbors", queryVector);

        assertThat(rs3.hasNext()).isTrue();
        final Result result3 = rs3.next();
        final Object neighbors3 = result3.getProperty("neighbors");
        assertThat(neighbors3).isNotNull();
        assertThat(neighbors3).isInstanceOf(List.class);

        final List<?> neighborsList3 = (List<?>) neighbors3;
        assertThat(neighborsList3).hasSizeLessThanOrEqualTo(3);

        rs3.close();
    }

    @Override
    protected String getDatabasePath() {
        return "target/databases/test-fasttextsmall";
    }
}
