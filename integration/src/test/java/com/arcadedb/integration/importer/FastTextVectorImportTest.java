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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class FastTextVectorImportTest extends com.arcadedb.TestHelper
{
    @Test
    public void vectorNeighborsQuery() {
        database.command("sql", "import database file://src/test/resources/cc.en.300.small.vec.gz "  //
                + "with distanceFunction = cosine, m = 16, ef = 128, efConstruction = 128, " //
                + "vertexType = Word, edgeType = Proximity, vectorProperty = vector, idProperty = name" //
        );
        assertThat(database.countType("Word", true)).isEqualTo(1000);

        final ResultSet rs = database.command("SQL",
                "select expand(vectorNeighbors('Word[name,vector]','with',10))");

        final AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
            final Result record = rs.next();
            assertThat(record).isNotNull();
            Vertex vertex = (Vertex) record.getElementProperty("vertex");
            Float distance = record.getProperty("distance");
            total.incrementAndGet();
        }

        assertThat(total.get()).isEqualTo(10);
    }

    @Test
    public void parsingLimitEntries() {
        database.command("sql", "import database file://src/test/resources/cc.en.300.small.vec.gz "  //
                + "with distanceFunction = cosine, m = 16, ef = 128, efConstruction = 128, " //
                + "vertexType = Word, edgeType = Proximity, vectorProperty = vector, idProperty = name, "
                + "parsingLimitEntries = 101"
        );

        // The header is skipped, so we expect 100 entries
        assertThat(database.countType("Word", true)).isEqualTo(100);
    }

    @Override
    protected String getDatabasePath() {
        return "target/databases/test-fasttextsmall";
    }
}
