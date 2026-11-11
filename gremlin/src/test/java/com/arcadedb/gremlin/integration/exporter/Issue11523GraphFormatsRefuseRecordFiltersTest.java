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
package com.arcadedb.gremlin.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11523: the GraphML and GraphSON exports write the whole graph, so {@code includeTypes}, {@code excludeTypes} and
 * {@code includeRecords} were accepted and ignored: the export answered OK with every record in it. They are refused now, and
 * nothing is written.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11523GraphFormatsRefuseRecordFiltersTest {
  private static final String DATABASE_PATH = "target/databases/issue11523-graph-formats";

  @BeforeEach
  @AfterEach
  void clean() {
    TestServerHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    new File("exports/issue11523.graphson.tgz").delete();
    new File("exports/issue11523.graphml.tgz").delete();
  }

  @ParameterizedTest
  @ValueSource(strings = { "graphson", "graphml" })
  void recordFiltersAreRefusedByTheGraphFormats(final String format) {
    final String file = "issue11523." + format + ".tgz";
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("City");
      database.transaction(() -> {
        database.newVertex("Person").set("name", "Ann").save();
        database.newVertex("City").set("name", "Rome").save();
      });

      for (final String filter : new String[] { "includeTypes = 'Person'", "excludeTypes = 'City'", "includeRecords = '#1:0'" })
        assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://" + file + " WITH format = '" + format + "', " + filter))//
            .as("%s with %s", format, filter)//
            .hasStackTraceContaining("always exports the whole database");

      assertThat(new File("exports", file).exists()).isFalse();

      // CONTROL: WITHOUT A FILTER THE SAME FORMAT EXPORTS
      database.command("sql", "EXPORT DATABASE file://" + file + " WITH format = '" + format + "'");
      assertThat(new File("exports", file).exists()).isTrue();
    }
  }
}
