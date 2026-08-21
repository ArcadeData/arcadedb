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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6455: the JSONL exporter wrote a DATE property as epoch MILLISECONDS through the raw
 * {@code JSONObject.put(Object)} temporal branch, while the importer decodes a DATE number as epoch
 * DAYS. Every DATE at or after ~August 1981 overflows {@code LocalDate.ofEpochDay}'s range: the
 * resulting {@code DateTimeException} is swallowed by {@code Type.convert}'s broad exception catch
 * (logged at FINE, invisible by default) and the property is silently set to {@code null} instead of
 * the original date. Separately, DATETIME_NANOS/DATETIME_MICROS were hardcoded to millisecond
 * precision on export, losing everything past the third fractional digit.
 * <p>
 * Pins the full round trip: the modern-dated vertex and its edge both survive, and the DATE /
 * DATETIME_NANOS values come back exact instead of null / truncated.
 */
class Issue6455JsonlDateTimeRoundTripIT {
  private static final String SOURCE_PATH = "target/databases/issue6455-jsonl-datetime-source";
  private static final String TARGET_PATH = "target/databases/issue6455-jsonl-datetime-target";
  private static final String FILE        = "target/issue6455-jsonl-datetime.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File(FILE).delete();
  }

  @Test
  void modernDateAndNanosPrecisionSurviveAnExportImportCycle() throws Exception {
    final LocalDate birth = LocalDate.of(2024, 1, 15);
    final LocalDateTime createdAt = LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_456_789);

    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.transaction(() -> {
        final VertexType personType = source.getSchema().buildVertexType().withName("Person").create();
        personType.createProperty("id", Type.INTEGER);
        personType.createProperty("birth", Type.DATE);
        personType.createProperty("createdAt", Type.DATETIME_NANOS);
        source.getSchema().buildEdgeType().withName("Friend").create();

        final MutableVertex a = source.newVertex("Person").set("id", 0).set("birth", birth).set("createdAt", createdAt)
            .save();
        final MutableVertex b = source.newVertex("Person").set("id", 1).save();
        a.newEdge("Friend", b);
      });

      assertThat(source.countType("Person", false)).isEqualTo(2);
      assertThat(source.countType("Friend", false)).isEqualTo(1);
    }

    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(FILE).exists()).isTrue();

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + new File(FILE).getAbsolutePath());
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      // Neither the modern-dated vertex nor its edge was silently dropped.
      assertThat(target.countType("Person", false)).as("the modern-dated vertex must not be dropped").isEqualTo(2);
      assertThat(target.countType("Friend", false)).as("the edge must not cascade-drop with its vertex").isEqualTo(1);

      final Vertex imported = target.query("sql", "select from Person where id = ?", 0).next().getVertex().get();
      assertThat((LocalDate) imported.get("birth")).isEqualTo(birth);
      assertThat((LocalDateTime) imported.get("createdAt")).isEqualTo(createdAt);
    }
  }
}
