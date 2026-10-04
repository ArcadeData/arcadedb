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
package com.arcadedb.database;

import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #9038: reading a record of a closed database must throw {@link DatabaseIsClosedException}, not answer empty.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9038ReadAfterCloseTest {
  private static final String PATH = "target/databases/Issue9038ReadAfterCloseTest";

  @Test
  void recordReadsAfterCloseThrow() {
    FileUtils.deleteRecursively(new File(PATH));
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    try {
      try (final Database db = factory.create()) {
        db.command("sql", "CREATE DOCUMENT TYPE Person");
        db.transaction(() -> db.command("sql", "INSERT INTO Person SET name = 'Ann'"));
      }

      final Database db = factory.open();
      try {
        runAssertions(db);
      } finally {
        if (db.isOpen())
          db.close();
      }
    } finally {
      FileUtils.deleteRecursively(new File(PATH));
    }
  }

  private static void runAssertions(final Database db) {
    final Result sorted = db.query("sql", "SELECT FROM Person ORDER BY name").next();
    final Result projected = db.query("sql", "SELECT name FROM Person").next();
    final Document ann = db.query("sql", "SELECT FROM Person WHERE name = 'Ann'").next().getElement().get();
    assertThat(ann.get("name")).isEqualTo("Ann");
    db.close();

    assertThatThrownBy(() -> sorted.getProperty("name")).isInstanceOf(DatabaseIsClosedException.class);
    assertThatThrownBy(sorted::getPropertyNames).isInstanceOf(DatabaseIsClosedException.class);
    assertThatThrownBy(sorted::toJSON).isInstanceOf(DatabaseIsClosedException.class);
    assertThatThrownBy(() -> ann.get("name")).isInstanceOf(DatabaseIsClosedException.class);
    assertThatThrownBy(() -> ann.has("name")).isInstanceOf(DatabaseIsClosedException.class);
    assertThatThrownBy(ann::getPropertyNames).isInstanceOf(DatabaseIsClosedException.class);
    assertThatThrownBy(ann::toMap).isInstanceOf(DatabaseIsClosedException.class);
    assertThatThrownBy(ann::toJSON).isInstanceOf(DatabaseIsClosedException.class);
    // A PROJECTION COPIED ITS VALUES WHEN THE ROW WAS MADE
    assertThat((String) projected.getProperty("name")).isEqualTo("Ann");
  }
}
