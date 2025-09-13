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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class Issue1898Test extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sqlscript", """
          CREATE VERTEX TYPE Asset IF NOT EXISTS;
          CREATE PROPERTY Asset.id IF NOT EXISTS STRING (mandatory true);
          CREATE PROPERTY Asset.addresses IF NOT EXISTS LIST OF STRING;
          CREATE INDEX IF NOT EXISTS ON Asset (id) UNIQUE;
          """);
    });

    database.transaction(() -> {
      database.command("sqlscript", """
          INSERT INTO Asset CONTENT {"id":"first", "addresses":["192.168.10.10","192.168.20.10"]};
          INSERT INTO Asset CONTENT {"id":"second"};
          INSERT INTO Asset CONTENT {"id":"third"};
          """);
    });
  }

  @Test
  void selectWithOr() {

    database.transaction(() -> {
      final ResultSet rs = database.command("SQL", """
          select from Asset WHERE addresses CONTAINS '192.168.10.10'
          """);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.stream().count()).isEqualTo(1);
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", """
          select from Asset where
          id='wrong id'
          OR
          addresses CONTAINS '192.168.10.10'
          """);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.nextIfAvailable().getProperty("id").equals("first")).isTrue();
      assertThat(rs.hasNext()).isFalse();
    });
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", """
          select from Asset where id='first' OR id='second'
          """);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.stream().count()).isEqualTo(2);
    });
  }
}
