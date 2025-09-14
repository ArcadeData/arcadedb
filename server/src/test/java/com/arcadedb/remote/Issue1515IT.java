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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class Issue1515IT extends BaseGraphServerTest {

  protected String getDatabaseName() {
    return "issue1515";
  }

  @Test
  void rename_me() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    String script = """
        alter database `arcadedb.dateImplementation` `java.time.LocalDate`;
        alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`;
        alter database `arcadedb.dateFormat` 'dd MM yyyy GG';
        alter database `arcadedb.dateTimeFormat` 'dd MM yyyy GG HH:mm:ss';
        create property Person.name if not exists String (mandatory true, notnull true);
        create index if not exists on Person (name) unique;
        create property Person.dateOfBirth if not exists Date;
        create property Person.dateOfDeath if not exists Date;
        """;

    database.transaction(() ->
        database.command("sqlscript", script));

    database.transaction(() ->
        database.command("sql", """
            insert into Person set name = 'Hannibal',
            dateOfBirth = date('01 01 0001 BC', 'dd MM yyyy GG'),
            dateOfDeath = date('01 01 0001 AD', 'dd MM yyyy GG')
            """));

    ResultSet result = database.query("sql", "select from Person where name = 'Hannibal'");
    Result doc = result.next();
    assertThat(doc.<String>getProperty("dateOfBirth")).isEqualTo("01 01 0001 BC");
    assertThat(doc.<String>getProperty("dateOfDeath")).isEqualTo("01 01 0001 AD");

  }
}
