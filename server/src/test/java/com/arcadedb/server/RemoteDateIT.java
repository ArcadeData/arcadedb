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
package com.arcadedb.server;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests dates by using server and/or remote connection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteDateIT extends BaseGraphServerTest {

  @Test
  public void testDateTimeMicros1() {

    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, "graph", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      // Create and configure the database through the server

      // Configure datetime settings
      database.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`");
      database.command("sql", """
          alter database `arcadedb.dateTimeFormat` "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
          """);

      // Create schema
      DocumentType dtOrders = database.getSchema().createDocumentType("Order");
      dtOrders.createProperty("vstart", Type.DATETIME_MICROS);

      LocalDateTime vstart = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);

      ResultSet resultSet = database.command("sql", "INSERT INTO Order SET vstart = ?", vstart);
      System.out.println("resultSet.next()\n          .toJSON() = " + resultSet.next()
          .toJSON());
      assertThat(resultSet.next()
          .toJSON()
          .getLong("vstart"))
          .isEqualTo(DateUtils.dateTimeToTimestamp(vstart, ChronoUnit.MICROS));
      resultSet = database.query("sql", "select from Order");
      Result result = resultSet.next();
      assertThat(result.toElement().get("vstart")).isEqualTo(vstart);

      resultSet = database.query("sql", "select from Order");
      result = resultSet.next();
      assertThat(result.toElement().get("vstart")).isEqualTo(vstart.toString());

    }
  }

}
