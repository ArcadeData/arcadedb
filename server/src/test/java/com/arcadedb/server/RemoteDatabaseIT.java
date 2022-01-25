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
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RemoteDatabaseIT extends BaseGraphServerTest {

  @Test
  public void simpleTx() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, "graph", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      // BEGIN
      database.transaction(() -> {
        // CREATE DOCUMENT
        ResultSet result = database.command("SQL", "insert into Person set name = 'Elon'");
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.hasNext());
        Result rec = result.next();
        Assertions.assertTrue(rec.toJSON().contains("Elon"));
        String rid = rec.getProperty("@rid");

        // RETRIEVE DOCUMENT WITH QUERY
        result = database.query("SQL", "select from Person where name = 'Elon'");
        Assertions.assertTrue(result.hasNext());

        // UPDATE DOCUMENT WITH COMMAND
        result = database.command("SQL", "update Person set lastName = 'Musk' where name = 'Elon'");
        Assertions.assertTrue(result.hasNext());
        Assertions.assertEquals(1, new JSONObject(result.next().toJSON()).getInt("count"));

        JSONObject record = database.lookupByRID(rid);
        Assertions.assertNotNull(result);
        Assertions.assertEquals("Musk", record.getString("lastName"));
      });

      // RETRIEVE DOCUMENT WITH QUERY AFTER COMMIT
      final ResultSet result = database.query("SQL", "select from Person where name = 'Elon'");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals("Musk", result.next().getProperty("lastName"));
    });
  }
}
