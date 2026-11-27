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
package com.arcadedb.mongo;

import com.arcadedb.database.Database;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code mongo} query language over the HTTP query endpoint: the rows are read as the response is written, so the row limit of
 * the request bounds what is read from the collection, and a limit that covers the collection still answers all of it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class MongoQueryLanguageHttpTest extends BaseMongoServerTest {
  private static final String TYPE    = "HttpItem";
  private static final int    RECORDS = 6_000;

  @Test
  void theRowLimitOfARequestBoundsWhatIsReadFromTheCollection() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createDocumentType(TYPE);
    database.transaction(() -> {
      for (int k = 1; k <= RECORDS; k++)
        database.newDocument(TYPE).set("k", k).save();
    });
    final AtomicLong reads = new AtomicLong();
    final AfterRecordReadListener listener = record -> {
      reads.incrementAndGet();
      return record;
    };
    database.getSchema().getType(TYPE).getEvents().registerListener(listener);
    try {
      final JSONArray limited = query("{ collection: '" + TYPE + "', query: {} }", 7).getJSONArray("result");
      assertThat(limited.length()).isEqualTo(7);
      assertThat(reads.get()).as("seven rows asked for, a scan batch read ahead: not the collection").isLessThan(RECORDS / 2);

      reads.set(0);
      final JSONArray sorted = query("{ collection: '" + TYPE + "', query: { $orderBy: { k: -1 } } }", 3).getJSONArray("result");
      assertThat(sorted.length()).isEqualTo(3);
      assertThat(sorted.getJSONObject(0).getInt("k")).isEqualTo(RECORDS);
      assertThat(sorted.getJSONObject(2).getInt("k")).isEqualTo(RECORDS - 2);

      final JSONArray all = query("{ collection: '" + TYPE + "', query: {} }", RECORDS).getJSONArray("result");
      assertThat(all.length()).as("a limit that covers the collection answers all of it").isEqualTo(RECORDS);
    } finally {
      database.getSchema().getType(TYPE).getEvents().unregisterListener(listener);
    }
  }

  private JSONObject query(final String command, final int limit) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(0, "/api/v1/query/" + getDatabaseName())).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    final JSONObject payload = new JSONObject().put("language", "mongo").put("command", command).put("serializer", "record");
    if (limit > 0)
      payload.put("limit", limit);
    formatPayload(connection, payload);
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(readResponse(connection));
    } finally {
      connection.disconnect();
    }
  }
}
