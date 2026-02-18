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

import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.temporal.*;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteTypesIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void documentType() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase db = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      db.command("sqlscript", """
          create vertex type SimpleVertex if not exists;
          alter type SimpleVertex custom javaClass='test.SimpleVertex';

          create property SimpleVertex.uuid if not exists STRING;
          create property SimpleVertex.s  if not exists  STRING;
          create property SimpleVertex.i  if not exists  INTEGER;
          create property SimpleVertex.f  if not exists FLOAT;
          create property SimpleVertex.b  if not exists BOOLEAN;
          create property SimpleVertex.fecha  if not exists  DATETIME;
          create property SimpleVertex.serial  if not exists LONG;
          create property SimpleVertex.oI  if not exists INTEGER;
          create property SimpleVertex.oFl  if not exists FLOAT;
          create property SimpleVertex.oB  if not exists  BOOLEAN;
          """);

      db.begin();
      final RemoteMutableVertex nvSaved = db.newVertex("SimpleVertex");

      nvSaved.set("s", "string");
      nvSaved.set("b", true);
      nvSaved.set("oB", true);
      float f = 1.0f;
      nvSaved.set("f", f);
      nvSaved.set("oFl", 1.0f);
      nvSaved.set("i", 1);
      nvSaved.set("oI", 1);
      LocalDateTime targetDate = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
      nvSaved.set("fecha", targetDate);
      nvSaved.save();

      db.commit();

      RID rid = nvSaved.getIdentity();
      assertThat(rid).isNotNull();

      MutableVertex v = db.lookupByRID(rid).asVertex().modify();
      v.reload();

      assertThat(v.getIdentity()).isEqualTo(rid);

      // String property
      assertThat(v.get("s")).isEqualTo("string");
      assertThat(v.get("s")).isInstanceOf(String.class);

      // Boolean properties
      assertThat(v.get("b")).isEqualTo(true);
      assertThat(v.get("b")).isInstanceOf(Boolean.class);
      assertThat(v.get("oB")).isEqualTo(true);
      assertThat(v.get("oB")).isInstanceOf(Boolean.class);

      // Float properties
      assertThat(v.get("f")).isEqualTo(1.0f);
      assertThat(v.get("f")).isInstanceOf(Float.class);
      assertThat(v.getFloat("f")).isEqualTo(1.0f);
      assertThat(v.get("oFl")).isEqualTo(1.0f);
      assertThat(v.get("oFl")).isInstanceOf(Float.class);

      // Integer properties
      assertThat(v.get("i")).isEqualTo(1);
      assertThat(v.get("i")).isInstanceOf(Integer.class);
      assertThat(v.get("oI")).isEqualTo(1);
      assertThat(v.get("oI")).isInstanceOf(Integer.class);

      // DateTime properties (truncate to seconds since milliseconds are not preserved)
      assertThat(v.get("fecha")).isInstanceOf(LocalDateTime.class);
      assertThat(v.getDate("fecha")).isInstanceOf(Date.class);
      assertThat(v.getLocalDateTime("fecha")).isEqualTo(targetDate.truncatedTo(ChronoUnit.SECONDS));
    });
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
