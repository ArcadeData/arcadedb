/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteTypesIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  public void documentType() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase db = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      db.command("sqlscript", """
          create vertex type SimpleVertex if not exists;
          alter type SimpleVertex custom javaClass='test.SimpleVertex';

          create property SimpleVertex.s  if not exists  STRING;
          create property SimpleVertex.i  if not exists  INTEGER;
          create property SimpleVertex.sh  if not exists SHORT;
          create property SimpleVertex.f  if not exists FLOAT;
          create property SimpleVertex.b  if not exists BOOLEAN;
          create property SimpleVertex.fecha  if not exists  DATETIME;
          create property SimpleVertex.serial  if not exists LONG;
          create property SimpleVertex.oI  if not exists INTEGER;
          create property SimpleVertex.oF  if not exists FLOAT;
          create property SimpleVertex.oB  if not exists  BOOLEAN;
          """);

      db.begin();
      final RemoteMutableVertex nvSaved = db.newVertex("SimpleVertex");

      nvSaved.set("s", "string");
      nvSaved.set("b", true);
      nvSaved.set("oB", true);
      float f = 1.0f;
      nvSaved.set("f", f);
      nvSaved.set("oF", 1.0f);
      nvSaved.set("sh", (short) 1);
      nvSaved.set("i", 1);
      nvSaved.set("oI", 1);
      LocalDateTime targetDate = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
      nvSaved.set("fecha", targetDate);
      nvSaved.save();

      db.commit();

      RID rid = nvSaved.getIdentity();

      MutableVertex v = db.lookupByRID(rid).asVertex().modify();
      v.reload();

      assertThat(nvSaved.get("s")).isInstanceOf(String.class);
      assertThat(nvSaved.get("b")).isInstanceOf(Boolean.class);
      assertThat(nvSaved.get("oB")).isInstanceOf(Boolean.class);
      assertThat(nvSaved.get("f")).isInstanceOf(Float.class);
      assertThat(nvSaved.get("oF")).isInstanceOf(Float.class);
      assertThat(nvSaved.get("i")).isInstanceOf(Integer.class);
      assertThat(nvSaved.get("oI")).isInstanceOf(Integer.class);
      assertThat(nvSaved.get("fecha")).isInstanceOf(LocalDateTime.class);
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
