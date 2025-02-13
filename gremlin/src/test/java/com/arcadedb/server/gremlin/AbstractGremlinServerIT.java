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
package com.arcadedb.server.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.fail;
import java.io.IOException;

import static org.assertj.core.api.Assertions.fail;

public abstract class AbstractGremlinServerIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();

    // COPY GREMLIN SERVER FILES BEFORE STARTING THE GREMLIN SERVER
    new File("./target/config").mkdirs();

    try {
      FileUtils.writeFile(new File("./target/config/gremlin-server.yaml"),
          FileUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("gremlin-server.yaml"), "utf8"));

      FileUtils.writeFile(new File("./target/config/gremlin-server.properties"),
          FileUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("gremlin-server.properties"), "utf8"));

      FileUtils.writeFile(new File("./target/config/gremlin-server.groovy"),
          FileUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("gremlin-server.groovy"), "utf8"));

      GlobalConfiguration.SERVER_PLUGINS.setValue("GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin");

    } catch (final IOException e) {
      fail("", e);
    }
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(getDatabaseName()))
      server.create(getDatabaseName());
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(getDatabaseName()))
      server.drop(getDatabaseName());
    super.endTest();
  }
}
