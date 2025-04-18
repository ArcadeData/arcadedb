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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.logging.*;

/**
 * Executes all the tests while the server is up and running.
 */
public abstract class StaticBaseServerTest {
  public static final String DEFAULT_PASSWORD_FOR_TESTS = "DefaultPasswordForTests";

  protected StaticBaseServerTest() {
  }

  public void setTestConfiguration() {
    GlobalConfiguration.resetAll();
    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);
    GlobalConfiguration.SERVER_HTTP_IO_THREADS.setValue(2);
    GlobalConfiguration.TYPE_DEFAULT_BUCKETS.setValue(2);
  }

  @BeforeEach
  public void beginTest() {
    TestServerHelper.checkActiveDatabases();
    TestServerHelper.deleteDatabaseFolders(5);

    setTestConfiguration();

    LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "Starting test...");
  }

  @AfterEach
  public void endTest() {
    TestServerHelper.checkActiveDatabases();
    TestServerHelper.deleteDatabaseFolders(5);
  }

  protected static void testLog(final String msg, final Object... args) {
    LogManager.instance().log(StaticBaseServerTest.class, Level.INFO,
        "***********************************************************************************");
    LogManager.instance().log(StaticBaseServerTest.class, Level.INFO, "TEST: " + msg, args);
    LogManager.instance().log(StaticBaseServerTest.class, Level.INFO,
        "***********************************************************************************");
  }
}
