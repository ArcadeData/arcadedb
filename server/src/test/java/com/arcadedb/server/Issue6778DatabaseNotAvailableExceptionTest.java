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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.DatabaseNotAvailableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6778: {@link ArcadeDBServer#getDatabase(String, boolean, boolean)} with
 * {@code allowLoad=false} and no open handle for the name must throw the narrow
 * {@link DatabaseNotAvailableException} - not the generic {@code DatabaseOperationException} it used to
 * throw - so {@code AbstractServerHttpHandler} can answer an accurate 404 instead of falling through to a
 * generic 500 (pinned separately in {@code Issue6201ErrorStatusParityTest}).
 */
class Issue6778DatabaseNotAvailableExceptionTest extends StaticBaseServerTest {
  private ArcadeDBServer server;

  @BeforeEach
  public void beginTest() {
    super.beginTest();
  }

  @AfterEach
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();
    super.endTest();
  }

  @Test
  void allowLoadFalseWithNoOpenHandleThrowsDatabaseNotAvailableException() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);

    server = new ArcadeDBServer(config);
    server.start();

    assertThatThrownBy(() -> server.getDatabase("issue6778-never-opened", false, false))
        .isInstanceOf(DatabaseNotAvailableException.class)
        .hasMessageContaining("issue6778-never-opened");
  }
}
