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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.server.security.ServerSecurityException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ServerDefaultDatabasesIT extends BaseGraphServerTest {

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected boolean isPopulateDatabase() {
    return false;
  }

  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, "Universe[elon:musk:admin];Amiga[Jay:Miner:admin,Jack:Tramiel:admin,root]");
  }

  @Test
  public void checkDefaultDatabases() throws IOException {
    getServer(0).getSecurity().authenticate("elon", "musk", null);
    getServer(0).getSecurity().authenticate("elon", "musk", "Universe");
    getServer(0).getSecurity().authenticate("Jay", "Miner", null);
    getServer(0).getSecurity().authenticate("Jay", "Miner", "Amiga");
    getServer(0).getSecurity().authenticate("Jack", "Tramiel", null);
    getServer(0).getSecurity().authenticate("Jack", "Tramiel", "Amiga");
    getServer(0).getSecurity().authenticate("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS, null);
    getServer(0).getSecurity().authenticate("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS, "Amiga");

    try {
      getServer(0).getSecurity().authenticate("elon", "musk", "Amiga");
      Assertions.fail();
    } catch (ServerSecurityException e) {
      // EXPECTED
    }

    try {
      getServer(0).getSecurity().authenticate("Jack", "Tramiel", "Universe");
      Assertions.fail();
    } catch (ServerSecurityException e) {
      // EXPECTED
    }

    try {
      getServer(0).getSecurity().authenticate("Jack", "Tramiel", "RandomName");
      Assertions.fail();
    } catch (ServerSecurityException e) {
      // EXPECTED
    }

    Assertions.assertTrue(getServer(0).existsDatabase("Universe"));
    Assertions.assertTrue(getServer(0).existsDatabase("Amiga"));

    ((DatabaseInternal) getServer(0).getDatabase("Universe")).getEmbedded().drop();
    ((DatabaseInternal) getServer(0).getDatabase("Amiga")).getEmbedded().drop();
  }
}
