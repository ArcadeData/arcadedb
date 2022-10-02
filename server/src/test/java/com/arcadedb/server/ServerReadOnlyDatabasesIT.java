/*
 * Copyright © 2022-present Arcade Data Ltd (info@arcadedata.com)
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
 * SPDX-FileCopyrightText: 2022-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server;

import static com.arcadedb.engine.PaginatedFile.MODE.READ_ONLY;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;

public class ServerReadOnlyDatabasesIT extends BaseGraphServerTest {

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected void populateDatabase() {
  }

  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, "Universe[elon:musk:admin];Amiga[Jay:Miner:admin,Jack:Tramiel:admin,root]");
    config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASE_MODE, "READ_ONLY");
  }

  @Test
  public void checkDefaultDatabases() throws IOException {
    Assertions.assertTrue(getServer(0).existsDatabase("Universe"));
    Assertions.assertTrue(getServer(0).existsDatabase("Amiga"));

    Assertions.assertTrue(READ_ONLY.equals(getServer(0).getDatabase("Universe").getMode()));
    Assertions.assertTrue(READ_ONLY.equals(getServer(0).getDatabase("Amiga").getMode()));
  }
}
