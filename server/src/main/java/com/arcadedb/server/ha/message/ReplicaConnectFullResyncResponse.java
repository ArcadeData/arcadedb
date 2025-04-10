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
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;

import java.util.*;

public class ReplicaConnectFullResyncResponse extends HAAbstractCommand {
  private Set<String> databases;

  public ReplicaConnectFullResyncResponse() {
  }

  public ReplicaConnectFullResyncResponse(final Set<String> databases) {
    this.databases = databases;
  }

  @Override
  public HACommand execute(final HAServer server, final HAServer.ServerInfo remoteServerName, final long messageNumber) {
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putUnsignedNumber(databases.size());
    for (final String db : databases)
      stream.putString(db);
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    databases = new HashSet<>();
    final int fileCount = (int) stream.getUnsignedNumber();
    for (int i = 0; i < fileCount; ++i)
      databases.add(stream.getString());
  }

  public Set<String> getDatabases() {
    return databases;
  }

  @Override
  public String toString() {
    return "fullResync(dbs=" + databases + ")";
  }
}
