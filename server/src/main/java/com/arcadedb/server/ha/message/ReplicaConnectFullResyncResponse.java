/*
 * Copyright 2021 Arcade Data Ltd
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
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

import java.util.HashSet;
import java.util.Set;

public class ReplicaConnectFullResyncResponse extends HAAbstractCommand {
  private long        lastMessageNumber;
  private Set<String> databases;

  public ReplicaConnectFullResyncResponse() {
  }

  public ReplicaConnectFullResyncResponse(final long lastMessageNumber, final Set<String> databases) {
    this.databases = databases;
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putLong(lastMessageNumber);
    stream.putUnsignedNumber(databases.size());
    for (String db : databases)
      stream.putString(db);
  }

  @Override
  public void fromStream(final Binary stream) {
    lastMessageNumber = stream.getLong();
    databases = new HashSet<>();
    final int fileCount = (int) stream.getUnsignedNumber();
    for (int i = 0; i < fileCount; ++i)
      databases.add(stream.getString());
  }

  public long getLastMessageNumber() {
    return lastMessageNumber;
  }

  public Set<String> getDatabases() {
    return databases;
  }

  @Override
  public String toString() {
    return "fullResync(lastMessageNumber=" + lastMessageNumber + " dbs=" + databases + ")";
  }
}
