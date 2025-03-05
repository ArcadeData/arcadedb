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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;

import java.util.logging.Level;
import java.util.stream.Collectors;

public class UpdateClusterConfiguration extends HAAbstractCommand {
  private HAServer.HACluster cluster;

  // Constructor for serialization
  public UpdateClusterConfiguration() {
  }

  public UpdateClusterConfiguration(final HAServer.HACluster cluster) {
    this.cluster = cluster;
  }

  @Override
  public HACommand execute(final HAServer server, final HAServer.ServerInfo remoteServer, final long messageNumber) {
    LogManager.instance().log(this, Level.INFO, "Updating server list=%s from `%s` ", cluster, remoteServer);
    server.setServerAddresses(cluster);
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(cluster.getServers().stream().map(HAServer.ServerInfo::toString).collect(Collectors.joining(",")));
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    cluster = new HAServer.HACluster(server.getHA().parseServerList(stream.getString()));
  }

  @Override
  public String toString() {
    return "updateClusterConfig(servers=" + cluster + ")";
  }
}
