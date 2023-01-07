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
package com.arcadedb.server.http.handler;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.logging.*;

public class GetServersHandler extends AbstractHandler {
  public GetServersHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final ServerSecurityUser user) {
    exchange.setStatusCode(200);

    final HAServer ha = httpServer.getServer().getHA();
    if (ha == null) {
      exchange.getResponseSender().send("{}");
    } else {
      final JSONObject response = createResult(user, null);

      final JSONObject haJSON = new JSONObject();
      response.put("ha", haJSON);

      haJSON.put("clusterName", ha.getClusterName());
      haJSON.put("onlineReplicas", ha.getOnlineReplicas());
      haJSON.put("electionStatus", ha.getElectionStatus().toString());

      haJSON.put("network", ha.getStats());

      final JSONArray databases = new JSONArray();
      haJSON.put("databases", databases);

      for (String dbName : httpServer.getServer().getDatabaseNames()) {
        final ServerDatabase db = (ServerDatabase) httpServer.getServer().getDatabase(dbName);
        final ReplicatedDatabase rdb = ((ReplicatedDatabase) db.getWrappedDatabaseInstance());

        final JSONObject databaseJSON = new JSONObject();
        databaseJSON.put("name", rdb.getName());
        databaseJSON.put("quorum", rdb.getQuorum());
        databases.put(databaseJSON);
      }

      final String leaderServer = ha.isLeader() ? ha.getServer().getHttpServer().getListeningAddress() : ha.getLeader().getRemoteHTTPAddress();
      final String replicaServers = ha.getReplicaServersHTTPAddressesList();
      LogManager.instance().log(this, Level.INFO, "Returning configuration leaderServer=%s replicaServers=[%s]", leaderServer, replicaServers);

      exchange.getResponseSender().send(response.toString());
    }
  }
}
