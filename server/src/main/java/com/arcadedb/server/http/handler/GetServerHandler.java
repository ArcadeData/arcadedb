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
import com.arcadedb.server.ServerMetrics;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import io.undertow.server.HttpServerExchange;

import java.util.*;
import java.util.logging.*;

public class GetServerHandler extends AbstractHandler {
  public GetServerHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final ServerSecurityUser user) {
    exchange.setStatusCode(200);

    final JSONObject response = createResult(user, null);

    final String mode = getQueryParameter(exchange, "mode", "default");

    final JSONObject metricsJSON = new JSONObject();
    response.put("metrics", metricsJSON);

    final JSONObject timersJSON = new JSONObject();
    metricsJSON.put("timers", timersJSON);

    final ServerMetrics metrics = httpServer.getServer().getServerMetrics();
    for (Map.Entry<String, Timer> entry : metrics.getTimers().entrySet()) {
      final Timer timer = entry.getValue();
      final Snapshot snapshot = timer.getSnapshot();

      timersJSON.put(entry.getKey(), new JSONObject().put("count", timer.getCount())//
          .put("oneMinRate", timer.getOneMinuteRate())//
          .put("min", snapshot.getMin())//
          .put("max", snapshot.getMax())//
          .put("mean", snapshot.getMean())//
          .put("perc99", snapshot.get99thPercentile())//
      );
    }

    final JSONObject metersJSON = new JSONObject();
    metricsJSON.put("meters", metersJSON);

    for (Map.Entry<String, Meter> entry : metrics.getMeters().entrySet()) {
      final Meter meter = entry.getValue();

      metersJSON.put(entry.getKey(), new JSONObject().put("count", meter.getCount())//
          .put("oneMinRate", meter.getOneMinuteRate())//
      );
    }

    if ("cluster".equals(mode)) {
      final HAServer ha = httpServer.getServer().getHA();
      if (ha != null) {
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

      }
    }
    exchange.getResponseSender().send(response.toString());
  }
}
