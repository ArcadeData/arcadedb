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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.Profiler;
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

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

public class GetServerHandler extends AbstractHandler {
  public GetServerHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public void execute(final HttpServerExchange exchange, final ServerSecurityUser user) {
    exchange.setStatusCode(200);

    final JSONObject response = createResult(user, null);

    final String mode = getQueryParameter(exchange, "mode", "default");

    if ("basic".equals(mode)) {
      // JUST RETURN BASIC SERVER DATA
    } else if ("default".equals(mode)) {
      exportMetrics(response);
      exportSettings(response);
    } else if ("cluster".equals(mode)) {
      exportCluster(exchange, response);
    }
    exchange.getResponseSender().send(response.toString());
  }

  private void exportCluster(final HttpServerExchange exchange, final JSONObject response) {
    final HAServer ha = httpServer.getServer().getHA();
    if (ha != null) {
      final JSONObject haJSON = new JSONObject();
      response.put("ha", haJSON);

      haJSON.put("clusterName", ha.getClusterName());
      haJSON.put("leader", ha.getLeaderName());
      haJSON.put("electionStatus", ha.getElectionStatus().toString());
      haJSON.put("network", ha.getStats());

      if (!ha.isLeader()) {
        // ASK TO THE LEADER THE NETWORK COMPOSITION
        try {
          HttpURLConnection connection = (HttpURLConnection) new URL(
              "http://" + ha.getLeader().getRemoteHTTPAddress() + "/api/v1/server?mode=cluster").openConnection();

          try {
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization", exchange.getRequestHeaders().get("Authorization").getFirst());
            connection.connect();

            JSONObject leaderResponse = new JSONObject(readResponse(connection));
            final JSONObject network = leaderResponse.getJSONObject("ha").getJSONObject("network");
            haJSON.getJSONObject("network").put("replicas", network.getJSONArray("replicas"));

          } catch (Exception e) {
            throw new RuntimeException(e);
          } finally {
            connection.disconnect();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      final JSONArray databases = new JSONArray();

      for (String dbName : httpServer.getServer().getDatabaseNames()) {
        final ServerDatabase db = (ServerDatabase) httpServer.getServer().getDatabase(dbName);
        final ReplicatedDatabase rdb = ((ReplicatedDatabase) db.getWrappedDatabaseInstance());

        final JSONObject databaseJSON = new JSONObject();
        databaseJSON.put("name", rdb.getName());
        databaseJSON.put("quorum", rdb.getQuorum());
        databases.put(databaseJSON);
      }

      haJSON.put("databases", databases);

      final String leaderServer = ha.isLeader() ? ha.getServer().getHttpServer().getListeningAddress() : ha.getLeader().getRemoteHTTPAddress();
      final String replicaServers = ha.getReplicaServersHTTPAddressesList();

      haJSON.put("leaderAddress", leaderServer);
      haJSON.put("replicaAddresses", replicaServers);

      LogManager.instance().log(this, Level.FINE, "Returning configuration leaderServer=%s replicaServers=[%s]", leaderServer, replicaServers);
    }
  }

  private void exportMetrics(final JSONObject response) {
    final JSONObject metricsJSON = new JSONObject();
    response.put("metrics", metricsJSON);

    metricsJSON.put("profiler", Profiler.INSTANCE.toJSON());

    final JSONObject timersJSON = new JSONObject();
    metricsJSON.put("timers", timersJSON);

    final ServerMetrics metrics = httpServer.getServer().getServerMetrics();
    for (Map.Entry<String, Timer> entry : metrics.getTimers().entrySet()) {
      final Timer timer = entry.getValue();
      final Snapshot snapshot = timer.getSnapshot();

      timersJSON.put(entry.getKey(), new JSONObject().put("count", timer.getCount())//
          .put("oneMinRate", timer.getOneMinuteRate())//
          .put("mean", snapshot.getMean() / 1000000)//
          .put("perc99", snapshot.get99thPercentile() / 1000000)//
          .put("min", snapshot.getMin() / 1000000)//
          .put("max", snapshot.getMax() / 1000000)//
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
  }

  private void exportSettings(final JSONObject response) {
    final ContextConfiguration srvCfg = httpServer.getServer().getConfiguration();
    final Set<String> contextKeys = srvCfg.getContextKeys();

    final List<Map<String, Object>> settings = new ArrayList<>();
    for (GlobalConfiguration cfg : GlobalConfiguration.values()) {
      if (cfg.getScope() != GlobalConfiguration.SCOPE.DATABASE) {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("key", cfg.getKey());
        map.put("value", convertValue(cfg.getKey(), cfg.getValue()));
        map.put("description", cfg.getDescription());
        map.put("overridden", contextKeys.contains(cfg.getKey()));
        map.put("default", convertValue(cfg.getKey(), cfg.getDefValue()));
        settings.add(map);
      }
    }
    response.put("settings", settings);
  }

  private String readResponse(final HttpURLConnection connection) throws IOException {
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    final InputStream in = connection.getInputStream();
    final Scanner scanner = new Scanner(in);

    final StringBuilder buffer = new StringBuilder();

    while (scanner.hasNext()) {
      buffer.append(scanner.next().replace('\n', ' '));
    }

    return buffer.toString();
  }

  private Object convertValue(final String key, Object value) {
    if (key.toLowerCase().contains("password"))
      // MASK SENSITIVE DATA
      value = "*****";

    if (value instanceof Class)
      value = ((Class<?>) value).getName();

    return value;
  }
}
