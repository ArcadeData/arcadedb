package com.arcadedb.server.http.handler;

import java.util.Deque;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.DataFabricRestClient;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;

import io.undertow.server.HttpServerExchange;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetHistoryHandler extends AbstractHandler {
    public GetHistoryHandler(final HttpServer httpServer) {
        super(httpServer);
    }

    @Override
    protected ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user) {
        try {
            // Grab all params from request path
            final Deque<String> databaseParam = exchange.getQueryParameters().get("database");
            String database = databaseParam.isEmpty() ? null : databaseParam.getFirst().trim();
            if (database != null && database.isEmpty()) {
                database = null;
            }

            if (database == null) {
                return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is null\"}");
            }

            final Deque<String> entityTypeParam = exchange.getQueryParameters().get("entityType");
            String entityType = entityTypeParam.isEmpty() ? null : entityTypeParam.getFirst().trim();
            if (entityType != null && entityType.isEmpty()) {
                entityType = null;
            }

            if (entityType == null) {
                return new ExecutionResponse(400, "{ \"error\" : \"EntityType parameter is null\"}");
            }

            final Deque<String> ridParam = exchange.getQueryParameters().get("rid");
            String rid = ridParam.isEmpty() ? null : ridParam.getFirst().trim();
            if (rid != null && rid.isEmpty()) {
                rid = null;
            }

            if (rid == null) {
                return new ExecutionResponse(400, "{ \"error\" : \"Rid parameter is null\"}");
            }

            // Replace a url escaped colon with the actual colon.
            rid = rid.replace("%3a", ":");
            rid = rid.replace("%3A", ":");

            // Replace the leading hash in the RID. The caller putting the hash in the RID
            // will mess up with REST request pathing
            rid = "#" + rid;

            // Make REST request to lakehouse
            String url = "http://df-lakehouse/api/v1/lakehouse/schemas/arcadedbcdc_" + database;
            String query = String.format(
                    "SELECT CAST(MAP_FROM_ENTRIES(ARRAY[('eventId', eventid ), ('timestamp ', CAST(from_unixtime(CAST(timestamp AS BIGINT)/1000) AS VARCHAR)), "
                            +
                            " ('entityId', entityid ), ('user', username), ('eventType', eventType), ('entity', eventpayload)]) AS JSON) as history "
                            +
                            "FROM arcadedbcdc_%s.admin_%s WHERE entityname = '%s' AND entityid = '%s' ORDER BY timestamp DESC",
                    database, database, entityType, rid);
            JSONObject body = new JSONObject();
            body.put("sql", query);

            String response = DataFabricRestClient.postAuthenticatedAndGetResponse(url, body.toString());

            if (response != null) {
                var jo = new JSONObject(response);
                var ja = jo.getJSONArray("data");
                var newArray = new JSONArray();

                if (ja.length() == 0) {
                    return new ExecutionResponse(404, "{ \"error\" : \"NotFound\"}");
                }

                for (int i = 0; i < ja.length(); i++) {
                    newArray.put(new JSONObject(ja.getString(i)).getJSONObject("history"));
                }

                return new ExecutionResponse(200, "{ \"result\" : " + newArray + "}");
            } else {
                return new ExecutionResponse(400, "{ \"error\" : \"bad request\"}");
            }
        } catch (Exception e) {
            log.error("Error serving history request.", e.getMessage());
            log.debug("Exception", e);
        }

        return new ExecutionResponse(400, "{ \"error\" : \"bad request\"}");
    }
}
