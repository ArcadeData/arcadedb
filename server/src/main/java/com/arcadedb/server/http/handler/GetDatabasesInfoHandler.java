package com.arcadedb.server.http.handler;

import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;

import io.undertow.server.HttpServerExchange;

/**
 * Returns metadata about the available databases for the current user that the UI can display, 
 * such as classificaiton markings.
 */
public class GetDatabasesInfoHandler  extends AbstractHandler {
  public GetDatabasesInfoHandler(final HttpServer httpServer) {
    super(httpServer);
  }

    @Override
    protected ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user) throws Exception {

      JSONArray databases = new JSONArray();

        for (String databaseName : user.getAuthorizedDatabases()) {
          if (httpServer.getServer().getDatabaseNames().contains(databaseName)) {
            var db = httpServer.getServer().getDatabase(databaseName);

            if (db != null) {
                EmbeddedSchema embeddedSchema =  db.getSchema().getEmbedded();

                final JSONObject root = new JSONObject();
                root.put("name", databaseName);
                root.put("classification", embeddedSchema.getClassification());

                if (embeddedSchema.getOwner() != null) {
                 root.put("owner", embeddedSchema.getOwner());
                }
                root.put("isPublic", embeddedSchema.isPublic());

                if (embeddedSchema.getCreatedBy() != null) {
                  root.put("createdBy", embeddedSchema.getCreatedBy());
                }

                if (embeddedSchema.getCreatedDateTime() != null) {
                  root.put("createdDateTime", embeddedSchema.getCreatedDateTime());
                }

                databases.put(root);
            }
          }
        }
        
        return new ExecutionResponse(200, "{ \"result\" : " + databases + "}");
    }
}
