package com.arcadedb.server.http.handler;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.MinioRestClient;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.server.security.oidc.role.ServerAdminRole;

import io.undertow.io.IoCallback;
import io.undertow.io.Sender;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import lombok.extern.slf4j.Slf4j;

/**
 * Backup all databases (with backups enabled) to minio
 */
@Slf4j
public class PostBackupHandler implements HttpHandler {

    private final HttpServer httpServer;

    public PostBackupHandler(final HttpServer httpServer) {
        this.httpServer = httpServer;
    }

    public void handleRequest(final HttpServerExchange exchange) throws Exception {

        // Roles to check for when authorizing backups
        var anyRequiredRoles = List.of(ServerAdminRole.BACKUP_DATABASE,
                ServerAdminRole.ALL);

        // Reject unauthenticated requests
        final HeaderValues authorization = exchange.getRequestHeaders().get("Authorization");
        if (true && (authorization == null || authorization.isEmpty())) {
            exchange.setStatusCode(403);
            sendErrorResponse(exchange, 403, "No authentication was provided", null, null);
            return;
        }

        // Get user from request
        ServerSecurityUser user = null;
        if (authorization != null) {
            if (GlobalConfiguration.OIDC_AUTH.getValueAsBoolean()) {
                // TODO only allow root user basic access if JWT auth enabled

                HeaderValues authHeader = exchange.getRequestHeaders().get(Headers.AUTHORIZATION);

                if (authHeader != null && authHeader.toString().contains("Bearer ")) {

                    String encodedJwt = authHeader.get(0);
                    String decodedJwt = AbstractHandler.decodeTokenParts(encodedJwt)[1];
                    decodedJwt = decodedJwt.replaceAll(" ", "");
                    JSONObject json = new JSONObject(decodedJwt);
                    String username = json.getString("preferred_username");
                    user = httpServer.getServer().getSecurity().authenticate(username, null);
                } else {
                    exchange.setStatusCode(403);
                    sendErrorResponse(exchange, 403, "Invalid authentication was provided", null, null);
                }
            }
        }

        // Check user has required roles
        if (!httpServer.getServer().getSecurity().checkUserHasAnyServerAdminRole(user,
                anyRequiredRoles)) {
            exchange.setStatusCode(401);
            exchange.getResponseSender().send("Unauthorized to backup databases");
            return;
        }

        checkServerIsLeaderIfInHA();

        // Set header for sending SSEs back to client
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/event-stream");

        exchange.dispatch(() -> {
            try {
                backupDatabases(exchange);
            } catch (Exception e) {
                log.error("Error backing up databases", e.getMessage());
                log.debug("Exception", e);
            }
        });
    }

    /**
     * Send an event to the client over SSE, formatted for severity and timestamp
     */
    private void sendEvent(final HttpServerExchange exchange, String message) {

        // Log message to container
        if (message.contains("INFO")) {
            log.info(message);
        } else {
            log.error(message);
        }

        var enhancedMessage = String.format("%s %s\n", Instant.now().toString(), message);
        exchange.getResponseSender().send(enhancedMessage, new IoCallback() {
            @Override
            public void onComplete(HttpServerExchange exchange, Sender sender) {
                // NO-OP (keep the connection open)
            }

            @Override
            public void onException(HttpServerExchange exchange, Sender sender, IOException exception) {
                log.error("Error sending event", exception.getMessage());
                log.debug("Exception", exception);
            }
        });
    }

    private void backupDatabases(final HttpServerExchange exchange) {
        var databaseNames = httpServer.getServer().getDatabaseNames();

        int counter = 1;
        int successCounter = 0;
        int failureCounter = 0;
        int skippedCounter = 0;
        Instant start = Instant.now();

        for (String databaseName : databaseNames) {
            var database = httpServer.getServer().getDatabase(databaseName);
            if (database.getSchema().getEmbedded().shouldBackup()) {
                try {
                    backupDatabase(database, counter, databaseNames.size(), exchange);
                    successCounter++;

                } catch (Exception e) {
                    failureCounter++;
                    String event = String.format("ERROR - (%d/%d) Error backing up database %s, message: %s",
                            counter, databaseNames.size(), databaseName, e.getMessage());
                    sendEvent(exchange, event);
                    log.error("Error backing up database", e.getMessage());
                    log.debug("Exception", e);
                }
            } else {
                skippedCounter++;
                String event = String.format("INFO - (%d/%d) Skipping disabled backup of: %s", counter,
                        databaseNames.size(), database.getName());
                sendEvent(exchange, event);
            }
            counter++;
        }

        String ellapsed = formatDuration(Duration.ofMillis(Instant.now().toEpochMilli() - start.toEpochMilli()));
        String event = String.format(
                "INFO - Arcade backup complete. %d databases backed up in %s. %d skipped, %d failed",
                successCounter, ellapsed, skippedCounter, failureCounter);
        sendEvent(exchange, event);
        exchange.getResponseSender().close();
    }

    private void backupDatabase(Database database, int counter, int numDatabases, final HttpServerExchange exchange) {
        Instant dbStart = Instant.now();

        String event = String.format("INFO - (%d/%d) Starting backup of database: %s", counter,
                numDatabases, database.getName());
        sendEvent(exchange, event);

        final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmssSSS");
        String fileName = String.format("%s-backup-%s.zip", database.getName(),
                dateFormat.format(System.currentTimeMillis()));

        // Construct and execute the arcade sql command to backup the database to a
        // local file.
        String command = String.format("backup database file://%s", fileName);
        database.command("sql", command, httpServer.getServer().getConfiguration(), new HashMap<>());

        MinioRestClient.uploadBackup(database.getName(), fileName);

        // TODO delete local backup file, or clean up old backups periodically?

        Instant dbStop = Instant.now();
        String ellapsed = formatDuration(Duration.ofMillis(dbStop.toEpochMilli() - dbStart.toEpochMilli()));
        event = String.format("INFO - (%d/%d) Backup of database %s completed in %s", counter,
                numDatabases, database.getName(), ellapsed);
        sendEvent(exchange, event);
    }

    private void checkServerIsLeaderIfInHA() {
        final HAServer ha = httpServer.getServer().getHA();
        if (ha != null && !ha.isLeader())
            // NOT THE LEADER
            throw new ServerIsNotTheLeaderException("Backup of database can be executed only on the leader server",
                    ha.getLeaderName());
    }

    /**
     * Format ellapsed time as HH:MM:SS for easier readibility
     */
    private String formatDuration(Duration duration) {
        long HH = duration.toHours();
        long MM = duration.toMinutesPart();
        long SS = duration.toSecondsPart();
        return String.format("%02d:%02d:%02d", HH, MM, SS);
    }

    private void sendErrorResponse(final HttpServerExchange exchange, final int code, final String errorMessage,
            final Throwable e, final String exceptionArgs) {
        if (!exchange.isResponseStarted())
            exchange.setStatusCode(code);
        exchange.getResponseSender().send(errorMessage);
    }
}
