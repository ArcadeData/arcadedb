package com.arcadedb.e2e;


import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.utility.FileUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Testcontainers
public class E2eTest {

    @Container
    private static GenericContainer arcade = new GenericContainer("arcadedata/arcadedb:latest")
            .withExposedPorts(2480, 6379, 5432)
            .withEnv("arcadedb.server.rootPassword", "playwithdata")
            .withEnv("arcadedb.server.defaultDatabases", "beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz}")
            .withEnv("arcadedb.server.plugins", "Redis:com.arcadedb.redis.RedisProtocolPlugin, MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin, Postgres:com.arcadedb.postgres.PostgresProtocolPlugin, GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin")
            .waitingFor(Wait.forListeningPort());


    @Test
    void http() throws Exception {

        String address = arcade.getHost();
        Integer port = arcade.getFirstMappedPort();
        HttpURLConnection connection = (HttpURLConnection) new URL("http://" + address + ":" + port + "/api/v1/query/beer").openConnection();

        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization",
                "Basic " + Base64.getEncoder().encodeToString("root:playwithdata".getBytes(StandardCharsets.UTF_8)));
        formatPost(connection, "sql", "select from Beer limit 1", null, new HashMap<>());
        connection.connect();

        try {
            final String response = readResponse(connection);
            System.out.println("response = " + response);
            Assertions.assertEquals(200, connection.getResponseCode());
            Assertions.assertEquals("OK", connection.getResponseMessage());
            Assertions.assertTrue(response.contains("Beer"));
        } finally {
            connection.disconnect();
        }


    }

    @Test
    void remoteSQL() {
        String address = arcade.getHost();
        Integer port = arcade.getFirstMappedPort();

        RemoteDatabase database = new RemoteDatabase(address, port, "beer", "root", "playwithdata");

        database.transaction(() -> {
            // CREATE DOCUMENT

            // RETRIEVE DOCUMENT WITH QUERY
            ResultSet result = database.query("SQL", "select from Beer limit 10");
            Assertions.assertTrue(result.hasNext());
        });

    }

    @Test
    void remoteGremlin() {
        String address = arcade.getHost();
        Integer port = arcade.getFirstMappedPort();

        RemoteDatabase database = new RemoteDatabase(address, port, "beer", "root", "playwithdata");

        database.transaction(() -> {
            // CREATE DOCUMENT

            // RETRIEVE DOCUMENT WITH QUERY
            ResultSet result = database.query("gremlin", "g.V()");
            Assertions.assertTrue(result.hasNext());
        });

    }

    @Test
    void jdbc() throws Exception {
        Class.forName("org.postgresql.Driver");

        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "playwithdata");
        props.setProperty("ssl", "false");

        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://" + arcade.getHost() + ":" + arcade.getMappedPort(5432) + "/beer", props)) {
            try (Statement st = conn.createStatement()) {

                try (java.sql.ResultSet rs = st.executeQuery("SELECT * FROM Beer limit 10")) {
                    while (rs.next()) {
                        System.out.println("First Name: " + rs.getString("name") + " - Last Name: " + rs.getString("descript"));
                    }
                }
            }
        }
    }

    protected void formatPost(final HttpURLConnection connection, final String language, final String payloadCommand, final String serializer,
                              final Map<String, Object> params) throws Exception {
        connection.setDoOutput(true);
        if (payloadCommand != null) {
            final JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("language", language);
            jsonRequest.put("command", payloadCommand);
            if (serializer != null)
                jsonRequest.put("serializer", serializer);

            if (params != null) {
                final JSONObject jsonParams = new JSONObject(params);
                jsonRequest.put("params", jsonParams);
            }

            final byte[] postData = jsonRequest.toString().getBytes(StandardCharsets.UTF_8);
            connection.setRequestProperty("Content-Length", Integer.toString(postData.length));
            try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
                wr.write(postData);
            }
        }
    }

    protected String readResponse(final HttpURLConnection connection) throws IOException {
        InputStream in = connection.getInputStream();

        String buffer = FileUtils.readStreamAsString(in, "utf8");
        return buffer.replace('\n', ' ');
    }

}
