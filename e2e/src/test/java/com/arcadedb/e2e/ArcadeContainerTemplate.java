package com.arcadedb.e2e;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public abstract class ArcadeContainerTemplate {

    static final GenericContainer arcade;

    static {
        arcade = new GenericContainer("arcadedata/arcadedb:latest")
                .withExposedPorts(2480, 6379, 5432, 8182)
                .withEnv("arcadedb.server.rootPassword", "playwithdata")
                .withEnv("arcadedb.server.defaultDatabases", "beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz}")
                .withEnv("arcadedb.server.plugins", "Redis:com.arcadedb.redis.RedisProtocolPlugin, " +
                        "MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin, " +
                        "Postgres:com.arcadedb.postgres.PostgresProtocolPlugin, " +
                        "GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin")
                .waitingFor(Wait.forListeningPort());
        arcade.start();
    }


    String address = arcade.getHost();

    int httpPort = arcade.getMappedPort(2480);

    int redisPort = arcade.getMappedPort(6379);

    int pgsqlPort = arcade.getMappedPort(5432);

    int gremlinPort = arcade.getMappedPort(8182);
}
