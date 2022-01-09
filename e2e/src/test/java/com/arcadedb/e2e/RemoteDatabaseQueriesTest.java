package com.arcadedb.e2e;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteDatabaseQueriesTest extends ArcadeContainerTemplate {

    private RemoteDatabase database;

    @BeforeEach
    void setUp() {
        database = new RemoteDatabase(address, httpPort, "beer", "root", "playwithdata");
        database.setTimeout(10000);
    }

    @AfterEach
    void tearDown() {
        database.close();
    }

    @Test
    void simpleSQLQuery() {
        database.transaction(() -> {
            ResultSet result = database.query("SQL", "select from Beer limit 10");
            assertThat(result.countEntries()).isEqualTo(10);
        });

    }

    @Test
    void simpleGremlinQuery() {
        database.transaction(() -> {
            ResultSet result = database.query("gremlin", "g.V().limit(10)");
            assertThat(result.countEntries()).isEqualTo(10);
        });
    }

}
