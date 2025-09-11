package com.arcadedb.e2e;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.grpc.RemoteGrpcDatabase;
import com.arcadedb.remote.grpc.RemoteGrpcServer;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteGrpcDatabaseTest extends ArcadeContainerTemplate {

  private RemoteGrpcDatabase database;
  private RemoteGrpcServer   server;

  @BeforeEach
  void setUp() {

    server = new RemoteGrpcServer(host, grpcPort, "root", "playwithdata", true, List.of());
    database = new RemoteGrpcDatabase(server, host, grpcPort, httpPort, "beer", "root", "playwithdata");
    // ENLARGE THE TIMEOUT TO PASS THESE TESTS ON CI (GITHUB ACTIONS)
    database.setTimeout(60_000);
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.close();
  }

  @Test
  void simpleSQLQuery() {
    database.transaction(() -> {
      final ResultSet result = database.query("SQL", "select * from Beer limit 10");
      assertThat(CollectionUtils.countEntries(result)).isEqualTo(10);
    }, true, 10);
  }

  @Test
  @Disabled("Gremlin not supported yet")
  void simpleGremlinQuery() {
    database.transaction(() -> {
      final ResultSet result = database.query("gremlin", "g.V().limit(10)");
      assertThat(CollectionUtils.countEntries(result)).isEqualTo(10);
    }, false, 10);
  }

  @Test
  @Disabled("Cypher not supported yet")
  void simpleCypherQuery() {
    database.transaction(() -> {
      final ResultSet result = database.query("cypher", "MATCH(p:Beer) RETURN * LIMIT 10");
      assertThat(CollectionUtils.countEntries(result)).isEqualTo(10);
    }, false, 10);
  }
}
