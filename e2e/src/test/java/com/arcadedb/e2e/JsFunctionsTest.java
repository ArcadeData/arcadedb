package com.arcadedb.e2e;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsFunctionsTest extends ArcadeContainerTemplate {
  private RemoteDatabase database;

  @BeforeEach
  void setUp() {
    database = new RemoteDatabase(host, httpPort, "beer", "root", "playwithdata");
    // ENLARGE THE TIMEOUT TO PASS THESE TESTS ON CI (GITHUB ACTIONS)
    database.setTimeout(60_000);
    database.command("sql", "define function math.sum \"return a + b\" parameters [a,b] language js");

  }

  @AfterEach
  void tearDown() {
    database.command("sql", "delete function math.sum");
    if (database != null) database.close();
  }

  @Test
  void jsMathSum() {
    final ResultSet result = database.command("sql", "select `math.sum`(?,?) as result", 3, 5);
    assertThat(result.next().<Integer>getProperty("result")).isEqualTo(8);

  }

}
