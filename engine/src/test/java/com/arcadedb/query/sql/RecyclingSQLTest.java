package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RecyclingSQLTest extends TestHelper {

  @Test
  void testOrderByRidDesc() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE TestV");
      database.command("sql", "INSERT INTO TestV SET id = 1");
      database.command("sql", "INSERT INTO TestV SET id = 2");
      database.command("sql", "INSERT INTO TestV SET id = 3");

      ResultSet rs = database.query("sql", "select from TestV order by @rid desc");
      assertThat(rs.hasNext()).isTrue();
      var first = rs.next();
      //System.out.println("First result: " + first);
      assertThat(first).isNotNull();
      rs.close();
    });
  }

  @Test
  void testDeleteFrom() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE TestV2");
      database.command("sql", "INSERT INTO TestV2 SET id = 1");
      database.command("sql", "INSERT INTO TestV2 SET id = 2");

      long beforeCount = database.countType("TestV2", true);
      assertThat(beforeCount).isEqualTo(2);

      database.command("sql", "delete from TestV2");

      long afterCount = database.countType("TestV2", true);
      assertThat(afterCount).isEqualTo(0);
    });
  }
}
