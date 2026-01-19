package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class OrderByRidTest extends TestHelper {

  @Test
  void testOrderByRidDesc() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE TestVertex");

      // Insert several vertices
      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO TestVertex SET id = " + i);
      }

      // Query with ORDER BY @rid DESC
      ResultSet rs = database.query("sql", "select from TestVertex order by @rid desc");

      List<RID> rids = new ArrayList<>();
      while (rs.hasNext()) {
        var result = rs.next();
        result.getIdentity().ifPresent(rids::add);
      }
      rs.close();

      System.out.println("Total results: " + rids.size());
      assertThat(rids).hasSize(10);

      // Verify they're in descending order
      for (int i = 0; i < rids.size() - 1; i++) {
        RID current = rids.get(i);
        RID next = rids.get(i + 1);
        System.out.println("RID[" + i + "]: " + current + " position=" + current.getPosition());
        assertThat(current.getPosition()).isGreaterThanOrEqualTo(next.getPosition());
      }

      // First result should have the highest position
      RID first = rids.get(0);
      RID last = rids.get(rids.size() - 1);
      System.out.println("First RID: " + first + " position=" + first.getPosition());
      System.out.println("Last RID: " + last + " position=" + last.getPosition());
      assertThat(first.getPosition()).isGreaterThanOrEqualTo(last.getPosition());
    });
  }

  @Test
  void testOrderByRidAsc() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE TestVertex2");

      for (int i = 0; i < 5; i++) {
        database.command("sql", "INSERT INTO TestVertex2 SET id = " + i);
      }

      ResultSet rs = database.query("sql", "select from TestVertex2 order by @rid asc");

      List<RID> rids = new ArrayList<>();
      while (rs.hasNext()) {
        var result = rs.next();
        result.getIdentity().ifPresent(rids::add);
      }
      rs.close();

      assertThat(rids).hasSize(5);

      // Verify they're in ascending order
      for (int i = 0; i < rids.size() - 1; i++) {
        RID current = rids.get(i);
        RID next = rids.get(i + 1);
        assertThat(current.getPosition()).isLessThanOrEqualTo(next.getPosition());
      }
    });
  }
}
