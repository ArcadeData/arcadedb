package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #1842: SQL: Setting "ORDER BY" Direction via Parameter
 * <p>
 * This test verifies that the ORDER BY direction can be set via a parameter using
 * boolean values (true for ASC, false for DESC) as an alternative to the ASC/DESC keywords.
 * </p>
 */
public class OrderByDirectionParameterTest extends TestHelper {

  @Test
  void testOrderByDirectionWithTrueForAsc() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestOrderParam IF NOT EXISTS");
      database.command("sql", "DELETE FROM TestOrderParam");

      // Insert test data
      for (int i = 0; i < 5; i++) {
        database.command("sql", "INSERT INTO TestOrderParam SET num = " + i);
      }

      // Query with ORDER BY using true (should be ASC)
      final ResultSet rs = database.query("sql", "SELECT num FROM TestOrderParam ORDER BY num true");

      final List<Integer> nums = new ArrayList<>();
      while (rs.hasNext()) {
        final Result result = rs.next();
        nums.add(result.getProperty("num"));
      }
      rs.close();

      assertThat(nums).hasSize(5);
      // Verify ascending order
      for (int i = 0; i < nums.size() - 1; i++) {
        assertThat(nums.get(i)).isLessThanOrEqualTo(nums.get(i + 1));
      }
    });
  }

  @Test
  void testOrderByDirectionWithFalseForDesc() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestOrderParam2 IF NOT EXISTS");
      database.command("sql", "DELETE FROM TestOrderParam2");

      // Insert test data
      for (int i = 0; i < 5; i++) {
        database.command("sql", "INSERT INTO TestOrderParam2 SET num = " + i);
      }

      // Query with ORDER BY using false (should be DESC)
      final ResultSet rs = database.query("sql", "SELECT num FROM TestOrderParam2 ORDER BY num false");

      final List<Integer> nums = new ArrayList<>();
      while (rs.hasNext()) {
        final Result result = rs.next();
        nums.add(result.getProperty("num"));
      }
      rs.close();

      assertThat(nums).hasSize(5);
      // Verify descending order
      for (int i = 0; i < nums.size() - 1; i++) {
        assertThat(nums.get(i)).isGreaterThanOrEqualTo(nums.get(i + 1));
      }
    });
  }

  @Test
  void testOrderByDirectionWithBooleanParameter() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestOrderParam3 IF NOT EXISTS");
      database.command("sql", "DELETE FROM TestOrderParam3");

      // Insert test data
      for (int i = 0; i < 5; i++) {
        database.command("sql", "INSERT INTO TestOrderParam3 SET num = " + i);
      }

      // Test with parameter true (ASC)
      Map<String, Object> params = new HashMap<>();
      params.put("dir", true);

      ResultSet rs = database.query("sql", "SELECT num FROM TestOrderParam3 ORDER BY num :dir", params);

      List<Integer> nums = new ArrayList<>();
      while (rs.hasNext()) {
        final Result result = rs.next();
        nums.add(result.getProperty("num"));
      }
      rs.close();

      assertThat(nums).hasSize(5);
      // Verify ascending order
      for (int i = 0; i < nums.size() - 1; i++) {
        assertThat(nums.get(i)).isLessThanOrEqualTo(nums.get(i + 1));
      }

      // Test with parameter false (DESC)
      params.put("dir", false);

      rs = database.query("sql", "SELECT num FROM TestOrderParam3 ORDER BY num :dir", params);

      nums = new ArrayList<>();
      while (rs.hasNext()) {
        final Result result = rs.next();
        nums.add(result.getProperty("num"));
      }
      rs.close();

      assertThat(nums).hasSize(5);
      // Verify descending order
      for (int i = 0; i < nums.size() - 1; i++) {
        assertThat(nums.get(i)).isGreaterThanOrEqualTo(nums.get(i + 1));
      }
    });
  }

  @Test
  void testOrderByDirectionWithStringParameter() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestOrderParam5 IF NOT EXISTS");
      database.command("sql", "DELETE FROM TestOrderParam5");

      // Insert test data
      for (int i = 0; i < 5; i++) {
        database.command("sql", "INSERT INTO TestOrderParam5 SET num = " + i);
      }

      // Test with string parameter "ASC"
      Map<String, Object> params = new HashMap<>();
      params.put("dir", "ASC");

      ResultSet rs = database.query("sql", "SELECT num FROM TestOrderParam5 ORDER BY num :dir", params);

      List<Integer> nums = new ArrayList<>();
      while (rs.hasNext()) {
        final Result result = rs.next();
        nums.add(result.getProperty("num"));
      }
      rs.close();

      assertThat(nums).hasSize(5);
      // Verify ascending order
      for (int i = 0; i < nums.size() - 1; i++) {
        assertThat(nums.get(i)).isLessThanOrEqualTo(nums.get(i + 1));
      }

      // Test with string parameter "DESC"
      params.put("dir", "DESC");

      rs = database.query("sql", "SELECT num FROM TestOrderParam5 ORDER BY num :dir", params);

      nums = new ArrayList<>();
      while (rs.hasNext()) {
        final Result result = rs.next();
        nums.add(result.getProperty("num"));
      }
      rs.close();

      assertThat(nums).hasSize(5);
      // Verify descending order
      for (int i = 0; i < nums.size() - 1; i++) {
        assertThat(nums.get(i)).isGreaterThanOrEqualTo(nums.get(i + 1));
      }

      // Test with lowercase string parameter "desc"
      params.put("dir", "desc");

      rs = database.query("sql", "SELECT num FROM TestOrderParam5 ORDER BY num :dir", params);

      nums = new ArrayList<>();
      while (rs.hasNext()) {
        final Result result = rs.next();
        nums.add(result.getProperty("num"));
      }
      rs.close();

      assertThat(nums).hasSize(5);
      // Verify descending order
      for (int i = 0; i < nums.size() - 1; i++) {
        assertThat(nums.get(i)).isGreaterThanOrEqualTo(nums.get(i + 1));
      }
    });
  }

  @Test
  void testMultipleOrderByWithMixedDirections() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestOrderParam6 IF NOT EXISTS");
      database.command("sql", "DELETE FROM TestOrderParam6");

      // Insert test data with two fields
      database.command("sql", "INSERT INTO TestOrderParam6 SET category = 'A', num = 3");
      database.command("sql", "INSERT INTO TestOrderParam6 SET category = 'A', num = 1");
      database.command("sql", "INSERT INTO TestOrderParam6 SET category = 'B', num = 2");
      database.command("sql", "INSERT INTO TestOrderParam6 SET category = 'B', num = 4");

      // Test with mixed direction: category ASC (true), num DESC (false)
      Map<String, Object> params = new HashMap<>();
      params.put("catDir", true);
      params.put("numDir", false);

      ResultSet rs = database.query("sql", "SELECT category, num FROM TestOrderParam6 ORDER BY category :catDir, num :numDir", params);

      List<String> categories = new ArrayList<>();
      List<Integer> nums = new ArrayList<>();
      while (rs.hasNext()) {
        final Result result = rs.next();
        categories.add(result.getProperty("category"));
        nums.add(result.getProperty("num"));
      }
      rs.close();

      assertThat(categories).containsExactly("A", "A", "B", "B");
      assertThat(nums).containsExactly(3, 1, 4, 2);
    });
  }

  @Test
  void testOriginalAscDescKeywordsStillWork() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestOrderParam4 IF NOT EXISTS");
      database.command("sql", "DELETE FROM TestOrderParam4");

      // Insert test data
      for (int i = 0; i < 5; i++) {
        database.command("sql", "INSERT INTO TestOrderParam4 SET num = " + i);
      }

      // Test ASC keyword
      ResultSet rs = database.query("sql", "SELECT num FROM TestOrderParam4 ORDER BY num ASC");

      List<Integer> nums = new ArrayList<>();
      while (rs.hasNext()) {
        final Result result = rs.next();
        nums.add(result.getProperty("num"));
      }
      rs.close();

      assertThat(nums).hasSize(5);
      for (int i = 0; i < nums.size() - 1; i++) {
        assertThat(nums.get(i)).isLessThanOrEqualTo(nums.get(i + 1));
      }

      // Test DESC keyword
      rs = database.query("sql", "SELECT num FROM TestOrderParam4 ORDER BY num DESC");

      nums = new ArrayList<>();
      while (rs.hasNext()) {
        final Result result = rs.next();
        nums.add(result.getProperty("num"));
      }
      rs.close();

      assertThat(nums).hasSize(5);
      for (int i = 0; i < nums.size() - 1; i++) {
        assertThat(nums.get(i)).isGreaterThanOrEqualTo(nums.get(i + 1));
      }
    });
  }
}
