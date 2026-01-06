package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiplicationOverflowTest extends TestHelper {

  @Test
  public void testMultiplicationOverflowToLong() {
    database.transaction(() -> {
      // Test case that caused the original bug: 1000*3600*24*365
      final ResultSet result1 = database.query("sql", "SELECT 1000*3600*24*365 as value");
      assertThat(result1.hasNext()).isTrue();
      final Result r1 = result1.next();
      final Object value1 = r1.getProperty("value");
      System.out.println("1000*3600*24*365 = " + value1);
      assertThat(value1).isInstanceOf(Long.class);
      assertThat(((Number) value1).longValue()).isEqualTo(31_536_000_000L);
      result1.close();

      // Test multiplication that stays within Integer range
      final ResultSet result2 = database.query("sql", "SELECT 1000*1000 as value");
      assertThat(result2.hasNext()).isTrue();
      final Result r2 = result2.next();
      final Object value2 = r2.getProperty("value");
      System.out.println("1000*1000 = " + value2 + " (type: " + value2.getClass().getSimpleName() + ")");
      assertThat(value2).isInstanceOf(Integer.class);
      assertThat(((Number) value2).intValue()).isEqualTo(1_000_000);
      result2.close();

      // Test multiplication at the edge of Integer.MAX_VALUE
      final ResultSet result3 = database.query("sql", "SELECT 50000*50000 as value");  // = 2,500,000,000 > Integer.MAX_VALUE
      assertThat(result3.hasNext()).isTrue();
      final Result r3 = result3.next();
      final Object value3 = r3.getProperty("value");
      System.out.println("50000*50000 = " + value3 + " (type: " + value3.getClass().getSimpleName() + ")");
      assertThat(value3).isInstanceOf(Long.class);
      assertThat(((Number) value3).longValue()).isEqualTo(2_500_000_000L);
      result3.close();

      // Test negative multiplication overflow
      final ResultSet result4 = database.query("sql", "SELECT (-1000)*3600*24*365 as value");
      assertThat(result4.hasNext()).isTrue();
      final Result r4 = result4.next();
      final Object value4 = r4.getProperty("value");
      System.out.println("(-1000)*3600*24*365 = " + value4 + " (type: " + value4.getClass().getSimpleName() + ")");
      assertThat(value4).isInstanceOf(Long.class);
      assertThat(((Number) value4).longValue()).isEqualTo(-31_536_000_000L);
      result4.close();

      // Test the division expression from the original bug report
      final ResultSet result5 = database.query("sql",
          "SELECT $val2/$val1 as ratio LET $val1 = 1/1000/3600/24/365, $val2 = 1/(1000*3600*24*365)");
      assertThat(result5.hasNext()).isTrue();
      final Result r5 = result5.next();
      final Object ratio = r5.getProperty("ratio");
      System.out.println("Ratio $val2/$val1 = " + ratio);
      assertThat(ratio).isInstanceOf(Number.class);
      assertThat(((Number) ratio).doubleValue()).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.0001));
      result5.close();
    });
  }

  @Test
  public void testMultiplicationWithMixedTypes() {
    database.transaction(() -> {
      // Test Integer * Long
      final ResultSet result1 = database.query("sql", "SELECT 1000 * 1000000000L as value");
      assertThat(result1.hasNext()).isTrue();
      final Result r1 = result1.next();
      final Object value1 = r1.getProperty("value");
      System.out.println("1000 * 1000000000L = " + value1 + " (type: " + value1.getClass().getSimpleName() + ")");
      assertThat(value1).isInstanceOf(Long.class);
      assertThat(((Number) value1).longValue()).isEqualTo(1_000_000_000_000L);
      result1.close();

      // Test Integer * Double
      final ResultSet result2 = database.query("sql", "SELECT 1000 * 3600.5 as value");
      assertThat(result2.hasNext()).isTrue();
      final Result r2 = result2.next();
      final Object value2 = r2.getProperty("value");
      System.out.println("1000 * 3600.5 = " + value2 + " (type: " + value2.getClass().getSimpleName() + ")");
      assertThat(value2).isInstanceOf(Number.class);
      assertThat(((Number) value2).doubleValue()).isCloseTo(3_600_500.0, org.assertj.core.data.Offset.offset(0.001));
      result2.close();
    });
  }
}
