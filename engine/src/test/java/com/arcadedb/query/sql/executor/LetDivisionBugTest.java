package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LetDivisionBugTest extends TestHelper {

  @Test
  public void testLetDivisionEvaluation() {
    database.transaction(() -> {
      // Test the reported issue
      final ResultSet result1 = database.query("sql",
          "SELECT $val2/$val1 as ratio LET $val1 = 1/1000/3600/24/365, $val2 = 1/(1000*3600*24*365)");

      assertThat(result1.hasNext()).isTrue();
      final Result r1 = result1.next();
      final Object ratio = r1.getProperty("ratio");
      System.out.println("Ratio: " + ratio);
      System.out.println("Ratio type: " + (ratio != null ? ratio.getClass().getName() : "null"));

      // The ratio should be 1.0 since both expressions are mathematically equivalent
      assertThat(ratio).isNotNull();
      if (ratio instanceof Number) {
        double ratioValue = ((Number) ratio).doubleValue();
        System.out.println("Ratio value: " + ratioValue);
        assertThat(ratioValue).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.0001));
      }
      result1.close();

      // Test the individual values
      final ResultSet result2 = database.query("sql",
          "SELECT $val1, $val2 LET $val1 = 1/1000/3600/24/365, $val2 = 1/(1000*3600*24*365)");

      assertThat(result2.hasNext()).isTrue();
      final Result r2 = result2.next();
      final Object val1 = r2.getProperty("$val1");
      final Object val2 = r2.getProperty("$val2");
      System.out.println("$val1: " + val1);
      System.out.println("$val2: " + val2);

      if (val1 instanceof Number && val2 instanceof Number) {
        double v1 = ((Number) val1).doubleValue();
        double v2 = ((Number) val2).doubleValue();
        System.out.println("$val1 (double): " + v1);
        System.out.println("$val2 (double): " + v2);
        System.out.println("Calculated ratio: " + (v2 / v1));

        // Both should be approximately 3.17E-11
        assertThat(v1).isCloseTo(v2, org.assertj.core.data.Percentage.withPercentage(0.01));
      }
      result2.close();
    });
  }

  @Test
  public void testSimpleDivision() {
    database.transaction(() -> {
      // Test simple division order
      final ResultSet result = database.query("sql",
          "SELECT 1/1000/3600/24/365 as val1, 1/(1000*3600*24*365) as val2");

      assertThat(result.hasNext()).isTrue();
      final Result r = result.next();
      final Object val1 = r.getProperty("val1");
      final Object val2 = r.getProperty("val2");
      System.out.println("Direct val1: " + val1);
      System.out.println("Direct val2: " + val2);

      if (val1 instanceof Number && val2 instanceof Number) {
        double v1 = ((Number) val1).doubleValue();
        double v2 = ((Number) val2).doubleValue();
        System.out.println("Direct val1 (double): " + v1);
        System.out.println("Direct val2 (double): " + v2);
      }
      result.close();

      // Test intermediate calculations to understand the issue
      final ResultSet result2 = database.query("sql",
          "SELECT 1000*3600 as step1, 1000*3600*24 as step2, 1000*3600*24*365 as step3");

      assertThat(result2.hasNext()).isTrue();
      final Result r2 = result2.next();
      System.out.println("step1 (1000*3600): " + r2.getProperty("step1"));
      System.out.println("step2 (1000*3600*24): " + r2.getProperty("step2"));
      System.out.println("step3 (1000*3600*24*365): " + r2.getProperty("step3"));
      result2.close();

      // Test what 1/step3 gives
      final ResultSet result3 = database.query("sql",
          "SELECT 1/(1000*3600) as div1, 1/(1000*3600*24) as div2, 1/(1000*3600*24*365) as div3");

      assertThat(result3.hasNext()).isTrue();
      final Result r3 = result3.next();
      System.out.println("div1 (1/(1000*3600)): " + r3.getProperty("div1"));
      System.out.println("div2 (1/(1000*3600*24)): " + r3.getProperty("div2"));
      System.out.println("div3 (1/(1000*3600*24*365)): " + r3.getProperty("div3"));
      result3.close();

      // Now assert that val1 and val2 should be equal
      if (val1 instanceof Number && val2 instanceof Number) {
        double v1 = ((Number) val1).doubleValue();
        double v2 = ((Number) val2).doubleValue();
        assertThat(v1).isCloseTo(v2, org.assertj.core.data.Percentage.withPercentage(0.01));
      }
    });
  }
}
