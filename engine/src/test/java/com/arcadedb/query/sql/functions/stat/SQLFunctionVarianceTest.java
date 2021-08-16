package com.arcadedb.query.sql.functions.stat;

import com.arcadedb.query.sql.function.stat.SQLFunctionVariance;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SQLFunctionVarianceTest {

  private SQLFunctionVariance variance;

  @BeforeEach
  public void setup() {
    variance = new SQLFunctionVariance();
  }

  @Test
  public void testEmpty() {
    Object result = variance.getResult();
    Assertions.assertNull(result);
  }

  @Test
  public void testVariance() {
    Integer[] scores = { 4, 7, 15, 3 };

    for (Integer s : scores) {
      variance.execute(null, null, null, new Object[] { s }, null);
    }

    Object result = variance.getResult();
    Assertions.assertEquals(22.1875, result);
  }

  @Test
  public void testVariance1() {
    Integer[] scores = { 4, 7 };

    for (Integer s : scores) {
      variance.execute(null, null, null, new Object[] { s }, null);
    }

    Object result = variance.getResult();
    Assertions.assertEquals(2.25, result);
  }

  @Test
  public void testVariance2() {
    Integer[] scores = { 15, 3 };

    for (Integer s : scores) {
      variance.execute(null, null, null, new Object[] { s }, null);
    }

    Object result = variance.getResult();
    Assertions.assertEquals(36.0, result);
  }
}
