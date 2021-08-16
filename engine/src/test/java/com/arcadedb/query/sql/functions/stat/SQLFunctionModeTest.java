package com.arcadedb.query.sql.functions.stat;

import com.arcadedb.query.sql.function.stat.SQLFunctionMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class SQLFunctionModeTest {

  private SQLFunctionMode mode;

  @BeforeEach
  public void setup() {
    mode = new SQLFunctionMode();
  }

  @Test
  public void testEmpty() {
    Object result = mode.getResult();
    Assertions.assertNull(result);
  }

  @Test
  public void testSingleMode() {
    int[] scores = { 1, 2, 3, 3, 3, 2 };

    for (int s : scores) {
      mode.execute(null, null, null, new Object[] { s }, null);
    }

    Object result = mode.getResult();
    Assertions.assertEquals(3, (int) ((List<Integer>) result).get(0));
  }

  @Test
  public void testMultiMode() {
    int[] scores = { 1, 2, 3, 3, 3, 2, 2 };

    for (int s : scores) {
      mode.execute(null, null, null, new Object[] { s }, null);
    }

    Object result = mode.getResult();
    List<Integer> modes = (List<Integer>) result;
    Assertions.assertEquals(2, modes.size());
    Assertions.assertTrue(modes.contains(2));
    Assertions.assertTrue(modes.contains(3));
  }

  @Test
  public void testMultiValue() {
    List[] scores = new List[2];
    scores[0] = Arrays.asList(new Integer[] { 1, 2, null, 3, 4 });
    scores[1] = Arrays.asList(new Integer[] { 1, 1, 1, 2, null });

    for (List s : scores) {
      mode.execute(null, null, null, new Object[] { s }, null);
    }

    Object result = mode.getResult();
    Assertions.assertEquals(1, (int) ((List<Integer>) result).get(0));
  }
}
