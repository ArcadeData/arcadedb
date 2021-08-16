package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Created by luigidellaquila on 04/11/16.
 */
public class ResultSetTest {
  @Test
  public void testResultStream() {
    InternalResultSet rs = new InternalResultSet();
    for (int i = 0; i < 10; i++) {
      ResultInternal item = new ResultInternal();
      item.setProperty("i", i);
      rs.add(item);
    }
    Optional<Integer> result = rs.stream().map(x -> (int) x.getProperty("i")).reduce((a, b) -> a + b);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(45, result.get().intValue());
  }

  @Test
  public void testResultEmptyVertexStream() {
    InternalResultSet rs = new InternalResultSet();
    for (int i = 0; i < 10; i++) {
      ResultInternal item = new ResultInternal();
      item.setProperty("i", i);
      rs.add(item);
    }
    Optional<Integer> result = rs.vertexStream().map(x -> (int) x.get("i")).reduce((a, b) -> a + b);
    Assertions.assertFalse(result.isPresent());
  }

  @Test
  public void testResultEdgeVertexStream() {
    InternalResultSet rs = new InternalResultSet();
    for (int i = 0; i < 10; i++) {
      ResultInternal item = new ResultInternal();
      item.setProperty("i", i);
      rs.add(item);
    }
    Optional<Integer> result = rs.vertexStream().map(x -> (int) x.get("i")).reduce((a, b) -> a + b);
    Assertions.assertFalse(result.isPresent());
  }
}
