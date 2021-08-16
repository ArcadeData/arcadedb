package com.arcadedb.query.sql.method;

import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.method.misc.SQLMethodKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SQLMethodKeysTest {

  private SQLMethodKeys function;

  @BeforeEach
  public void setup() {
    function = new SQLMethodKeys();
  }

  @Test
  public void testWithOResult() {

    ResultInternal resultInternal = new ResultInternal();
    resultInternal.setProperty("name", "Foo");
    resultInternal.setProperty("surname", "Bar");

    Object result = function.execute(null, null, null, resultInternal, null);
    assertEquals(new LinkedHashSet(Arrays.asList("name", "surname")), result);
  }
}
