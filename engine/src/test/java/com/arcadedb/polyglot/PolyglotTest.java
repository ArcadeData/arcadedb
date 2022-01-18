package com.arcadedb.polyglot;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.graalvm.polyglot.PolyglotException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.*;
import java.util.*;
import java.util.concurrent.*;

public class PolyglotTest extends TestHelper {
  @Test
  public void testSum() {
    ResultSet result = database.command("js", "3 + 5");

    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));
  }

  @Test
  public void testEmbeddedFunction() {
    ResultSet result = database.command("js", "function sum(a,b){return a + b;} sum(3,5);");
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));
  }

  @Test
  public void testReuseSameQueryEngine() {
    ResultSet result = database.command("js", "function sum(a,b){return a + b;} sum(3,5);");
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));

    result = database.command("js", "function sum(a,b){return a + b;} sum(3,5);");
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));
  }

  @Test
  public void testDatabaseQuery() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Product");
      database.newVertex("Product").set("name", "Amiga 1200", "price", 900).save();
    });

    ResultSet result = database.command("js", "database.query('sql', 'select from Product')");
    Assertions.assertTrue(result.hasNext());

    final Vertex vertex = result.next().getRecord().get().asVertex();
    Assertions.assertEquals("Amiga 1200", vertex.get("name"));
    Assertions.assertEquals(900, vertex.get("price"));
  }

  @Test
  public void testSandbox() {
    // BY DEFAULT NO JAVA PACKAGES ARE ACCESSIBLE
    try {
      ResultSet result = database.command("js", "let BigDecimal = Java.type('java.math.BigDecimal'); new BigDecimal(1)");
      Assertions.assertFalse(result.hasNext());
      Assertions.fail("It should not execute the function");
    } catch (Exception e) {
      Assertions.assertTrue(e instanceof UserCodeException);
      Assertions.assertTrue(e.getCause() instanceof PolyglotException);
      Assertions.assertTrue(e.getCause().getMessage().contains("java.math.BigDecimal"));
    }

    // ALLOW ACCESSING TO BIG DECIMAL CLASS
    ((EmbeddedDatabase) database).registerReusableQueryEngine(
        new PolyglotQueryEngine.PolyglotQueryEngineFactory("js").setAllowedPackages(Collections.singletonList("java.math.BigDecimal"))
            .getInstance((DatabaseInternal) database));

    ResultSet result = database.command("js", "let BigDecimal = Java.type('java.math.BigDecimal'); new BigDecimal(1)");

    Assertions.assertTrue(result.hasNext());

    Assertions.assertEquals(new BigDecimal(1), result.next().getProperty("value"));
  }

  @Test
  public void testTimeout() {
    GlobalConfiguration.POLYGLOT_COMMAND_TIMEOUT.setValue(2000);
    try {
      database.command("js", "while(true);");
      Assertions.fail("It should not execute the function");
    } catch (Exception e) {
      Assertions.assertTrue(e instanceof UserCodeException);
      Assertions.assertTrue(e.getCause() instanceof TimeoutException);
    } finally {
      GlobalConfiguration.POLYGLOT_COMMAND_TIMEOUT.reset();
    }
  }
}
