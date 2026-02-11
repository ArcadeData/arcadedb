package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import org.junit.jupiter.api.Test;

public class DebugVertexFormatTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("TestVertex"))
        database.getSchema().buildVertexType().withName("TestVertex").withTotalBuckets(1).create();
    });
  }

  @Test
  public void debugVertexFormat() {
    database.transaction(() -> {
      try {
        System.out.println("\n===== Creating new vertex =====");
        final MutableVertex v1 = database.newVertex("TestVertex");

        // Check initial state
        final java.lang.reflect.Field formatVersionField = MutableVertex.class.getDeclaredField("formatVersion");
        formatVersionField.setAccessible(true);
        System.out.println("Before save - formatVersion: " + formatVersionField.get(v1));
        System.out.println("Before save - buffer: " + (v1.getBuffer() != null ? "exists" : "null"));

        System.out.println("\n===== Saving vertex =====");
        v1.save();

        // Check after save
        System.out.println("After save - formatVersion: " + formatVersionField.get(v1));
        System.out.println("After save - buffer: " + (v1.getBuffer() != null ? "exists" : "null"));

        if (v1.getBuffer() != null) {
          final Binary buffer = v1.getBuffer();
          buffer.position(0);
          System.out.println("Buffer byte 0: " + buffer.getByte());
          System.out.println("Buffer byte 1: " + buffer.getByte());
          System.out.println("Buffer size: " + buffer.size());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
