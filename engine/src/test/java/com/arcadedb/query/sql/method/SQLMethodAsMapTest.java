package com.arcadedb.query.sql.method;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.method.misc.SQLMethodAsMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Tests the "asMap()" method implemented by the OSQLMethodAsMap class. Note that the only input to
 * the execute() method from the OSQLMethod interface that is used is the ioResult argument (the 4th
 * argument).
 *
 * @author Michael MacFadden
 */
public class SQLMethodAsMapTest {

  private SQLMethodAsMap function;

  @BeforeEach
  public void setup() {
    function = new SQLMethodAsMap();
  }

  @Test
  public void testMap() {
    // The expected behavior is to return the map itself.
    HashMap<Object, Object> aMap = new HashMap<Object, Object>();
    aMap.put("p1", 1);
    aMap.put("p2", 2);
    Object result = function.execute(null, null, null, aMap, null);
    Assertions.assertEquals(result, aMap);
  }

  @Test
  public void testNull() {
    // The expected behavior is to return an empty map.
    Object result = function.execute(null, null, null, null, null);
    Assertions.assertEquals(result, new HashMap<Object, Object>());
  }

  public void testODocument() {
    // The expected behavior is to return a map that has the field names mapped
    // to the field values of the ODocument.
    MutableDocument doc = new MutableDocument(null, null, null) {
    };

    doc.set("f1", 1);
    doc.set("f2", 2);

    Object result = function.execute(null, null, null, doc, null);

    Assertions.assertEquals(result, doc.toMap());
  }

  @Test
  public void testIterable() {
    // The expected behavior is to return a map where the even values (0th,
    // 2nd, 4th, etc) are keys and the odd values (1st, 3rd, etc.) are
    // property values.
    ArrayList<Object> aCollection = new ArrayList<Object>();
    aCollection.add("p1");
    aCollection.add(1);
    aCollection.add("p2");
    aCollection.add(2);

    Object result = function.execute(null, null, null, aCollection, null);

    HashMap<Object, Object> expected = new HashMap<Object, Object>();
    expected.put("p1", 1);
    expected.put("p2", 2);
    Assertions.assertEquals(result, expected);
  }

  @Test
  public void testIterator() {
    // The expected behavior is to return a map where the even values (0th,
    // 2nd, 4th, etc) are keys and the odd values (1st, 3rd, etc.) are
    // property values.
    ArrayList<Object> aCollection = new ArrayList<Object>();
    aCollection.add("p1");
    aCollection.add(1);
    aCollection.add("p2");
    aCollection.add(2);

    Object result = function.execute(null, null, null, aCollection.iterator(), null);

    HashMap<Object, Object> expected = new HashMap<Object, Object>();
    expected.put("p1", 1);
    expected.put("p2", 2);
    Assertions.assertEquals(result, expected);
  }

  public void testOtherValue() {
    // The expected behavior is to return null.
    Object result = function.execute(null, null, null, Integer.valueOf(4), null);
    Assertions.assertEquals(result, null);
  }
}
