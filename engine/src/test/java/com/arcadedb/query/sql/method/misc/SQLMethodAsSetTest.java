package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the "asSet()" method implemented by the OSQLMethodAsSet class. Note that the only input to
 * the execute() method from the OSQLMethod interface that is used is the ioResult argument (the 4th
 * argument).
 *
 * @author Michael MacFadden
 */
public class SQLMethodAsSetTest {

    private SQLMethod function;

    @BeforeEach
    public void setup() {
        function = new SQLMethodAsSet();
    }

    @Test
    public void testSet() {
        // The expected behavior is to return the set itself.
        HashSet<Object> aSet = new HashSet<Object>();
        aSet.add(1);
        aSet.add("2");
        Object result = function.execute(null, null, null, aSet, null);
        assertEquals(result, aSet);
    }

    @Test
    public void testNull() {
        // The expected behavior is to return an empty set.
        Object result = function.execute(null, null, null, null, null);
        assertEquals(result, new HashSet<Object>());
    }

    @Test
    public void testCollection() {
        // The expected behavior is to return a set with all of the elements
        // of the collection in it.
        ArrayList<Object> aCollection = new ArrayList<Object>();
        aCollection.add(1);
        aCollection.add("2");
        Object result = function.execute(null, null, null, aCollection, null);

        HashSet<Object> expected = new HashSet<Object>();
        expected.add(1);
        expected.add("2");
        assertEquals(result, expected);
    }

    @Test
    public void testIterable() {
        // The expected behavior is to return a set with all of the elements
        // of the iterable in it.
        ArrayList<Object> values = new ArrayList<Object>();
        values.add(1);
        values.add("2");

        TestIterable<Object> anIterable = new TestIterable<Object>(values);
        Object result = function.execute(null, null, null, anIterable, null);

        HashSet<Object> expected = new HashSet<Object>();
        expected.add(1);
        expected.add("2");

        assertEquals(result, expected);
    }

    @Test
    public void testIterator() {
        // The expected behavior is to return a set with all of the elements
        // of the iterator in it.
        ArrayList<Object> values = new ArrayList<Object>();
        values.add(1);
        values.add("2");

        TestIterable<Object> anIterable = new TestIterable<Object>(values);
        Object result = function.execute(null, null, null, anIterable.iterator(), null);

        HashSet<Object> expected = new HashSet<Object>();
        expected.add(1);
        expected.add("2");

        assertEquals(result, expected);
    }

    public void testODocument() {
        // The expected behavior is to return a set with only the single
        // ODocument in it.
        MutableDocument doc = new MutableDocument(null, null, null) {
        };

        doc.set("f1", 1);
        doc.set("f2", 2);

        Object result = function.execute(null, null, null, doc, null);

        HashSet<Object> expected = new HashSet<Object>();
        expected.add(doc);

        assertEquals(result, expected);
    }

    @Test
    public void testOtherSingleValue() {
        // The expected behavior is to return a set with only the single
        // element in it.

        Object result = function.execute(null, null, null, Integer.valueOf(4), null);
        HashSet<Object> expected = new HashSet<Object>();
        expected.add(Integer.valueOf(4));
        assertEquals(result, expected);
    }
}
