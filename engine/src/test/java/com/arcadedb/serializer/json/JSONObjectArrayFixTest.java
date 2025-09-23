package com.arcadedb.serializer.json;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class to verify the fix for JSON empty array serialization issue.
 * Tests the specific case where Object[] arrays (including empty ones)
 * should be properly serialized as JSON arrays instead of string representations.
 */
public class JSONObjectArrayFixTest {

    @Test
    public void testEmptyObjectArraySerialization() {
        JSONObject json = new JSONObject();
        Object[] emptyArray = new Object[0];

        // This should not produce a string representation like "[Ljava.lang.Object;@b78a709"
        json.put("emptyArray", emptyArray);

        String jsonString = json.toString();

        // Should contain proper JSON array syntax
        assertTrue(jsonString.contains("\"emptyArray\":[]"),
                  "Empty Object[] array should serialize as empty JSON array []");
        assertFalse(jsonString.contains("[Ljava.lang.Object;"),
                   "Should not contain Java object string representation");
    }

    @Test
    public void testNonEmptyObjectArraySerialization() {
        JSONObject json = new JSONObject();
        Object[] array = new Object[]{"test", 123, true};

        json.put("testArray", array);

        String jsonString = json.toString();

        // Should contain proper JSON array with values
        assertTrue(jsonString.contains("\"testArray\":["),
                  "Object[] array should serialize as JSON array");
        assertTrue(jsonString.contains("\"test\"") && jsonString.contains("123") && jsonString.contains("true"),
                  "Array values should be properly serialized");
        assertFalse(jsonString.contains("[Ljava.lang.Object;"),
                   "Should not contain Java object string representation");
    }

    @Test
    public void testMixedObjectArraySerialization() {
        JSONObject json = new JSONObject();
        Object[] mixedArray = new Object[]{null, "string", 42, new Object[0]};

        json.put("mixedArray", mixedArray);

        String jsonString = json.toString();

        // Should handle nested arrays and null values properly
        assertTrue(jsonString.contains("\"mixedArray\":["),
                  "Mixed Object[] array should serialize as JSON array");
        assertTrue(jsonString.contains("null"),
                  "Null values should be preserved");
        assertFalse(jsonString.contains("[Ljava.lang.Object;"),
                   "Should not contain Java object string representation");
    }

    @Test
    public void testObjectArrayDeserialization() {
        // Test round-trip: serialize -> parse -> verify
        JSONObject original = new JSONObject();
        Object[] testArray = new Object[]{"hello", 999, false};
        original.put("data", testArray);

        String jsonString = original.toString();

        // Parse it back
        JSONObject parsed = new JSONObject(jsonString);
        JSONArray parsedArray = parsed.getJSONArray("data");

        assertEquals(3, parsedArray.length(), "Parsed array should have correct length");
        assertEquals("hello", parsedArray.get(0), "First element should match");
        assertEquals(999, parsedArray.get(1), "Second element should match");
        assertEquals(false, parsedArray.get(2), "Third element should match");
    }
}
