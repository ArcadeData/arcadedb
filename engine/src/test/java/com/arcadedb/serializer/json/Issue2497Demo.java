package com.arcadedb.serializer.json;

/**
 * Demonstration class for Issue #2497 - JSON empty array serialization fix.
 * This class shows the before/after behavior of Object[] array serialization.
 */
public class Issue2497Demo {

    public static void main(String[] args) {
        System.out.println("=== Issue #2497 Demo: JSON Object[] Array Serialization Fix ===\n");

        demonstrateEmptyArrayFix();
        demonstrateNonEmptyArrayFix();
        demonstrateNestedArrayFix();
        demonstrateSQLInsertScenario();
    }

    private static void demonstrateEmptyArrayFix() {
        System.out.println("1. Empty Object[] Array Test:");
        System.out.println("-----------------------------");

        JSONObject json = new JSONObject();
        Object[] emptyArray = new Object[0];

        json.put("emptyArray", emptyArray);
        String result = json.toString();

        System.out.println("Input: Object[] emptyArray = new Object[0]");
        System.out.println("JSONObject.put(\"emptyArray\", emptyArray)");
        System.out.println("Result: " + result);

        if (result.contains("\"emptyArray\":[]")) {
            System.out.println("✅ FIXED: Properly serialized as empty JSON array");
        } else {
            System.out.println("❌ BROKEN: Would show Java object reference");
        }
        System.out.println();
    }

    private static void demonstrateNonEmptyArrayFix() {
        System.out.println("2. Non-Empty Object[] Array Test:");
        System.out.println("---------------------------------");

        JSONObject json = new JSONObject();
        Object[] array = new Object[]{"hello", 42, true, null};

        json.put("data", array);
        String result = json.toString();

        System.out.println("Input: Object[] array = {\"hello\", 42, true, null}");
        System.out.println("JSONObject.put(\"data\", array)");
        System.out.println("Result: " + result);

        if (result.contains("\"data\":[") && result.contains("\"hello\"")) {
            System.out.println("✅ FIXED: Properly serialized as JSON array with values");
        } else {
            System.out.println("❌ BROKEN: Would show Java object reference");
        }
        System.out.println();
    }

    private static void demonstrateNestedArrayFix() {
        System.out.println("3. Nested Object[] Array Test:");
        System.out.println("------------------------------");

        JSONObject json = new JSONObject();
        Object[] nestedArray = new Object[]{
            "outer",
            new Object[]{"inner1", "inner2"},
            new Object[0] // empty nested array
        };

        json.put("nested", nestedArray);
        String result = json.toString();

        System.out.println("Input: Nested Object[] arrays with empty array inside");
        System.out.println("Result: " + result);

        if (result.contains("\"nested\":[") && result.contains("[]")) {
            System.out.println("✅ FIXED: Nested arrays properly serialized");
        } else {
            System.out.println("❌ BROKEN: Would show Java object references");
        }
        System.out.println();
    }

    private static void demonstrateSQLInsertScenario() {
        System.out.println("4. SQL INSERT Scenario (typical use case):");
        System.out.println("------------------------------------------");

        // Simulate what happens in SQL INSERT with empty array content
        JSONObject document = new JSONObject();
        document.put("id", 1);
        document.put("name", "test");
        document.put("tags", new Object[0]); // This is what SQL INSERT would create
        document.put("metadata", new Object[]{"key1", "value1"});

        String result = document.toString();

        System.out.println("Simulating SQL INSERT INTO table (id, name, tags, metadata)");
        System.out.println("VALUES (1, 'test', [], ['key1', 'value1'])");
        System.out.println();
        System.out.println("Generated JSON: " + result);

        boolean hasProperEmptyArray = result.contains("\"tags\":[]");
        boolean hasProperNonEmptyArray = result.contains("\"metadata\":[") && result.contains("\"key1\"");
        boolean noJavaReferences = !result.contains("[Ljava.lang.Object;");

        if (hasProperEmptyArray && hasProperNonEmptyArray && noJavaReferences) {
            System.out.println("✅ FIXED: SQL INSERT scenarios work correctly");
        } else {
            System.out.println("❌ BROKEN: SQL INSERT would produce invalid JSON");
        }
        System.out.println();
    }

    /**
     * Method to show what the behavior was BEFORE the fix (for documentation)
     */
    private static void showPreviousBrokenBehavior() {
        System.out.println("=== BEFORE FIX (broken behavior) ===");
        System.out.println("Object[] emptyArray = new Object[0];");
        System.out.println("json.put(\"test\", emptyArray);");
        System.out.println("Result would be: {\"test\":\"[Ljava.lang.Object;@b78a709\"}");
        System.out.println();
        System.out.println("=== AFTER FIX (correct behavior) ===");
        System.out.println("Object[] emptyArray = new Object[0];");
        System.out.println("json.put(\"test\", emptyArray);");
        System.out.println("Result is now: {\"test\":[]}");
    }
}
