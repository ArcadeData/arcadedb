package com.arcadedb.remote.grpc.utils;
import java.util.Map;

import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Vertex;
import com.arcadedb.server.grpc.Record.Builder;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;

public class ProtoUtils {

    /**
     * Converts an ArcadeDB Record into a gRPC Record message.
     *
     * @param rec ArcadeDB record (Document, Vertex, or Edge)
     * @return Proto Record message
     */
    public static com.arcadedb.server.grpc.Record toProtoRecord(Record rec) {
        
    	if (rec == null) {
            throw new IllegalArgumentException("Record cannot be null");
        }

        Builder builder = com.arcadedb.server.grpc.Record.newBuilder()
                .setRid(rec.getIdentity().toString());

        // Determine logical type/class name
        
        String typeName = null;
        
        if (rec instanceof Vertex) {
        	
        	typeName = rec.asVertex().getType().getName();
        }

        if (rec instanceof Document) {
        	
        	typeName = rec.asDocument().getType().getName();
        }
        
        if (typeName != null && !typeName.isEmpty()) {
           
        	builder.setType(typeName);
        }

        // Convert properties
        if (rec instanceof Document doc) {
            for (String propName : doc.getPropertyNames()) {
                Object value = doc.get(propName);
                builder.putProperties(propName, toProtoValue(value));
            }
        }

        return builder.build();
    }

    /**
     * Converts a Java Object into a protobuf Value for Record.properties map.
     *
     * Supports primitives, strings, numbers, booleans, lists, and maps.
     *
     * @param value Object value
     * @return Protobuf Value
     */
    private static Value toProtoValue(Object value) {
       
    	if (value == null) {
            return Values.ofNull();
        }
        if (value instanceof String s) {
            return Values.of(s);
        }
        if (value instanceof Integer i) {
            return Values.of(i);
        }
        if (value instanceof Long l) {
            return Values.of(l);
        }
        if (value instanceof Float f) {
            return Values.of(f);
        }
        if (value instanceof Double d) {
            return Values.of(d);
        }
        if (value instanceof Boolean b) {
            return Values.of(b);
        }
        if (value instanceof java.util.Date dt) {
            // Serialize date as ISO8601 string
            return Values.of(dt.toInstant().toString());
        }
        if (value instanceof Iterable<?> list) {
            var lv = com.google.protobuf.ListValue.newBuilder();
            for (Object item : list) {
                lv.addValues(toProtoValue(item));
            }
            return Values.of(lv.build());
        }
        if (value instanceof Map<?, ?> map) {
            var structBuilder = com.google.protobuf.Struct.newBuilder();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                structBuilder.putFields(String.valueOf(entry.getKey()), toProtoValue(entry.getValue()));
            }
            return Values.of(structBuilder.build());
        }
        // fallback to string
        return Values.of(String.valueOf(value));
    }
}