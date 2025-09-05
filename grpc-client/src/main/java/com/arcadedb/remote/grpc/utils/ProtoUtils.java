package com.arcadedb.remote.grpc.utils;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;

import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.Edge;
import com.arcadedb.server.grpc.GrpcRecord;
import com.arcadedb.server.grpc.GrpcValue;
import com.arcadedb.server.grpc.GrpcValueType;
import com.google.protobuf.Value;
import com.google.protobuf.Struct;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;

public class ProtoUtils {

    /**
     * Converts an ArcadeDB Record into a gRPC GrpcRecord message.
     *
     * @param rec ArcadeDB record (Document, Vertex, or Edge)
     * @return Proto GrpcRecord message
     */
    public static GrpcRecord toProtoRecord(Record rec) {
        if (rec == null) {
            throw new IllegalArgumentException("Record cannot be null");
        }

        GrpcRecord.Builder builder = GrpcRecord.newBuilder();
        
        // Set RID if available
        if (rec.getIdentity() != null) {
            builder.setRid(rec.getIdentity().toString());
        }

        // Determine logical type/class name
        String typeName = null;
        
        if (rec instanceof Vertex) {
            Vertex vertex = rec.asVertex();
            if (vertex != null && vertex.getType() != null) {
                typeName = vertex.getType().getName();
            }
        } else if (rec instanceof Edge) {
            Edge edge = rec.asEdge();
            if (edge != null && edge.getType() != null) {
                typeName = edge.getType().getName();
            }
        } else if (rec instanceof Document) {
            Document doc = rec.asDocument();
            if (doc != null && doc.getType() != null) {
                typeName = doc.getType().getName();
            }
        }
        
        if (typeName != null && !typeName.isEmpty()) {
            builder.setType(typeName);
        }

        // Convert properties
        if (rec instanceof Document) {
            Document doc = (Document) rec;
            for (String propName : doc.getPropertyNames()) {
                Object value = doc.get(propName);
                builder.putProperties(propName, toGrpcValue(value));
            }
        }

        return builder.build();
    }

    /**
     * Converts a Map into a gRPC GrpcRecord message.
     *
     * @param map Map with properties
     * @param rid Optional RID
     * @param type Optional type name
     * @return Proto GrpcRecord message
     */
    public static GrpcRecord mapToProtoRecord(Map<String, Object> map, String rid, String type) {
        GrpcRecord.Builder builder = GrpcRecord.newBuilder();
        
        if (rid != null && !rid.isEmpty()) {
            builder.setRid(rid);
        }
        
        if (type != null && !type.isEmpty()) {
            builder.setType(type);
        }
        
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            builder.putProperties(entry.getKey(), toGrpcValue(entry.getValue()));
        }
        
        return builder.build();
    }

    /**
     * Converts a Java Object into a GrpcValue for GrpcRecord.properties map.
     *
     * Supports primitives, strings, numbers, booleans, lists, maps, dates, and RIDs.
     *
     * @param value Object value
     * @return GrpcValue
     */
    public static GrpcValue toGrpcValue(Object value) {
        GrpcValue.Builder builder = GrpcValue.newBuilder();
        Value.Builder valueBuilder = Value.newBuilder();
        
        if (value == null) {
            valueBuilder.setNullValue(NullValue.NULL_VALUE);
            builder.setValueType(GrpcValueType.RECORD_VALUE_TYPE_UNSPECIFIED);
        } else if (value instanceof Boolean) {
            valueBuilder.setBoolValue((Boolean) value);
            builder.setValueType(GrpcValueType.BOOLEAN);
        } else if (value instanceof Byte) {
            valueBuilder.setNumberValue(((Byte) value).doubleValue());
            builder.setValueType(GrpcValueType.BYTE);
        } else if (value instanceof Short) {
            valueBuilder.setNumberValue(((Short) value).doubleValue());
            builder.setValueType(GrpcValueType.SHORT);
        } else if (value instanceof Integer) {
            valueBuilder.setNumberValue(((Integer) value).doubleValue());
            builder.setValueType(GrpcValueType.INTEGER);
        } else if (value instanceof Long) {
            valueBuilder.setNumberValue(((Long) value).doubleValue());
            builder.setValueType(GrpcValueType.LONG);
        } else if (value instanceof Float) {
            valueBuilder.setNumberValue(((Float) value).doubleValue());
            builder.setValueType(GrpcValueType.FLOAT);
        } else if (value instanceof Double) {
            valueBuilder.setNumberValue((Double) value);
            builder.setValueType(GrpcValueType.DOUBLE);
        } else if (value instanceof String) {
            valueBuilder.setStringValue((String) value);
            builder.setValueType(GrpcValueType.STRING);
        } else if (value instanceof RID) {
            valueBuilder.setStringValue(value.toString());
            builder.setValueType(GrpcValueType.LINK);
        } else if (value instanceof java.util.Date) {
            // Serialize date as ISO8601 string
            valueBuilder.setStringValue(((java.util.Date) value).toInstant().toString());
            builder.setValueType(GrpcValueType.DATETIME);
        } else if (value instanceof java.time.LocalDate) {
            valueBuilder.setStringValue(value.toString());
            builder.setValueType(GrpcValueType.DATE);
        } else if (value instanceof java.time.LocalDateTime) {
            valueBuilder.setStringValue(value.toString());
            builder.setValueType(GrpcValueType.DATETIME);
        } else if (value instanceof java.time.Instant) {
            valueBuilder.setStringValue(value.toString());
            builder.setValueType(GrpcValueType.DATETIME);
        } else if (value instanceof byte[]) {
            // For binary data, encode as base64 string
            valueBuilder.setStringValue(java.util.Base64.getEncoder().encodeToString((byte[]) value));
            builder.setValueType(GrpcValueType.BINARY);
        } else if (value instanceof Map) {
            Struct.Builder structBuilder = Struct.newBuilder();
            Map<?, ?> map = (Map<?, ?>) value;
            
            // Determine if this is an embedded document or a regular map
            boolean isEmbedded = map.containsKey("@type") || map.containsKey("@class");
            
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                GrpcValue nestedValue = toGrpcValue(entry.getValue());
                structBuilder.putFields(String.valueOf(entry.getKey()), nestedValue.getValue());
            }
            
            valueBuilder.setStructValue(structBuilder.build());
            builder.setValueType(isEmbedded ? GrpcValueType.EMBEDDED : GrpcValueType.MAP);
            
            // Set ofType if this is an embedded document with a known type
            if (isEmbedded && map.containsKey("@type")) {
                builder.setOfType(String.valueOf(map.get("@type")));
            }
        } else if (value instanceof Collection) {
            ListValue.Builder listBuilder = ListValue.newBuilder();
            Collection<?> collection = (Collection<?>) value;
            
            String elementType = null;
            for (Object item : collection) {
                GrpcValue nestedValue = toGrpcValue(item);
                listBuilder.addValues(nestedValue.getValue());
                
                // Try to infer element type from first non-null element
                if (elementType == null && item != null) {
                    elementType = inferTypeString(item);
                }
            }
            
            valueBuilder.setListValue(listBuilder.build());
            builder.setValueType(GrpcValueType.LIST);
            
            if (elementType != null) {
                builder.setOfType(elementType);
            }
        } else if (value instanceof Document) {
            // Handle embedded documents
            Document doc = (Document) value;
            Map<String, Object> docMap = new HashMap<>();
            
            for (String propName : doc.getPropertyNames()) {
                docMap.put(propName, doc.get(propName));
            }
            
            if (doc.getType() != null) {
                docMap.put("@type", doc.getType().getName());
            }
            
            // Recursive call to handle as map
            return toGrpcValue(docMap);
        } else {
            // Fallback to string representation
            valueBuilder.setStringValue(value.toString());
            builder.setValueType(GrpcValueType.STRING);
        }
        
        builder.setValue(valueBuilder.build());
        return builder.build();
    }

    /**
     * Converts a GrpcValue back to a Java Object.
     *
     * @param grpcValue GrpcValue to convert
     * @return Java Object
     */
    public static Object fromGrpcValue(GrpcValue grpcValue) {
        if (grpcValue == null) {
            return null;
        }

        Value value = grpcValue.getValue();
        GrpcValueType type = grpcValue.getValueType();

        switch (value.getKindCase()) {
        case NULL_VALUE:
            return null;
            
        case BOOL_VALUE:
            return value.getBoolValue();
            
        case NUMBER_VALUE:
            double numValue = value.getNumberValue();
            // Use the type hint to determine the specific numeric type
            switch (type) {
            case BYTE:
                return (byte) numValue;
            case SHORT:
                return (short) numValue;
            case INTEGER:
                return (int) numValue;
            case LONG:
                return (long) numValue;
            case FLOAT:
                return (float) numValue;
            case DOUBLE:
            case DECIMAL:
            default:
                return numValue;
            }
            
        case STRING_VALUE:
            String strValue = value.getStringValue();
            // Check if this is a special string type
            switch (type) {
            case LINK:
                // Parse as RID if it looks like one
                if (strValue.startsWith("#")) {
                    try {
                        return new RID(strValue);
                    } catch (Exception e) {
                        return strValue;
                    }
                }
                return strValue;
            case BINARY:
                // Decode base64 string back to byte array
                try {
                    return java.util.Base64.getDecoder().decode(strValue);
                } catch (Exception e) {
                    return strValue;
                }
            case DATE:
            case DATETIME:
                // Could parse as date/time if needed
                // For now, return as string and let the caller handle parsing
                return strValue;
            default:
                return strValue;
            }
            
        case STRUCT_VALUE:
            Map<String, Object> map = new HashMap<>();
            value.getStructValue().getFieldsMap().forEach((k, v) -> {
                // Create a GrpcValue wrapper for recursive conversion
                // We don't have type info for nested values, so use UNSPECIFIED
                GrpcValue nestedValue = GrpcValue.newBuilder()
                        .setValue(v)
                        .setValueType(GrpcValueType.RECORD_VALUE_TYPE_UNSPECIFIED)
                        .build();
                map.put(k, fromGrpcValue(nestedValue));
            });
            return map;
            
        case LIST_VALUE:
            List<Object> list = new ArrayList<>();
            value.getListValue().getValuesList().forEach(v -> {
                // Create a GrpcValue wrapper for recursive conversion
                GrpcValue nestedValue = GrpcValue.newBuilder()
                        .setValue(v)
                        .setValueType(GrpcValueType.RECORD_VALUE_TYPE_UNSPECIFIED)
                        .build();
                list.add(fromGrpcValue(nestedValue));
            });
            return list;
            
        default:
            return null;
        }
    }

    /**
     * Helper method to infer type string from an object.
     * Used for setting the ofType field in collections.
     */
    private static String inferTypeString(Object obj) {
        if (obj == null) return null;
        if (obj instanceof String) return "String";
        if (obj instanceof Integer) return "Integer";
        if (obj instanceof Long) return "Long";
        if (obj instanceof Double) return "Double";
        if (obj instanceof Float) return "Float";
        if (obj instanceof Boolean) return "Boolean";
        if (obj instanceof java.util.Date) return "Date";
        if (obj instanceof RID) return "Link";
        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            if (map.containsKey("@type")) {
                return String.valueOf(map.get("@type"));
            }
            return "Map";
        }
        if (obj instanceof Collection) return "List";
        return obj.getClass().getSimpleName();
    }
}