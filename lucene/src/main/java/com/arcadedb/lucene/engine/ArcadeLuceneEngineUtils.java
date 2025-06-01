package com.arcadedb.lucene.engine;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.document.Document;
import com.arcadedb.index.IndexDefinition;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.DocumentType;

import org.apache.lucene.search.SortField;
// Corrected import for SortField.Type
// import org.apache.lucene.search.SortField.первый; // This was incorrect in the prompt
// No, SortField.Type is an enum inside SortField, direct import not needed for Type itself,
// but rather SortField.Type.INT etc.

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ArcadeLuceneEngineUtils {

    private static final Logger logger = Logger.getLogger(ArcadeLuceneEngineUtils.class.getName());

    /**
     * Builds a list of Lucene SortField objects based on sorting criteria
     * specified in the metadata document.
     *
     * @param arcadedbMetadata The metadata document, typically from query options.
     *                         Expected to contain a "sort" or "orderBy" field.
     *                         The value can be a String (e.g., "fieldA ASC, fieldB DESC")
     *                         or a List of Maps (e.g., [{"field": "fieldA", "direction": "ASC"}, ...]).
     * @param indexDefinition Optional: The index definition, used to infer field types for sorting if not specified.
     * @param database Optional: The database instance, used to get schema for type inference.
     * @return A list of Lucene SortField objects.
     */
    public static List<SortField> buildSortFields(Document arcadedbMetadata, IndexDefinition indexDefinition, DatabaseInternal database) {
        List<SortField> sortFields = new ArrayList<>();
        if (arcadedbMetadata == null) {
            return sortFields;
        }

        Object sortCriteria = arcadedbMetadata.get("sort");
        if (sortCriteria == null) {
            sortCriteria = arcadedbMetadata.get("orderBy");
        }

        if (sortCriteria == null) {
            return sortFields;
        }

        if (sortCriteria instanceof String) {
            // Parse string like "fieldA ASC, fieldB DESC"
            String[] criteria = ((String) sortCriteria).split(",");
            for (String criterion : criteria) {
                String[] parts = criterion.trim().split("\\s+"); // Use \\s+ for one or more spaces
                String fieldName = parts[0].trim();
                if (fieldName.isEmpty()) continue;

                boolean reverse = parts.length > 1 && "DESC".equalsIgnoreCase(parts[1].trim());

                SortField.Type sortType = inferSortType(fieldName, indexDefinition, database);
                sortFields.add(new SortField(fieldName, sortType, reverse));
            }
        } else if (sortCriteria instanceof List) {
            // Parse list of maps, e.g., [{"field": "fieldA", "direction": "ASC"}, ...]
            try {
                @SuppressWarnings("unchecked") // Generic type for list elements from Document.get()
                List<Object> criteriaList = (List<Object>) sortCriteria;
                for (Object criterionObj : criteriaList) {
                    if (criterionObj instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, String> criterion = (Map<String, String>) criterionObj;
                        String fieldName = criterion.get("field");
                        String direction = criterion.get("direction");
                        if (fieldName != null && !fieldName.trim().isEmpty()) {
                            boolean reverse = "DESC".equalsIgnoreCase(direction);
                            SortField.Type sortType = inferSortType(fieldName.trim(), indexDefinition, database);
                            sortFields.add(new SortField(fieldName.trim(), sortType, reverse));
                        }
                    } else if (criterionObj instanceof String) { // Support list of strings like ["fieldA ASC", "fieldB DESC"]
                         String[] parts = ((String)criterionObj).trim().split("\\s+");
                         String fieldName = parts[0].trim();
                         if (fieldName.isEmpty()) continue;
                         boolean reverse = parts.length > 1 && "DESC".equalsIgnoreCase(parts[1].trim());
                         SortField.Type sortType = inferSortType(fieldName, indexDefinition, database);
                         sortFields.add(new SortField(fieldName, sortType, reverse));
                    }
                }
            } catch (ClassCastException e) {
                logger.warning("Could not parse 'sort' criteria from List due to unexpected element types: " + e.getMessage());
            }
        } else {
            logger.warning("Unsupported 'sort' criteria format: " + sortCriteria.getClass().getName());
        }

        return sortFields;
    }

    /**
     * Infers the Lucene SortField.Type for a given field name.
     *
     * @param fieldName The name of the field.
     * @param indexDefinition Optional: The index definition containing schema information.
     * @param database Optional: The database instance for schema lookup.
     * @return The inferred SortField.Type, defaults to STRING if type cannot be determined.
     */
    private static SortField.Type inferSortType(String fieldName, IndexDefinition indexDefinition, DatabaseInternal database) {
        // Special Lucene sort field for relevance score
        if ("score".equalsIgnoreCase(fieldName) || SortField.FIELD_SCORE.toString().equals(fieldName)) {
            return SortField.Type.SCORE;
        }
        // Special Lucene sort field for document order
        if (SortField.FIELD_DOC.toString().equals(fieldName)) {
            return SortField.Type.DOC;
        }

        if (database != null && indexDefinition != null && indexDefinition.getTypeName() != null) {
            DocumentType docType = database.getSchema().getType(indexDefinition.getTypeName());
            if (docType != null) {
                Property property = docType.getProperty(fieldName);
                if (property != null) {
                    Type propertyType = property.getType();
                    switch (propertyType) {
                        case INTEGER:
                        case SHORT:
                        case BYTE:
                            return SortField.Type.INT;
                        case LONG:
                        case DATETIME: // Assuming DATETIME is stored as long epoch millis for sorting
                        case DATE:     // Assuming DATE is stored as long epoch millis for sorting
                            return SortField.Type.LONG;
                        case FLOAT:
                            return SortField.Type.FLOAT;
                        case DOUBLE:
                            return SortField.Type.DOUBLE;
                        case STRING:
                        case TEXT:
                        case ENUM:
                        case UUID: // UUIDs are often sorted as strings
                        case BINARY: // Might be sorted as string, or custom if specific byte order needed
                            return SortField.Type.STRING;
                        // Add other types as needed, e.g., CUSTOM for specific comparators
                        // BOOLEAN is not directly sortable with a standard SortField.Type unless mapped to INT/STRING
                        default:
                            logger.finer("Cannot infer specific Lucene SortField.Type for ArcadeDB Type " + propertyType + " on field '" + fieldName + "'. Defaulting to STRING.");
                            return SortField.Type.STRING;
                    }
                } else {
                     logger.finer("Property '" + fieldName + "' not found in type '" + indexDefinition.getTypeName() + "'. Defaulting to STRING sort type.");
                }
            } else {
                 logger.finer("DocumentType '" + indexDefinition.getTypeName() + "' not found in schema. Defaulting to STRING sort type for field '" + fieldName + "'.");
            }
        }
        // Default if schema info is unavailable or field not found
        logger.finer("Insufficient schema information for field '" + fieldName + "'. Defaulting to STRING sort type.");
        return SortField.Type.STRING;
    }
}
