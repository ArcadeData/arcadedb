/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arcadedb.lucene.builder;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.document.Document; // ArcadeDB Document
import com.arcadedb.index.IndexDefinition;
import com.arcadedb.lucene.index.ArcadeLuceneIndexType;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.lucene.document.Field; // Lucene Field
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;

public class LuceneDocumentBuilder {

    private static final Logger logger = Logger.getLogger(LuceneDocumentBuilder.class.getName());

    public org.apache.lucene.document.Document build(IndexDefinition indexDefinition,
                                                     Object key, // The key used for indexing (can be composite)
                                                     Identifiable identifiableValue, // The record to index
                                                     Map<String, Boolean> collectionFields, // Info about collection fields (if needed, from old engine)
                                                     com.arcadedb.document.Document metadata) { // Query/index time metadata

        org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();

        // Add RID field
        if (identifiableValue != null && identifiableValue.getIdentity() != null) {
            luceneDoc.add(ArcadeLuceneIndexType.createRidField(identifiableValue));
        }

        // Add KEY field(s) if the key is provided and the index is not on specific fields (manual index style)
        // For automatic indexes, the key is usually derived from the document's fields.
        if (key != null && (indexDefinition.getFields() == null || indexDefinition.getFields().isEmpty())) {
             // This logic is more for manual indexes where 'key' is the value being indexed.
             // For automatic indexes on document fields, this 'key' might be redundant or handled differently.
             // Assuming KEY field stores the string representation of the key for now.
            luceneDoc.add(new StringField(ArcadeLuceneIndexType.KEY, key.toString(), Field.Store.YES));
        }


        if (identifiableValue instanceof com.arcadedb.document.Document) {
            com.arcadedb.document.Document record = (com.arcadedb.document.Document) identifiableValue;
            DatabaseInternal db = record.getDatabase();
            DocumentType recordType = record.getType();

            List<String> fieldsToIndex = indexDefinition.getFields();
            if (fieldsToIndex == null || fieldsToIndex.isEmpty()) {
                // If no specific fields defined for index (e.g. manual index),
                // and we already added KEY, then we might be done for primary content for this key.
                // However, if the 'value' (record) itself should have its fields indexed,
                // then fieldsToIndex should probably default to all fields of the record.
                // This part depends on the semantics of "automatic" vs "manual" Lucene indexes.
                // For now, if no fields are in definition, we assume only KEY and RID are added.
            } else {
                for (String fieldName : fieldsToIndex) {
                    if (!record.has(fieldName)) {
                        continue;
                    }
                    Object fieldValue = record.get(fieldName);
                    if (fieldValue == null) {
                        continue;
                    }

                    Property property = recordType != null ? recordType.getProperty(fieldName) : null;
                    Type fieldType = property != null ? property.getType() : Type.STRING; // Default to STRING if no schema type

                    // Determine if field should be stored and sorted based on index definition options or metadata
                    boolean storeField = isToStore(indexDefinition, fieldName, metadata);
                    boolean sortField = isToSort(indexDefinition, fieldName, metadata);

                    if (fieldValue instanceof Collection && (fieldType == Type.EMBEDDEDLIST || fieldType == Type.EMBEDDEDSET || fieldType == Type.LIST)) {
                        Collection<?> collection = (Collection<?>) fieldValue;
                        Type linkedType = (property != null && property.getOfType() != null) ? property.getOfType() : null;

                        if (linkedType == null && !collection.isEmpty()) { // Try to infer from first element if not specified in schema
                            Object firstElement = collection.iterator().next();
                            if (firstElement instanceof Document) linkedType = Type.EMBEDDED; // Or specific DocumentType if available
                            else if (firstElement != null) linkedType = Type.getTypeByValue(firstElement);
                        }

                        if (linkedType != null && linkedType != Type.EMBEDDED && linkedType != Type.EMBEDDEDMAP) { // Scalar list/set
                            for (Object item : collection) {
                                if (item != null) {
                                    List<Field> itemFields = ArcadeLuceneIndexType.createFields(fieldName, item,
                                            storeField ? Field.Store.YES : Field.Store.NO,
                                            sortField, // Note: Sorting on multi-value fields needs specific Lucene setup.
                                                       // createFields will add DocValues for the type if sortField is true.
                                            linkedType);
                                    for (Field f : itemFields) {
                                        luceneDoc.add(f);
                                    }
                                }
                            }
                        } else { // EMBEDDEDLIST/SET of Documents, or list of EMBEDDEDMAP (unlikely for direct indexing here)
                            // FIXME: Implement flattening strategy for embedded documents in collections.
                            // Example: fieldName_embeddedField. This needs recursive calls or a helper.
                            // For now, logging a warning and indexing toString() for each item if it's a Document.
                            logger.warning("Full indexing of embedded documents within collection '" + fieldName + "' is not yet implemented. Indexing toString().");
                            if (linkedType == Type.EMBEDDED || (linkedType == null && collection.iterator().hasNext() && collection.iterator().next() instanceof Document)) {
                                for (Object item : collection) {
                                    if (item != null) {
                                         List<Field> itemFields = ArcadeLuceneIndexType.createFields(fieldName, item.toString(),
                                            storeField ? Field.Store.YES : Field.Store.NO, false, Type.STRING); // Index as string
                                         for (Field f : itemFields) { luceneDoc.add(f); }
                                    }
                                }
                            }
                        }
                    } else if (fieldValue instanceof Map && fieldType == Type.EMBEDDEDMAP) {
                        // FIXME: Implement flattening strategy for embedded maps.
                        // Example: fieldName_mapKey_embeddedField or index map entries as JSON/string.
                        logger.warning("Indexing embedded maps is not yet fully implemented for field: " + fieldName + ". Indexing toString().");
                        List<Field> mapFields = ArcadeLuceneIndexType.createFields(fieldName, fieldValue.toString(),
                            storeField ? Field.Store.YES : Field.Store.NO, false, Type.STRING);
                        for (Field f : mapFields) { luceneDoc.add(f); }

                    } else if (fieldValue instanceof Document && fieldType == Type.EMBEDDED) {
                        // FIXME: Implement flattening strategy for single embedded documents.
                        // Example: fieldName_embeddedField. This needs recursive calls or a helper.
                        logger.warning("Indexing single embedded documents is not yet fully implemented for field: " + fieldName + ". Indexing toString().");
                        List<Field> embeddedFields = ArcadeLuceneIndexType.createFields(fieldName, fieldValue.toString(),
                            storeField ? Field.Store.YES : Field.Store.NO, false, Type.STRING);
                        for (Field f : embeddedFields) { luceneDoc.add(f); }
                    } else { // Scalar field
                        List<Field> luceneFields = ArcadeLuceneIndexType.createFields(fieldName, fieldValue,
                                storeField ? Field.Store.YES : Field.Store.NO,
                                sortField,
                                fieldType);
                        for (Field f : luceneFields) {
                            luceneDoc.add(f);
                        }
                    }
                }
            }
        } else if (identifiableValue != null) {
            // If the value is an Identifiable but not a Document (e.g. just an RID for a manual index key)
            // and fields are defined in the index, this implies we should load the document
            // and then process its fields. This case should ideally be handled by the caller
            // by passing the actual Document record.
            // If only key and RID are indexed for non-Document identifiables, current logic is okay.
        }


        // Add _CLASS field if type is available
        String typeName = indexDefinition.getTypeName();
        if (typeName != null && !typeName.isEmpty()) {
             luceneDoc.add(new StringField("_CLASS", typeName, Field.Store.YES)); // Non-analyzed
        }

        // Log usage of collectionFields if it's passed but not deeply integrated yet
        if (collectionFields != null && !collectionFields.isEmpty()) {
            // The `collectionFields` map (from OrientDB's engine) indicated if a field was a collection of simple types.
            // This information might be used to guide specific tokenization or if ArcadeLuceneIndexType.createFields
            // needs more hints for collections of scalars vs. collections of embeddeds, though getType and getOfType should cover most cases.
            // For now, just logging its presence.
            logger.finer("Received 'collectionFields' map, but its specific nuanced behavior is not fully implemented beyond standard collection handling: " + collectionFields);
        }

        return luceneDoc;
    }

    /**
     * Determines if a field should be stored in the Lucene index based on index definition options.
                        }
                    }
                }
            }
        } else if (identifiableValue != null) {
            // If the value is an Identifiable but not a Document (e.g. just an RID for a manual index key)
            // and fields are defined in the index, this implies we should load the document
            // and then process its fields. This case should ideally be handled by the caller
            // by passing the actual Document record.
            // If only key and RID are indexed for non-Document identifiables, current logic is okay.
        }


        // Add _CLASS field if type is available
        String typeName = indexDefinition.getTypeName();
        if (typeName != null && !typeName.isEmpty()) {
             luceneDoc.add(new StringField("_CLASS", typeName, Field.Store.YES)); // Non-analyzed
        }


        return luceneDoc;
    }

    /**
     * Determines if a field should be stored in the Lucene index based on index definition options.
     * Convention:
     * - "storeFields": "*" or "ALL" means store all.
     * - "storeFields": "fieldA,fieldB" means store only these.
     * - "dontStoreFields": "fieldC,fieldD" means do not store these (takes precedence).
     * - "store.<fieldName>": "true" or "false" for field-specific setting.
     * Defaults to Field.Store.NO if not specified otherwise for full-text search efficiency.
     */
    private boolean isToStore(IndexDefinition indexDefinition, String fieldName, com.arcadedb.document.Document metadata) {
        Map<String, String> options = indexDefinition.getOptions();
        // Query-time metadata can override index-time options
        if (metadata != null) {
            Object fieldSpecificStoreMeta = metadata.get("store." + fieldName);
            if (fieldSpecificStoreMeta != null) return Boolean.parseBoolean(fieldSpecificStoreMeta.toString());

            List<String> queryStoredFields = metadata.get("storedFields"); // Assuming list of strings
            if (queryStoredFields != null) {
                if (queryStoredFields.contains(fieldName)) return true;
                if (queryStoredFields.contains("*") || queryStoredFields.contains("ALL")) return true;
            }
            List<String> queryDontStoreFields = metadata.get("dontStoreFields");
             if (queryDontStoreFields != null && queryDontStoreFields.contains(fieldName)) return false;
        }

        // Index definition options
        if (options != null) {
            String fieldSpecificStoreOpt = options.get("store." + fieldName);
            if (fieldSpecificStoreOpt != null) return Boolean.parseBoolean(fieldSpecificStoreOpt);

            String dontStoreFieldsOpt = options.get("dontStoreFields");
            if (dontStoreFieldsOpt != null) {
                List<String> dontStoreList = Arrays.asList(dontStoreFieldsOpt.toLowerCase().split("\\s*,\\s*"));
                if (dontStoreList.contains(fieldName.toLowerCase())) return false;
            }

            String storeFieldsOpt = options.get("storeFields");
            if (storeFieldsOpt != null) {
                if ("*".equals(storeFieldsOpt) || "ALL".equalsIgnoreCase(storeFieldsOpt)) return true;
                List<String> storeList = Arrays.asList(storeFieldsOpt.toLowerCase().split("\\s*,\\s*"));
                if (storeList.contains(fieldName.toLowerCase())) return true;
                // If storeFields is specified but doesn't list this field, and no "*" or "ALL", assume don't store (unless dontStoreFields also doesn't list it).
                // This means explicit list in storeFields acts as a whitelist if present.
                return false;
            }
        }
        // Default if no specific rules found: DO NOT STORE fields unless specified.
        return false;
    }

    /**
     * Determines if a field should have DocValues for sorting.
     * Convention:
     * - "sortableFields": "*" or "ALL" (less common for global sortability).
     * - "sortableFields": "fieldA,fieldB".
     * - "sort.<fieldName>": "true" or "false".
     * Defaults to false.
     */
    private boolean isToSort(IndexDefinition indexDefinition, String fieldName, com.arcadedb.document.Document metadata) {
        Map<String, String> options = indexDefinition.getOptions();
        // Query-time metadata can override index-time options
        if (metadata != null) {
            Object fieldSpecificSortMeta = metadata.get("sort." + fieldName);
            if (fieldSpecificSortMeta != null) return Boolean.parseBoolean(fieldSpecificSortMeta.toString());

            List<String> querySortableFields = metadata.get("sortableFields"); // Assuming list of strings
            if (querySortableFields != null) {
                 if (querySortableFields.contains("*") || querySortableFields.contains("ALL")) return true;
                 if (querySortableFields.contains(fieldName)) return true;
            }
        }

        // Index definition options
        if (options != null) {
            String fieldSpecificSortOpt = options.get("sort." + fieldName);
            if (fieldSpecificSortOpt != null) return Boolean.parseBoolean(fieldSpecificSortOpt);

            String sortableFieldsOpt = options.get("sortableFields");
            if (sortableFieldsOpt != null) {
                if ("*".equals(sortableFieldsOpt) || "ALL".equalsIgnoreCase(sortableFieldsOpt)) return true;
                List<String> sortList = Arrays.asList(sortableFieldsOpt.toLowerCase().split("\\s*,\\s*"));
                if (sortList.contains(fieldName.toLowerCase())) return true;
                 // If sortableFields is specified but doesn't list this field, and no "*" or "ALL", assume not sortable.
                return false;
            }
        }
        return false; // Default to not sortable
    }
}
