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
import com.arcadedb.database.DatabaseInternal; // Required for schema access
import com.arcadedb.document.Document; // ArcadeDB Document
import com.arcadedb.index.CompositeKey;
import com.arcadedb.index.IndexDefinition;
import com.arcadedb.lucene.parser.ArcadeLuceneMultiFieldQueryParser; // FIXME: Needs refactoring
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery; // For string ranges, newStringRange
import org.apache.lucene.index.Term;
// Import Point field range queries
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.DoublePoint;


public class LuceneQueryBuilder {

    private static final Logger logger = Logger.getLogger(LuceneQueryBuilder.class.getName());
    public static final Document EMPTY_METADATA = new Document(null); // Assuming Document can be db-less for this constant

    private final boolean allowLeadingWildcard;
    private final boolean splitOnWhitespace;

    public LuceneQueryBuilder(Document metadata) {
        if (metadata == null) {
            metadata = EMPTY_METADATA;
        }
        this.allowLeadingWildcard = Boolean.TRUE.equals(metadata.get("allowLeadingWildcard"));
        // Lucene's StandardQueryParser and MultiFieldQueryParser split on whitespace by default.
        // This setting in OrientDB was more about how the string was fed *to* the parser or if specific syntax implied no split.
        // For now, assuming default Lucene behavior is mostly fine. If specific "phrase" vs "term" logic is needed from splitOnWhitespace,
        // it would affect how the query string is constructed or which parser is used.
        this.splitOnWhitespace = Boolean.TRUE.equals(metadata.get("splitOnWhitespace")); // Default true
    }

    public Query query(IndexDefinition indexDefinition, Object key, Document metadata, Analyzer analyzer, DatabaseInternal database) throws ParseException {
        if (key == null) {
            throw new IllegalArgumentException("Query key cannot be null");
        }
        if (metadata == null) {
            metadata = EMPTY_METADATA;
        }

        String[] fields = indexDefinition.getFields().toArray(new String[0]);
        if (fields.length == 0) {
            // Default to a common field if not specified, e.g. "_all" or a convention
            // This case needs clarification based on how schema-less Lucene indexes were handled.
            // For now, let's assume if no fields, it might be a special query type or error.
            // Or, if key is string, it searches default fields of the parser.
            // For now, if no fields defined in index, and key is String, let parser use its default field.
            // This requires parser to be configured with default field(s).
            // fields = new String[] { "_DEFAULT_SEARCH_FIELD" }; // Placeholder for default search field
             logger.warning("Querying Lucene index " + indexDefinition.getName() + " with no fields defined in index definition. Query may not behave as expected.");
        }

        Map<String, Type> fieldTypes = new HashMap<>();
        if (database != null && indexDefinition.getTypeName() != null) {
            Schema schema = database.getSchema();
            DocumentType docType = schema.getType(indexDefinition.getTypeName());
            if (docType != null) {
                for (String fieldName : indexDefinition.getFields()) {
                    Property prop = docType.getProperty(fieldName);
                    if (prop != null) {
                        fieldTypes.put(fieldName, prop.getType());
                    } else {
                        fieldTypes.put(fieldName, Type.STRING); // Default if property not found in schema
                    }
                }
            } else {
                 for (String fieldName : indexDefinition.getFields()) {
                    fieldTypes.put(fieldName, Type.STRING); // Default if type not found
                }
            }
        } else {
             for (String fieldName : indexDefinition.getFields()) {
                fieldTypes.put(fieldName, Type.STRING); // Default if no DB or typeName
            }
        }


        if (key instanceof String) {
            // ArcadeLuceneMultiFieldQueryParser is now available.
            ArcadeLuceneMultiFieldQueryParser parser = new ArcadeLuceneMultiFieldQueryParser(fieldTypes, fields, analyzer);
            parser.setAllowLeadingWildcard(allowLeadingWildcard);
            // this.splitOnWhitespace is available but MultiFieldQueryParser handles split on whitespace by default.
            // If specific behavior like "always phrase if false" is needed, parser logic would be more complex.
            // For now, assuming standard MFQP behavior is sufficient.
            // if (!this.splitOnWhitespace) { /* Potentially use different parser or pre-process query string */ }

            Map<String, Float> boost = metadata.get("boost", Map.class);
            if (boost != null) {
                parser.setBoosts(boost);
            }
            return parser.parse((String) key);

        } else if (key instanceof CompositeKey) {
            CompositeKey compositeKey = (CompositeKey) key;
            List<Object> keys = compositeKey.getKeys();
            BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();

            if (keys.size() != fields.length) {
                throw new IllegalArgumentException("CompositeKey size does not match index definition fields count.");
            }

            for (int i = 0; i < keys.size(); i++) {
                Object partKey = keys.get(i);
                String fieldName = fields[i];
                Type fieldType = fieldTypes.getOrDefault(fieldName, Type.STRING);

                if (partKey != null) {
                    Query partQuery = com.arcadedb.lucene.index.ArcadeLuceneIndexType.createExactFieldQuery(fieldName, partKey, fieldType, database);
                    booleanQuery.add(partQuery, BooleanClause.Occur.MUST);
                }
            }
            return booleanQuery.build();
        }
        // FIXME: Add support for specific range query objects if defined (this would be a new key instanceof MyCustomRangeObject)
        // else if (key instanceof ...) {
        // MyCustomRange range = (MyCustomRange) key;
        // String fieldName = range.getField();
        // Type fieldType = fieldTypes.getOrDefault(fieldName, Type.STRING);
        // if (fieldType.isNumeric()) {
        //     if (fieldType == Type.LONG || fieldType == Type.INTEGER || fieldType == Type.SHORT || fieldType == Type.BYTE || fieldType == Type.DATETIME || fieldType == Type.DATE) {
        //         return LongPoint.newRangeQuery(fieldName, (Long)range.getLower(), (Long)range.getUpper());
        //     } // Add other numeric types
        // } else if (fieldType == Type.STRING) {
        //     return TermRangeQuery.newStringRange(fieldName, range.getLower().toString(), range.getUpper().toString(), range.isLowerInclusive(), range.isUpperInclusive());
        // }
        // }

        // Default fallback or throw exception for unsupported key types
        logger.warning("Unsupported key type for Lucene query: " + key.getClass().getName() + ". Attempting TermQuery on toString().");
        return new TermQuery(new Term(fields.length > 0 ? fields[0] : "_DEFAULT_", key.toString())); // Fallback, likely not useful
    }
}
