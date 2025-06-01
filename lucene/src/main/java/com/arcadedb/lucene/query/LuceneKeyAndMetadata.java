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
package com.arcadedb.lucene.query;

import com.arcadedb.document.Document;
import com.arcadedb.index.CompositeKey; // If key can be a CompositeKey
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Map;

/**
 * A container to pass a query key (which can be a simple string,
 * a CompositeKey, or other structures) along with associated metadata Document.
 * The metadata can include options for highlighting, sorting, limits, etc.
 */
public class LuceneKeyAndMetadata {

    public final Object key;
    public final Document metadata;
    private CommandContext context; // Optional command context

    /**
     * Constructor.
     *
     * @param key      The main query key (e.g., String, CompositeKey).
     * @param metadata A Document containing additional query parameters and options.
     */
    public LuceneKeyAndMetadata(Object key, Document metadata) {
        this.key = key;
        this.metadata = metadata != null ? metadata : new Document(null); // Ensure metadata is never null
    }

    /**
     * Constructor with command context.
     *
     * @param key      The main query key.
     * @param metadata A Document containing additional query parameters.
     * @param context  The SQL command execution context.
     */
    public LuceneKeyAndMetadata(Object key, Document metadata, CommandContext context) {
        this.key = key;
        this.metadata = metadata != null ? metadata : new Document(null); // Ensure metadata is never null
        this.context = context;
    }


    public Object getKey() {
        return key;
    }

    public Document getMetadata() {
        return metadata;
    }

    public CommandContext getContext() {
        return context;
    }

    public LuceneKeyAndMetadata setContext(CommandContext context) {
        this.context = context;
        return this;
    }

    /**
     * Helper to get metadata as a Map, typically for options.
     * @return Map representation of metadata, or empty map if null.
     */
    public Map<String, Object> getMetadataAsMap() {
        return this.metadata.toMap();
    }
}
