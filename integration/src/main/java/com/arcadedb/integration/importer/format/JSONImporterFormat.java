/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.integration.importer.format;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.google.gson.stream.JsonToken.BEGIN_ARRAY;
import static com.google.gson.stream.JsonToken.BEGIN_OBJECT;
import static com.google.gson.stream.JsonToken.END_ARRAY;
import static com.google.gson.stream.JsonToken.END_OBJECT;

/**
 * On {@code -onRowError skip}, {@code parseRecords} always begins a fresh, nested transaction per record ({@code
 * database.begin()} pushes an independent {@code TransactionContext} rather than reusing an active one - see {@code
 * LocalDatabase#begin()}), so its own commit/rollback can never affect a pre-existing caller transaction. See
 * {@link ImporterSettings#isSkipOnRowError()} for the full contract.
 */
public class JSONImporterFormat implements FormatImporter {
  static class CascadingProperties {
    final CascadingProperties parent;
    final Map<String, Object> map;

    public CascadingProperties(final CascadingProperties parent, final Map<String, Object> map) {
      this.parent = parent;
      this.map = map;
    }
  }

  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.EntityType entityType, final Parser parser,
      final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws IOException {

    final JSONObject mapping = settings.mapping != null ? new JSONObject(settings.mapping) : null;

    JsonToken waitFor = null;
    Object tagValue = null;

    try (final JsonReader reader = new Gson().newJsonReader(parser.getReader())) {
      while (reader.hasNext()) {
        JsonToken token = reader.peek();

        if (mapping == null) {
          // A single top-level object: there is no sibling record to continue with on failure, so -onRowError skip
          // has no recovery to do here - any error still aborts the import.
          if (settings.isSkipOnRowError())
            LogManager.instance()
                .log(this, Level.INFO,
                    "-onRowError skip has no effect on a single top-level JSON object (no -mapping set): there is no "
                        + "sibling record to continue with, so any error still aborts the import");
          // Discarded rather than checked here: safe because createRecord() returns null immediately whenever
          // mapping is null, before any type conversion that could actually throw.
          final Object record = parseRecord(reader, settings, context, database, mapping, false, new AtomicBoolean());
          if (record instanceof Map)
            saveAnonymousRecord(database, settings, (Map<String, Object>) record);
          return;
        }

        switch (token) {
        case BEGIN_OBJECT:
          reader.beginObject();
          break;
        case END_OBJECT:
          reader.endObject();
        case BEGIN_ARRAY:
          parseRecords(reader, database, settings, context, (JSONArray) tagValue, waitFor != token);
          break;
        case NAME:
          final String tag = reader.nextName();
          if (mapping.has(tag) || mapping.has("*")) {
            tagValue = mapping.has(tag) ? mapping.get(tag) : mapping.get("*");
            if (tagValue instanceof JSONArray)
              waitFor = BEGIN_ARRAY;
            else if (tagValue instanceof JSONObject)
              waitFor = BEGIN_OBJECT;
          }
        }
      }
    }
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.EntityType entityType, final Parser parser, final ImporterSettings settings,
      final AnalyzedSchema analyzedSchema) {
    return new SourceSchema(this, parser.getSource(), null);
  }

  @Override
  public String getFormat() {
    return "JSON";
  }

  private void parseRecords(final JsonReader reader, final Database database, final ImporterSettings settings,
      final ImporterContext context,
      final JSONArray mapping, boolean ignore) throws IOException {
    // Each record below commits/rolls back its own nested transaction (database.begin() nests rather than reusing
    // an already-active one - see LocalDatabase#begin()) - but a nested commit() is still independently durable, so
    // on an externally-managed database that could persist a record even if the caller's own transaction later
    // fails. Fail loudly instead; callerTransactionActiveOnEntry is the signal for that (see its Javadoc).
    if (settings.isSkipOnRowError() && context.callerTransactionActiveOnEntry)
      throw ImporterSettings.newExclusiveTransactionRequiredException();

    reader.beginArray();

    database.begin();
    try {
      parseRecordsArray(reader, database, settings, context, mapping, ignore);
    } catch (final IOException e) {
      // A genuinely source-level failure never passes through parseRecordsArray()'s per-record catch below (which
      // only catches RuntimeException), so the active transaction here is always this method's own, untouched
      // level - safe to roll back unconditionally.
      if (database.isTransactionActive())
        database.rollback();
      throw e;
    } catch (final RuntimeException e) {
      // Any RuntimeException reaching here has already passed through parseRecordsArray()'s per-record catch, which
      // (in "abort" mode, the only mode where it rethrows) already rolled back its own nested level. Don't roll back
      // again here: by this point the transaction stack may have already correctly unwound past our own level to a
      // caller's own still-active transaction (see parseRecordsArray()'s per-record catch for why that's safe to
      // leave alone) - rolling back again here can't tell that apart from a genuinely dangling level of our own, and
      // would discard the caller's unrelated pending work instead.
      throw e;
    }
  }

  private void parseRecordsArray(final JsonReader reader, final Database database, final ImporterSettings settings,
      final ImporterContext context, final JSONArray mapping, boolean ignore) throws IOException {
    final Object mappingValue = mapping != null && !mapping.isEmpty() ? mapping.get(0) : null;
    JSONObject mappingObject;

    long recordIndex = 0;
    while (true) {
      final JsonToken next;
      try {
        next = reader.peek();
      } catch (final RuntimeException e) {
        // Right here, the active transaction is always this loop's own level, just begun and not yet touched by
        // anything else this iteration (either parseRecords()'s initial database.begin() or this loop's own, at the
        // end of the previous iteration) - unlike a RuntimeException reaching parseRecords()'s outer catch after the
        // per-record try/catch below has already run (see there), there's no ambiguity here about whose level this
        // is, so it's unconditionally safe to roll back.
        if (database.isTransactionActive())
          database.rollback();
        throw e;
      }
      if (next != BEGIN_OBJECT)
        break;
      ++recordIndex;

      if (mappingValue instanceof JSONObject object) {
        mappingObject = object;
        ignore = false;
      } else
        mappingObject = null;

      // Set by parseRecord()/parseArray() if a nested BEGIN_OBJECT/BEGIN_ARRAY recursion failed somewhere below this
      // record: the only safe way to discard that partial write is to roll back this whole top-level record's own
      // transaction below.
      final AtomicBoolean recordFailed = new AtomicBoolean();

      // createRecord()/convertMap() count a new document/vertex/edge as soon as it is allocated, well before
      // save()/commit(), either of which can still fail - snapshot the counters so a rolled-back record can have
      // its counts undone below instead of leaking into the summary.
      final long createdDocumentsBefore = context.createdDocuments.get();
      final long createdVerticesBefore = context.createdVertices.get();
      final long createdEdgesBefore = context.createdEdges.get();

      try {
        final Object record = parseRecord(reader, settings, context, database, mappingObject, ignore, recordFailed);
        if (recordFailed.get())
          throw new ImportException("A nested object/array failed to import, skipping the whole record", null);

        if (record instanceof Map)
          saveAnonymousRecord(database, settings, (Map<String, Object>) record);

        database.commit();
      } catch (final RuntimeException e) {
        if (database.isTransactionActive())
          database.rollback();

        context.createdDocuments.set(createdDocumentsBefore);
        context.createdVertices.set(createdVerticesBefore);
        context.createdEdges.set(createdEdgesBefore);

        if (!settings.isSkipOnRowError())
          throw e;

        // The underlying JsonReader is positioned right after the offending record, so it's safe to continue with
        // the next array entry.
        logSkippedRecord("JSON record #" + recordIndex, e);
        context.errors.incrementAndGet();
      }

      database.begin();
    }

    database.commit();

    reader.endArray();
  }

  private static MutableDocument saveAnonymousRecord(final Database database, final ImporterSettings settings,
      final Map<String, Object> map) {
    // NO MAPPING, SAVE THE RECORD AS A DOCUMENT
    database.getSchema().getOrCreateDocumentType(settings.documentTypeName);
    return database.newDocument(settings.documentTypeName).set(map).save();
  }

  /**
   * Logs a skipped record/entry at WARNING (message only) and FINE (full stack trace), used wherever
   * {@code -onRowError skip} discards a JSON record, property, or array entry.
   */
  private void logSkippedRecord(final String what, final RuntimeException e) {
    LogManager.instance().log(this, Level.WARNING, "Error on importing %s, skipping it (reason: %s)", null, what, e.getMessage());
    LogManager.instance().log(this, Level.FINE, "Full error on importing %s", e, what);
  }

  private Object parseRecord(final JsonReader reader, final ImporterSettings settings, final ImporterContext context,
      final Database database,
      final JSONObject mapping, final boolean ignore, final AtomicBoolean recordFailed) throws IOException {
    final CascadingProperties attributes = ignore ? null : new CascadingProperties(null, new LinkedHashMap<>());

    context.parsed.incrementAndGet();

    reader.beginObject();
    while (reader.peek() != END_OBJECT) {
      final String attributeName = reader.nextName();
      Object attributeValue;

      final JsonToken propertyType = reader.peek();
      switch (propertyType) {
      case STRING:
        attributeValue = reader.nextString();
        break;
      case NUMBER:
        attributeValue = reader.nextDouble();
        break;
      case BOOLEAN:
        attributeValue = reader.nextBoolean();
        break;
      case NULL:
        reader.nextNull();
        attributeValue = null;
        break;
      case BEGIN_OBJECT:
        boolean ignoreObject = ignore;

        JSONObject mappingObject = null;
        if (mapping != null && mapping.has(attributeName)) {
          final Object mappingValue = mapping.get(attributeName);
          if (mappingValue instanceof JSONObject object)
            mappingObject = object;
          else if (mappingValue instanceof String && "@ignore".equals(mappingValue.toString()))
            ignoreObject = true;
        }
        // Must be caught here, at the call site, not left to unwind through this object's own while loop: by the
        // time it's caught in parseRecords() the outer reader.endObject() below would have been skipped, desyncing
        // the stream for the rest of the array. Setting recordFailed makes parseRecords() discard the whole
        // enclosing top-level record via its normal rollback path instead.
        try {
          attributeValue = parseRecord(reader, settings, context, database, mappingObject, ignoreObject, recordFailed);
        } catch (final RuntimeException e) {
          if (!settings.isSkipOnRowError())
            throw e;
          logSkippedRecord("nested JSON object property '" + attributeName + "'", e);
          recordFailed.set(true);
          attributeValue = null;
        }
        break;

      case BEGIN_ARRAY: {
        final JSONArray mappingArray = mapping != null && mapping.has(attributeName) ? mapping.getJSONArray(attributeName) : null;
        try {
          attributeValue = parseArray(reader, settings, context, database, mappingArray, ignore, recordFailed);
        } catch (final RuntimeException e) {
          if (!settings.isSkipOnRowError())
            throw e;
          logSkippedRecord("JSON array property '" + attributeName + "'", e);
          recordFailed.set(true);
          attributeValue = null;
        }
      }
      break;
      default:
        LogManager.instance().log(this, Level.WARNING, "Skipping property '%s' of type '%s'", attributeName, propertyType);
        context.errors.incrementAndGet();
        continue;
      }

      if (!ignore)
        attributes.map.put(attributeName, attributeValue);
    }

    reader.endObject();

    if (ignore)
      return null;

    resolveProperties(mapping, attributes);

    final Document record = createRecord(database, context, attributes, mapping, settings);
    if (record instanceof MutableDocument document) {
      document.save();
      return record;
    }

    return attributes.map;
  }

  private void resolveProperties(final JSONObject mapping, final CascadingProperties attributes) {
    if (mapping == null)
      return;

    for (Map.Entry<String, Object> entry : mapping.toMap().entrySet()) {
      if (entry.getKey().startsWith("@"))
        continue;
      final Object value = entry.getValue();
      if (value instanceof String string && string.startsWith("<") && string.endsWith(">")) {
        final String copyFrom = string.substring(1, string.length() - 1);
        attributes.map.put(entry.getKey(), getAttribute(attributes, copyFrom));
      }
    }
  }

  private Object getAttribute(final CascadingProperties properties, final String name) {
    if (properties == null)
      return null;

    if (name.startsWith("../"))
      return getAttribute(properties.parent, name.substring(3));

    return properties.map.get(name);
  }

  private Document createRecord(final Database database, final ImporterContext context, final CascadingProperties attributes,
      final JSONObject mapping,
      final ImporterSettings settings) {
    if (mapping == null)
      return null;
    //return saveAnonymousRecord(database, settings, (Map<String, Object>) attributes.map);

    if (!mapping.has(Property.CAT_PROPERTY)) {
      LogManager.instance()
          .log(this, Level.WARNING, "No @cat tag defined in mapping object. The following object will be skipped %s", attributes);
      context.errors.incrementAndGet();
      return null;
    }

    if (!mapping.has(Property.TYPE_PROPERTY)) {
      LogManager.instance()
          .log(this, Level.WARNING, "No @type tag defined in mapping object. The following object will be skipped %s", attributes);
      context.errors.incrementAndGet();
      return null;
    }

    String category = mapping.getString(Property.CAT_PROPERTY);
    String typeName = mapping.getString(Property.TYPE_PROPERTY);

    if (typeName.startsWith("<") && typeName.endsWith(">")) {
      // GET TYPE NAME FROM THE OBJECT
      typeName = typeName.substring(1, typeName.length() - 1);
      for (String tName : typeName.split(",")) {
        typeName = (String) getAttribute(attributes, tName);
        if (typeName != null)
          break;
      }
    }

    if (typeName == null) {
      LogManager.instance().log(this, Level.WARNING, "Type is null, skipping object %s", attributes);
      context.errors.incrementAndGet();
      return null;
    }

    final DocumentType type;
    switch (category) {
    case "v":
      type = database.getSchema().getOrCreateVertexType(typeName);
      break;
    case "d":
      type = database.getSchema().getOrCreateDocumentType(typeName);
      break;
    case "e":
      // IGNORE IN THIS PHASE, EDGES WILL BE MANAGED DURING MAPPING
      return null;
    default:
      LogManager.instance().log(this, Level.WARNING, "Record category '%s' not supported", category);
      context.errors.incrementAndGet();
      return null;
    }

    MutableDocument record = null;

    if (mapping.has("@id")) {
      final String id = mapping.getString("@id");
      final Object idValue = getAttribute(attributes, id);

      Property prop = type.getPropertyIfExists(id);
      if (prop == null) {
        if (idValue == null) {
          // NO ID FOUND, SKIP THE RECORD
          LogManager.instance()
              .log(this, Level.WARNING, "@id property not found on current record, skipping record: %s", attributes);
          context.errors.incrementAndGet();
          return null;
        }

        Type propType = Type.getTypeByValue(idValue);
        if (mapping.has("@idType"))
          propType = Type.getTypeByName(mapping.getString("@idType").toUpperCase(Locale.ENGLISH));

        prop = type.createProperty(id, propType);
      }

      prop.getOrCreateIndex(Schema.INDEX_TYPE.LSM_TREE, true);

      IndexCursor existent = database.lookupByKey(typeName, id, idValue);
      if (existent.hasNext()) {
        final String strategy = mapping.getString("@strategy", "");
        if ("merge".equalsIgnoreCase(strategy)) {
          record = existent.next().asDocument().modify();
        } else
          // SKIP IT, RETURN THE EXISTENT ONE
          return existent.next().asDocument();
      }
    }

    if (record == null) {
      switch (category) {
      case "v":
        record = database.newVertex(typeName);
        context.createdVertices.incrementAndGet();
        break;
      case "d":
        record = database.newDocument(typeName);
        context.createdDocuments.incrementAndGet();
        break;
      }
    }

    applyMappingRules(database, context, record, attributes, mapping, settings);

    final LinkedHashMap<String, Object> recordProperties = new LinkedHashMap<>(attributes.map);
    recordProperties.keySet().removeIf(name -> name.startsWith("@"));

    record.set(recordProperties);

    return record;
  }

  private void applyMappingRules(final Database database, final ImporterContext context, final MutableDocument record,
      final CascadingProperties attributes,
      final JSONObject mapping, final ImporterSettings settings) {
    resolveProperties(mapping, attributes);

    // CHECK FOR SPECIAL MAPPING
    for (String mappingName : mapping.keySet()) {
      final Object mappingValue = mapping.get(mappingName);
      final Object attributeValue = getAttribute(attributes, mappingName);

      if (attributeValue == null)
        continue;

      if (mappingValue instanceof JSONObject) {
//        if (!(attributeValue instanceof Map)) {
//          LogManager.instance()
//              .log(this, Level.WARNING, "Defined an object on mapping for property '%s' but found the object of class %s as attribute", mappingName,
//                  attributeValue.getClass());
//          context.errors.incrementAndGet();
//          continue;
//        }
        Object result = convertMap(database, context, record, attributeValue, mappingValue, attributes, settings);
        if (result instanceof Edge)
          // CONVERTED TO EDGE, REMOVE THE PROPERTY ENTIRELY
          attributes.map.remove(mappingName);

      } else if (mappingValue instanceof JSONArray array) {
        if (!(attributeValue instanceof Collection)) {
          LogManager.instance()
              .log(this, Level.WARNING,
                  "Defined an array on mapping for property '%s' but found the object of class %s as attribute", mappingName,
                  attributeValue.getClass());
          context.errors.incrementAndGet();
          continue;
        }

        final Object subMapping = array.get(0);
        for (Iterator<?> it = ((Collection<?>) attributeValue).iterator(); it.hasNext(); ) {
          final Object attributeArrayItem = it.next();
          Object result = convertMap(database, context, record, attributeArrayItem, subMapping, attributes, settings);
          if (result instanceof Edge)
            // CONVERTED TO EDGE, REMOVE THE PROPERTY ENTIRELY
            attributes.map.remove(mappingName);
        }
      } else if (mappingValue instanceof String && "@ignore".equals(mappingValue.toString())) {
        attributes.map.remove(mappingName);
      }

    }
  }

  private List<Object> parseArray(final JsonReader reader, final ImporterSettings settings, final ImporterContext context,
      final Database database,
      final JSONArray mapping, boolean ignore, final AtomicBoolean recordFailed) throws IOException {
    final List<Object> list = ignore ? null : new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != END_ARRAY) {
      Object entryValue;

      final JsonToken entryType = reader.peek();
      switch (entryType) {
      case STRING:
        entryValue = reader.nextString();
        break;
      case NUMBER:
        entryValue = reader.nextDouble();
        break;
      case BOOLEAN:
        entryValue = reader.nextBoolean();
        break;
      case NULL:
        reader.nextNull();
        entryValue = null;
        break;
      case BEGIN_OBJECT:
        final JSONObject mappingObject = mapping != null && !mapping.isEmpty() ? mapping.getJSONObject(0) : null;
        // Same reasoning as parseRecord()'s BEGIN_OBJECT case: catch here, at the call site, so a schema-conversion
        // failure on one array item cannot desync the reader for the rest of this array.
        try {
          entryValue = parseRecord(reader, settings, context, database, mappingObject, ignore, recordFailed);
        } catch (final RuntimeException e) {
          if (!settings.isSkipOnRowError())
            throw e;
          logSkippedRecord("JSON array entry", e);
          recordFailed.set(true);
          entryValue = null;
        }
        break;
      case BEGIN_ARRAY:
        final JSONArray mappingArray = mapping != null && !mapping.isEmpty() ? mapping.getJSONArray(0) : null;
        try {
          entryValue = parseArray(reader, settings, context, database, mappingArray, ignore, recordFailed);
        } catch (final RuntimeException e) {
          if (!settings.isSkipOnRowError())
            throw e;
          logSkippedRecord("nested JSON array entry", e);
          recordFailed.set(true);
          entryValue = null;
        }
        break;
      default:
        LogManager.instance().log(this, Level.WARNING, "Skipping entry of type '%s'", entryType);
        context.errors.incrementAndGet();
        continue;
      }

      if (!ignore)
        list.add(entryValue);
    }
    reader.endArray();

    return list;
  }

  private Object convertMap(final Database database, final ImporterContext context, final MutableDocument record,
      final Object value, final Object mapping,
      final CascadingProperties attributes, final ImporterSettings settings) {
    if (mapping instanceof JSONObject mappingObject) {

      final Map<String, Object> attributeMap;
      if (value instanceof Map)
        // CONVERT EMBEDDED MAP INTO A RECORD
        attributeMap = new LinkedHashMap<>((Map<String, Object>) value);
      else
        // TREAT THE VALUE AS ID
        attributeMap = new LinkedHashMap<>();

      final String subCategory = mappingObject.has(Property.CAT_PROPERTY) ? mappingObject.getString(Property.CAT_PROPERTY) : null;
      final String subTypeName = mappingObject.has(Property.TYPE_PROPERTY) ? mappingObject.getString(Property.TYPE_PROPERTY) : null;

      if ("e".equals(subCategory)) {
        // TRANSFORM INTO AN EDGE
        if (subTypeName == null) {
          LogManager.instance()
              .log(this, Level.WARNING, "Cannot convert object into an edge because the edge @type is not defined");
          context.errors.incrementAndGet();
          return null;
        }

        if (!(record instanceof Vertex)) {
          LogManager.instance()
              .log(this, Level.WARNING, "Cannot convert object into an edge because the root record is not a vertex");
          context.errors.incrementAndGet();
          return null;
        }

        final JSONObject destVertexMappingObject;
        final Object destVertexItem;

        if (mappingObject.has("@in")) {
          final Object inValue = mappingObject.get("@in");
          if (inValue instanceof String) {
            final String inVertex = inValue.toString();
            destVertexMappingObject = mappingObject.getJSONObject(inVertex);
            destVertexItem = attributeMap.get(inVertex);
          } else if (inValue instanceof JSONObject object) {
            destVertexMappingObject = object;
            attributeMap.put((String) destVertexMappingObject.get("@id"), value);
            destVertexItem = attributeMap;
          } else {
            LogManager.instance()
                .log(this, Level.WARNING,
                    "Cannot convert object into an edge because the destination vertx @in type is not supported: " + inValue);
            context.errors.incrementAndGet();
            return null;
          }
        } else {
          LogManager.instance()
              .log(this, Level.WARNING, "Cannot convert object into an edge because the destination vertx @in is not defined");
          context.errors.incrementAndGet();
          return null;
        }

        final MutableVertex destVertex;
        if (destVertexItem instanceof Document)
          destVertex = (MutableVertex) destVertexItem;
        else if (destVertexItem instanceof Map) {
          destVertex = (MutableVertex) createRecord(record.getDatabase(), context,
              new CascadingProperties(attributes, (Map<String, Object>) destVertexItem),
              destVertexMappingObject, settings);
          if (destVertex == null) {
            LogManager.instance().log(this, Level.WARNING, "Cannot convert inner map into destination vertex: %s", destVertexItem);
            context.errors.incrementAndGet();
            return null;
          }
        } else {
          LogManager.instance().log(this, Level.WARNING, "Cannot convert object " + destVertexItem + " into a record");
          context.errors.incrementAndGet();
          return null;
        }

        record.save();
        destVertex.save();

        database.getSchema().getOrCreateEdgeType(subTypeName);

        final String cardinality = mappingObject.getString("@cardinality", "");
        if ("no-duplicates".equalsIgnoreCase(cardinality)) {
          boolean duplicates = false;
          for (Iterator<Vertex> connectedVertices = ((Vertex) record).getVertices(Vertex.DIRECTION.OUT, subTypeName)
              .iterator(); connectedVertices.hasNext(); ) {
            final RID connectedVertex = connectedVertices.next().getIdentity();
            if (destVertex.getIdentity().equals(connectedVertex)) {
              duplicates = true;
              break;
            }
          }

          if (duplicates) {
            context.skippedEdges.incrementAndGet();
            return null;
          }
        }

        final MutableEdge edge = ((Vertex) record).newEdge(subTypeName, destVertex);

        attributeMap.keySet().removeIf(name -> name.startsWith("@"));
        edge.set(attributeMap);
        edge.save();

        context.createdEdges.incrementAndGet();

        return edge;
      }
    }
    return null;
  }
}
