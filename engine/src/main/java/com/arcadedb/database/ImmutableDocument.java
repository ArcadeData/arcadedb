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
package com.arcadedb.database;

import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;

/**
 * Immutable document implementation. To modify the record, you need to get the mutable representation by calling {@link #modify()}. This implementation keeps the
 * information in a byte[] to reduce the amount of objects to be managed by the Garbage Collector. For recurrent access to the record property you could evaluate
 * to return the mutable version of it that is backed by an internal map where the record properties are cached in RAM.
 *
 * @author Luca Garulli
 */
public class ImmutableDocument extends BaseDocument {

  protected ImmutableDocument(final Database graph, final DocumentType type, final RID rid, final Binary buffer) {
    super(graph, type, rid, buffer);
  }

  @Override
  public boolean has(final String propertyName) {
    if (propertyName == null)
      return false;

    checkForLazyLoading();
    return database.getSerializer().hasProperty(database, buffer, propertyName, rid);
  }

  @Override
  public Object get(final String propertyName) {
    if (propertyName == null)
      return null;

    checkForLazyLoading();
    try {
      return database.getSerializer()
          .deserializeProperty(database, buffer, new EmbeddedModifierProperty(this, propertyName), propertyName, rid);
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on loading property '%s' from record %s", e, propertyName, rid);
      return null;
    }
  }

  @Override
  public MutableDocument modify() {
    final Record recordInCache = database.getTransaction().getRecordFromCache(rid);
    if (recordInCache != null) {
      if (recordInCache instanceof MutableDocument document)
        return document;
      else if (!database.getTransaction().hasPageForRecord(rid.getPageId(database))) {
        // THE RECORD IS NOT IN TX, SO IT MUST HAVE BEEN LOADED WITHOUT A TX OR PASSED FROM ANOTHER TX
        // IT MUST BE RELOADED TO GET THE LATEST CHANGES. FORCE RELOAD
        try {
          // RELOAD THE PAGE FIRST TO AVOID LOOP WITH TRIGGERS (ENCRYPTION)
          database.getTransaction()
              .getPageToModify(rid.getPageId(database), ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).getPageSize(),
                  false);
          reload();
        } catch (final IOException e) {
          throw new DatabaseOperationException("Error on reloading document " + rid, e);
        }
      }
    }

    checkForLazyLoading();
    buffer.rewind();
    return new MutableDocument(database, type, rid, buffer.copyOfContent());
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    checkForLazyLoading();
    final Map<String, Object> map = database.getSerializer()
        .deserializeProperties(database, buffer, new EmbeddedModifierObject(this), rid);
    final JSONObject result = new JsonSerializer(database).map2json(map, type, includeMetadata);
    if (includeMetadata) {
      result.put(CAT_PROPERTY, "d");
      result.put(TYPE_PROPERTY, type.getName());
      if (getIdentity() != null)
        result.put(RID_PROPERTY, getIdentity().toString());
    }
    return result;
  }

  /**
   * Lazy-loads before reading, like every other accessor on this class ({@link #get}, {@link #has},
   * {@link #getPropertyNames}, {@link #toJSON}). Without it a record that had not been materialised yet - which is
   * every record handed out by {@code iterateType()} / {@code scanBucket()} - answered an EMPTY map instead of its
   * properties, silently. That is what made {@code copyType()} copy the right NUMBER of records and none of their
   * content (issue #5723), and what left {@link DetachedDocument} empty when detaching a record straight off a scan.
   * <p>
   * The empty map remains the answer for a record that genuinely cannot be materialised: no database, no RID to load
   * from, or an after-read event that filtered the record away.
   */
  @Override
  public Map<String, Object> propertiesAsMap() {
    if (database == null || (buffer == null && rid == null))
      return Collections.emptyMap();
    checkForLazyLoading();
    if (buffer == null)
      return Collections.emptyMap();
    // BELT AND BRACES: checkForLazyLoading() already guarantees this position (see its Javadoc), but the failure mode
    // of getting it wrong here is a SILENTLY empty map - which is how #5723 made copyType() copy empty records
    buffer.position(propertiesStartingPosition);
    return database.getSerializer().deserializeProperties(database, buffer, new EmbeddedModifierObject(this), rid);
  }

  /**
   * Returns only the specified properties as a map, using selective deserialization.
   * This avoids deserializing all properties when only a few are needed (OLAP optimization).
   *
   * @param fieldNames the property names to deserialize
   *
   * @return map of property name to value for the requested fields only
   */
  public Map<String, Object> propertiesAsMap(final String... fieldNames) {
    if (database == null || (buffer == null && rid == null))
      return Collections.emptyMap();
    if (fieldNames == null || fieldNames.length == 0)
      return propertiesAsMap();
    checkForLazyLoading();
    if (buffer == null)
      return Collections.emptyMap();
    buffer.position(propertiesStartingPosition);
    return database.getSerializer().deserializeProperties(database, buffer, new EmbeddedModifierObject(this), rid, fieldNames);
  }

  @Override
  public Map<String, Object> toMap() {
    return toMap(true);
  }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) {
    checkForLazyLoading();
    final Map<String, Object> result = new LinkedHashMap<>(
        database.getSerializer().deserializeProperties(database, buffer, new EmbeddedModifierObject(this), rid));
    if (includeMetadata) {
      result.put(CAT_PROPERTY, "d");
      result.put(TYPE_PROPERTY, type.getName());
      if (getIdentity() != null)
        result.put(RID_PROPERTY, getIdentity().toString());
    }
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder output = new StringBuilder(256);
    if (rid != null)
      output.append(rid);
    output.append('[');
    if (buffer == null)
      output.append('?');
    else {
      final int currPosition = buffer.position();

      try {
        buffer.position(propertiesStartingPosition);
        final Map<String, Object> map = this.database.getSerializer()
            .deserializeProperties(database, buffer, new EmbeddedModifierObject(this), rid);

        buffer.position(currPosition);

        int i = 0;
        for (final Map.Entry<String, Object> entry : map.entrySet()) {
          if (i > 0)
            output.append(',');

          output.append(entry.getKey());
          output.append('=');

          final Object v = entry.getValue();
          if (v != null && v.getClass().isArray()) {
            output.append('[');
            output.append(Array.getLength(v));
            output.append(']');
          } else
            output.append(v);
          i++;
        }
      } catch (Exception e) {
        output.append("corrupted?");
      }
    }
    output.append(']');
    return output.toString();
  }

  @Override
  public Set<String> getPropertyNames() {
    checkForLazyLoading();
    return database.getSerializer().getPropertyNames(database, buffer, rid);
  }

  /**
   * Materialises the record if it has not been loaded yet.
   * <p>
   * <b>Postcondition:</b> on return, either {@code buffer} is {@code null} - the record was filtered away by an
   * after-read event - or it is positioned at {@link #propertiesStartingPosition}, the first byte of the properties
   * section. Every accessor of this class relies on that: {@link #toJSON}, {@link #getPropertyNames}, {@link #toMap},
   * {@link #has} and {@link #get} all read straight from the current position without seeking first.
   * <p>
   * The path that made the postcondition conditional was the one where an after-read listener returns a
   * <i>different</i> record (the encryption hook). The replacement buffer comes from
   * {@code BinarySerializer.serializeDocument()}, which leaves it at position 1 when it could just copy the record's
   * own buffer, but at 0 when it had to re-serialise the properties of a dirty {@link MutableDocument} - the closing
   * {@code header.flip()}. In the second case the record-type byte was still ahead of the read cursor and the next
   * reader consumed it as the first byte of the header size (issue #5755). It stayed invisible because the only
   * in-tree listener of that shape decrypts <i>vertices</i>, and {@code ImmutableVertex.checkForLazyLoading()} re-reads
   * the fixed edge-pointer prefix from position 1 afterwards, repositioning as a side effect. Plain documents have no
   * such second pass.
   * <p>
   * That same branch also hands back {@code DatabaseContext.getTemporaryBuffer1()}, the per-thread scratch buffer the
   * serializer {@code clear()}s and reuses on every call, so the buffer has to be copied out before this record can
   * keep it - otherwise the next unrelated save on the same thread rewrites this record's content underneath it, and
   * the record starts answering with another record's values.
   *
   * @return {@code true} if the record was materialised by this call, {@code false} if it was already loaded or was
   * filtered away by an after-read event
   */
  protected boolean checkForLazyLoading() {
    if (buffer == null) {
      if (rid == null)
        throw new DatabaseOperationException("Document cannot be loaded because RID is null");

      buffer = database.getSchema().getBucketById(rid.getBucketId()).getRecord(rid);
      buffer.position(propertiesStartingPosition);

      final Record loaded = database.invokeAfterReadEvents(this);
      if (loaded == null) {
        buffer = null;
        return false;
      } else if (loaded != this) {
        // CREATE A BUFFER FROM THE MODIFIED RECORD. THIS IS NEEDED FOR ENCRYPTION THAT UPDATE THE RECORD WITH A MUTABLE.
        // getNotReusable() IS MANDATORY: FOR A DIRTY RECORD THE SERIALIZER RETURNS THE PER-THREAD SCRATCH BUFFER, WHICH
        // IT COPIES OUT, WHILE THE ALREADY-PRIVATE BUFFER OF THE OTHER BRANCH IS KEPT AS IS
        buffer = database.getSerializer().serialize(database, loaded).getNotReusable();
        // THE SERIALIZER HANDS THE BUFFER BACK AT 0 OR AT 1 DEPENDING ON THE BRANCH IT TOOK: NORMALISE IT SO THE
        // POSTCONDITION OF THIS METHOD HOLDS ON EVERY PATH
        buffer.position(propertiesStartingPosition);
      }

      return true;
    }

    buffer.position(propertiesStartingPosition);
    return false;
  }
}
