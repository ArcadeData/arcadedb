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

import com.arcadedb.serializer.json.JSONObject;

/**
 * Opaque payload record for EXTERNAL property values. Buffer = [RECORD_TYPE][value type][value bytes].
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ExternalValueRecord extends BaseRecord implements RecordInternal {
  // BaseRecord-subclass type tags (first byte of every record buffer):
  //   Document            = 0  (Document.java)
  //   Vertex              = 1  (Vertex.java)
  //   Edge                = 2  (Edge.java)
  //   EdgeSegment         = 3  (EdgeSegment.java / MutableEdgeSegment.java)
  //   EmbeddedDocument    = 4  (EmbeddedDocument.java)
  //   ExternalValueRecord = 5  (this class - paired-bucket payload for EXTERNAL property values)
  //   LightEdge           = 6  (LightEdge.java)
  // Add new values at the end and update this list.
  public static final byte RECORD_TYPE = 5;

  public ExternalValueRecord(final Database database, final RID rid, final Binary buffer) {
    super(database, rid, buffer);
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }

  @Override
  public void setIdentity(final RID rid) {
    this.rid = upgradeRID(rid);
  }

  @Override
  public void unsetDirty() {
    // NO-OP: BUFFER IS BUILT FRESH ON EACH SERIALIZE
  }

  public Binary getContent() {
    return buffer;
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    // ExternalValueRecord is engine-internal infrastructure. It's only reachable through a primary record's
    // TYPE_EXTERNAL pointer, and every public read path (Document.toJSON, REST handlers, Studio) walks through
    // BinarySerializer.deserializeProperty which materialises the value inline. If a generic record dumper hits
    // this method directly, it's a bug: failing loud surfaces it instead of returning silent empty data.
    throw new UnsupportedOperationException(
        "ExternalValueRecord is an internal payload of an EXTERNAL property and has no JSON representation; "
            + "read the value through its owning Document instead of dereferencing the external RID directly");
  }
}
