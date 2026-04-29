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
 * Lightweight record used by the serializer to store the value of a property flagged EXTERNAL in a paired bucket.
 * Holds an opaque pre-serialized buffer of the form `[RECORD_TYPE_EXTERNAL][value type byte][value bytes]`. The
 * leading record-type byte follows the convention of edge segments: the buffer is written verbatim by the bucket and
 * we do the framing ourselves.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ExternalValueRecord extends BaseRecord implements RecordInternal {
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

  /**
   * Returns the buffer with the RECORD_TYPE_EXTERNAL marker at byte 0 and the value blob ([type][value bytes]) following.
   */
  public Binary getContent() {
    return buffer;
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    // Internal infrastructure record - no user-visible JSON form. Returning empty keeps callers like generic record
    // dumpers safe even though they should never reach an EXTERNAL value record directly.
    return new JSONObject();
  }
}
