package com.arcadedb.schema;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

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
 */
public class IndexMetadata {
  public static final String COLLATION_CI      = "CI";
  public static final String COLLATION_DEFAULT = "DEFAULT";

  public String       typeName;
  public List<String> propertyNames;
  public List<String> collations;
  public int          associatedBucketId;

  public IndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    this.typeName = typeName;
    this.propertyNames = propertyNames != null ? List.of(propertyNames) : List.of();
    this.associatedBucketId = bucketId;
  }

  public void fromJSON(final JSONObject metadata) {
    typeName = metadata.getString("typeName");
    propertyNames = metadata.getJSONArray("properties").toListOfStrings();
    associatedBucketId = metadata.getInt("associatedBucketId", -1);
    final JSONArray collationsJSON = metadata.getJSONArray("collations", null);
    if (collationsJSON != null)
      collations = collationsJSON.toListOfStrings();
  }

  /**
   * Returns true if the property at the given index has case-insensitive collation.
   */
  public boolean isCaseInsensitive(final int propertyIndex) {
    return collations != null && propertyIndex < collations.size()
        && COLLATION_CI.equals(collations.get(propertyIndex));
  }

  /**
   * Returns true if any property in this index has case-insensitive collation.
   */
  public boolean hasAnyCaseInsensitive() {
    if (collations == null)
      return false;
    for (final String c : collations)
      if (COLLATION_CI.equals(c))
        return true;
    return false;
  }
}
