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
package com.arcadedb.remote;

import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;

import java.util.*;

public class RemoteImmutableDocument extends ImmutableDocument {
  protected final RemoteDatabase      remoteDatabase;
  protected final String              typeName;
  protected       Map<String, Object> map;

  protected RemoteImmutableDocument(final RemoteDatabase remoteDatabase, final Map<String, Object> attributes) {
    super(null, null, null, null);
    this.remoteDatabase = remoteDatabase;
    this.map = new HashMap<>(attributes);

    final String ridAsString = (String) attributes.get("@rid");
    if (ridAsString != null)
      this.rid = new RID(null, ridAsString);
    else
      this.rid = null;

    this.typeName = (String) attributes.get("@type");
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  @Override
  public synchronized boolean has(String propertyName) {
    return map.containsKey(propertyName);
  }

  public synchronized Object get(final String propertyName) {
    return map.get(propertyName);
  }
}
