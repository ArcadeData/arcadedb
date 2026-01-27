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
package com.arcadedb.bolt.message;

import com.arcadedb.bolt.packstream.PackStreamWriter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * ROUTE message for cluster routing information.
 * Signature: 0x66
 * Fields: routing (Map), bookmarks (List), database (String)
 */
public class RouteMessage extends BoltMessage {
  private final Map<String, Object> routing;
  private final List<String>        bookmarks;
  private final String              database;

  public RouteMessage(final Map<String, Object> routing, final List<String> bookmarks, final String database) {
    super(ROUTE);
    this.routing = routing != null ? routing : Map.of();
    this.bookmarks = bookmarks != null ? bookmarks : List.of();
    this.database = database;
  }

  public Map<String, Object> getRouting() {
    return routing;
  }

  public List<String> getBookmarks() {
    return bookmarks;
  }

  public String getDatabase() {
    return database;
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(signature, 3);
    writer.writeMap(routing);
    writer.writeList(bookmarks);
    if (database != null) {
      writer.writeString(database);
    } else {
      writer.writeNull();
    }
  }

  @Override
  public String toString() {
    return "ROUTE{routing=" + routing + ", bookmarks=" + bookmarks + ", database=" + database + "}";
  }
}
