/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

public class FileContentResponse extends HAAbstractCommand {
  private Binary  pagesContent;
  private int     pages;
  private boolean last;

  public FileContentResponse() {
  }

  public FileContentResponse(final Binary pagesContent, final int pages, final boolean last) {
    this.pagesContent = pagesContent;
    this.pages = pages;
    this.last = last;
  }

  public Binary getPagesContent() {
    return pagesContent;
  }

  public int getPages() {
    return pages;
  }

  public boolean isLast() {
    return last;
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putUnsignedNumber(pages);
    stream.putBytes(pagesContent.getContent(), pagesContent.size());
    stream.putByte((byte) (last ? 1 : 0));
  }

  @Override
  public void fromStream(final Binary stream) {
    pages = (int) stream.getUnsignedNumber();
    pagesContent = new Binary(stream.getBytes());
    last = stream.getByte() == 1;
  }

  @Override
  public String toString() {
    return "file=" + pages + " pages (" + pagesContent.size() + " bytes)";
  }
}
