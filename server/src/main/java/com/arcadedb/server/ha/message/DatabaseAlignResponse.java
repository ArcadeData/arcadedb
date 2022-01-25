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
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Response for a request. This is needed to check the quorum by the leader.
 */
public class DatabaseAlignResponse extends HAAbstractCommand {
  private List<int[]> alignedPages;
  private String      remoteServerName;

  public DatabaseAlignResponse() {
  }

  public DatabaseAlignResponse(List<int[]> alignedPages) {
    this.alignedPages = alignedPages;
  }

  public List<int[]> getAlignedPages() {
    return alignedPages;
  }

  public String getRemoteServerName() {
    return remoteServerName;
  }

  @Override
  public void toStream(final Binary stream) {
    if (alignedPages == null)
      stream.putInt(0);
    else {
      stream.putInt(alignedPages.size());
      for (int i = 0; i < alignedPages.size(); i++) {
        final int[] page = alignedPages.get(i);
        stream.putInt(page[0]);
        stream.putInt(page[1]);
        stream.putInt(page[2]);
      }
    }
  }

  @Override
  public void fromStream(ArcadeDBServer server, final Binary stream) {
    final int total = stream.getInt();
    if (total > 0) {
      alignedPages = new ArrayList<>(total);
      for (int i = 0; i < total; i++) {
        final int[] page = new int[3];
        page[0] = stream.getInt();
        page[1] = stream.getInt();
        page[2] = stream.getInt();

        alignedPages.add(page);
      }
    } else
      alignedPages = Collections.emptyList();
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    this.remoteServerName = remoteServerName;
    server.receivedResponse(remoteServerName, messageNumber, this);
    return null;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();

    for (int[] array : alignedPages) {
      if (buffer.length() > 0)
        buffer.append(',');
      buffer.append(Arrays.toString(array));
    }

    return "db-align-response(" + remoteServerName + ": [" + buffer + "])";
  }
}
