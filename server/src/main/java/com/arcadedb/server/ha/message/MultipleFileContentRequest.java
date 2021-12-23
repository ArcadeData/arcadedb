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
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;

import java.util.*;

public class MultipleFileContentRequest extends HAAbstractCommand {
  private List<FileContentRequest> requests;

  public MultipleFileContentRequest() {
  }

  public MultipleFileContentRequest(List<FileContentRequest> requests) {
    this.requests = requests;
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {

    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putInt(requests.size());

    for (int i = 0; i < requests.size(); i++) {
      final FileContentRequest req = requests.get(i);
      req.toStream(stream);
    }
  }

  @Override
  public void fromStream(ArcadeDBServer server, final Binary stream) {
    final int total = stream.getInt();

    requests = new ArrayList<>(total);
    for (int i = 0; i < total; i++) {
      final FileContentRequest req = new FileContentRequest();
      req.fromStream(server, stream);
      requests.add(req);
    }
  }

  @Override
  public String toString() {
    return "files" + requests;
  }
}
