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
package com.arcadedb.bench.workload.protocol;

import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.workload.RemoteClients;

/**
 * openCypher over HTTP: the {@link com.arcadedb.remote.RemoteDatabase} driver, language {@value ProtocolClient#LANGUAGE}.
 */
public final class HttpProtocolClient extends RemoteDatabaseClient {
  public HttpProtocolClient(final ServerUnderTest server, final String database) {
    super(RemoteClients.open(server, database));
  }

  @Override
  public void close() {
    db.close();
  }
}
