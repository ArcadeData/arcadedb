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
import com.arcadedb.remote.grpc.RemoteGrpcDatabase;
import com.arcadedb.remote.grpc.RemoteGrpcServer;
import com.arcadedb.test.support.ServerWrapper;

import java.util.List;

/**
 * openCypher over gRPC: the {@link RemoteGrpcDatabase} driver on a plaintext channel, language
 * {@value ProtocolClient#LANGUAGE}. The GrpcServerPlugin must be enabled.
 */
public final class GrpcProtocolClient extends RemoteDatabaseClient {
  private final RemoteGrpcServer grpcServer;

  public GrpcProtocolClient(final ServerUnderTest server, final String database) {
    this(server.wrapper(), database);
  }

  private GrpcProtocolClient(final ServerWrapper w, final String database) {
    this(new RemoteGrpcServer(w.host(), w.grpcPort(), USER, ServerUnderTest.PASSWORD, true, List.of()), w, database);
  }

  private GrpcProtocolClient(final RemoteGrpcServer grpcServer, final ServerWrapper w, final String database) {
    super(openDatabase(grpcServer, w, database));
    this.grpcServer = grpcServer;
  }

  /** The database over {@code grpcServer}; when it cannot be opened the channel is closed before the failure is thrown. */
  private static RemoteGrpcDatabase openDatabase(final RemoteGrpcServer grpcServer, final ServerWrapper w, final String database) {
    try {
      return new RemoteGrpcDatabase(grpcServer, w.host(), w.grpcPort(), w.httpPort(), database, USER, ServerUnderTest.PASSWORD);
    } catch (final RuntimeException e) {
      try {
        grpcServer.close();
      } catch (final RuntimeException close) {
        e.addSuppressed(close);
      }
      throw e;
    }
  }

  @Override
  public void close() {
    try {
      db.close();
    } finally {
      grpcServer.close();
    }
  }
}
