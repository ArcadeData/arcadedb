package com.arcadedb.remote.grpc;

import com.arcadedb.ContextConfiguration;

/**
 * Extended RemoteGrpcDatabase with compression support
 */
public class RemoteGrpcDatabaseWithCompression extends RemoteGrpcDatabase {

  public RemoteGrpcDatabaseWithCompression(RemoteGrpcServer remoteGrpcServer, String host, int grpcPort, int httpPort,
      String databaseName, String userName,
      String userPassword, ContextConfiguration configuration) {
    super(remoteGrpcServer, host, grpcPort, httpPort, databaseName, userName, userPassword, configuration);
  }
}
