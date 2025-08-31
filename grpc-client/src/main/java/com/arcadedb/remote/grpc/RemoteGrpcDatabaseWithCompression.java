package com.arcadedb.remote.grpc;

import java.util.concurrent.TimeUnit;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Extended RemoteGrpcDatabase with compression support
 */
public class RemoteGrpcDatabaseWithCompression extends RemoteGrpcDatabase {
    
    public RemoteGrpcDatabaseWithCompression(String server, int port, 
                                             String databaseName,
                                             String userName, 
                                             String userPassword,
                                             ContextConfiguration configuration) {
        super(server, port, databaseName, userName, userPassword, configuration);
    }
    
    @Override
    protected ManagedChannel createChannel(String server, int port) {
        
    	return ManagedChannelBuilder.forAddress(server, port)
            .usePlaintext()                    // No TLS/SSL
            .maxInboundMessageSize(100 * 1024 * 1024)  // 100MB max message size
            .keepAliveTime(30, TimeUnit.SECONDS)       // Keep-alive configuration
            .keepAliveTimeout(10, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .build();
    }
    
    @Override
    protected ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub createBlockingStub(ManagedChannel channel) {
        return ArcadeDbServiceGrpc.newBlockingStub(channel)
            .withCompression("gzip")           // Enable GZIP compression
            .withCallCredentials(createCredentials())
            .withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
    }
    
    @Override
    protected ArcadeDbServiceGrpc.ArcadeDbServiceStub createAsyncStub(ManagedChannel channel) {
        return ArcadeDbServiceGrpc.newStub(channel)
            .withCompression("gzip")           // Enable GZIP compression
            .withCallCredentials(createCredentials())
            .withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
    }
}
