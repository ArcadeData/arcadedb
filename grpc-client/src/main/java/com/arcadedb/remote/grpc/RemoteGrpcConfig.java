package com.arcadedb.remote.grpc;

import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;

public record RemoteGrpcConfig(boolean includeProjections, ProjectionEncoding projectionEncoding, int softLimitBytes) {

}
