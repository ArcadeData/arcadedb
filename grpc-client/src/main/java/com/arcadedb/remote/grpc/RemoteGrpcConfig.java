package com.arcadedb.remote.grpc;

import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;

public class RemoteGrpcConfig {

  private boolean            includeProjections;
  private ProjectionEncoding projectionEncoding;
  private int                softLimitBytes;

  public RemoteGrpcConfig(boolean includeProjections, ProjectionEncoding projectionEncoding, int softLimitBytes) {
    this.includeProjections = includeProjections;
    this.projectionEncoding = projectionEncoding;
    this.softLimitBytes = softLimitBytes;
  }

  public boolean isIncludeProjections() {
    return includeProjections;
  }

  public void setIncludeProjections(boolean includeProjections) {
    this.includeProjections = includeProjections;
  }

  public ProjectionEncoding getProjectionEncoding() {
    return projectionEncoding;
  }

  public void setProjectionEncoding(ProjectionEncoding projectionEncoding) {
    this.projectionEncoding = projectionEncoding;
  }

  public int getSoftLimitBytes() {
    return softLimitBytes;
  }

  public void setSoftLimitBytes(int softLimitBytes) {
    this.softLimitBytes = softLimitBytes;
  }

}
