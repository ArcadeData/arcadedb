package com.arcadedb.test.support;

import org.testcontainers.containers.GenericContainer;

public record ServerWrapper(String host,
                            int httpPort,
                            int grpcPort
) {
  public ServerWrapper(GenericContainer<?> container) {
    this(container.getHost(),
        container.getMappedPort(2480),
        container.getMappedPort(50051));
  }
}
