# ArcadeDB gRPC Server Configuration Examples

# ==========================================
# Basic Configuration (arcadedb-server.properties)
# ==========================================

# Enable gRPC server (default: true)
grpc.enabled=true

# Server mode: standard, xds, or both (default: standard)
grpc.mode=standard

# Standard gRPC server port (default: 50051)
grpc.port=50051

# Bind host (default: 0.0.0.0)
grpc.host=0.0.0.0

# Max message size in MB (default: 100)
grpc.maxMessageSize=100

# Enable gRPC reflection for debugging with grpcurl (default: true)
grpc.reflection.enabled=true

# Enable health checking endpoint (default: true)
grpc.health.enabled=true

# ==========================================
# XDS Configuration (for Service Mesh)
# ==========================================

# Enable XDS mode for Envoy/Istio integration
grpc.mode=xds
# or run both standard and XDS
# grpc.mode=both

# XDS server port (default: 50052)
grpc.xds.port=50052

# ==========================================
# TLS Configuration
# ==========================================

# Enable TLS (default: false)
grpc.tls.enabled=true

# Path to TLS certificate
grpc.tls.cert=/path/to/server.crt

# Path to TLS private key
grpc.tls.key=/path/to/server.key


arcadedb.grpc.compression.enabled=true     # Enable compression support
arcadedb.grpc.compression.force=false      # Force compression on all responses
arcadedb.grpc.compression.type=gzip        # Compression algorithm



# ==========================================
# Docker Compose Example
# ==========================================
# docker-compose.yml:
#
# version: '3.8'
# services:
#   arcadedb:
#     image: arcadedb/arcadedb:latest
#     ports:
#       - "2480:2480"  # HTTP
#       - "2424:2424"  # Binary
#       - "50051:50051" # gRPC
#     environment:
#       - ARCADEDB_OPTS_MEMORY=-Xms2G -Xmx2G
#       - arcadedb.grpc.enabled=true
#       - arcadedb.grpc.port=50051
#     volumes:
#       - ./databases:/var/arcadedb/databases

# ==========================================
# Kubernetes ConfigMap Example
# ==========================================
# apiVersion: v1
# kind: ConfigMap
# metadata:
#   name: arcadedb-config
# data:
#   arcadedb.properties: |
#     grpc.enabled=true
#     grpc.mode=xds
#     grpc.xds.port=50052
#     grpc.health.enabled=true
#     grpc.reflection.enabled=false

# ==========================================
# Istio Service Mesh Example
# ==========================================
# apiVersion: v1
# kind: Service
# metadata:
#   name: arcadedb-grpc
# spec:
#   ports:
#   - name: grpc-xds
#     port: 50052
#     protocol: TCP
#   - name: grpc
#     port: 50051
#     protocol: TCP
#   selector:
#     app: arcadedb







<!-- Add this to your pom.xml in the build/plugins section -->

<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-assembly-plugin</artifactId>
    <version>3.6.0</version>
    <configuration>
        <descriptorRefs>
            <descriptorRef>jar-with-dependencies</descriptorRef>
        </descriptorRefs>
        <archive>
            <manifest>
                <mainClass>com.arcadedb.server.grpc.GrpcStandaloneLauncher</mainClass>
            </manifest>
        </archive>
    </configuration>
    <executions>
        <execution>
            <id>make-grpc-standalone</id>
            <phase>package</phase>
            <goals>
                <goal>single</goal>
            </goals>
            <configuration>
                <finalName>arcadedb-grpc-server-${project.version}</finalName>
                <appendAssemblyId>false</appendAssemblyId>
            </configuration>
        </execution>
    </executions>
</plugin>

<!-- Alternative: Maven Shade Plugin for a more optimized JAR -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.5.1</version>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
            <configuration>
                <createDependencyReducedPom>false</createDependencyReducedPom>
                <transformers>
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                        <mainClass>com.arcadedb.server.grpc.GrpcStandaloneLauncher</mainClass>
                    </transformer>
                    <!-- Merge service files for gRPC -->
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                    <!-- Handle protobuf descriptors -->
                    <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                        <resource>META-INF/services/io.grpc.ServerProvider</resource>
                    </transformer>
                </transformers>
                <filters>
                    <filter>
                        <artifact>*:*</artifact>
                        <excludes>
                            <exclude>META-INF/*.SF</exclude>
                            <exclude>META-INF/*.DSA</exclude>
                            <exclude>META-INF/*.RSA</exclude>
                        </excludes>
                    </filter>
                </filters>
                <shadedArtifactAttached>true</shadedArtifactAttached>
                <shadedClassifierName>grpc-standalone</shadedClassifierName>
            </configuration>
        </execution>
    </executions>
</plugin>



properties# Basic
grpc.enabled=true
grpc.port=50051
grpc.host=0.0.0.0

# Advanced
grpc.mode=standard|xds|both
grpc.maxMessageSize=100
grpc.reflection.enabled=true
grpc.health.enabled=true

# Security
grpc.tls.enabled=true
grpc.tls.cert=/path/to/cert
grpc.tls.key=/path/to/key




version: '3.8'
services:
  arcadedb:
    image: arcadedb/arcadedb:latest
    ports:
      - "50051:50051"  # gRPC
    environment:
      - arcadedb.grpc.enabled=true

apiVersion: v1
kind: Service
metadata:
  name: arcadedb-grpc
spec:
  ports:
  - name: grpc-xds
    port: 50052
    protocol: TCP




# Development
mvn exec:java -Dexec.mainClass="com.arcadedb.server.grpc.GrpcStandaloneLauncher"

# Production
java -jar arcadedb-grpc-server.jar --config /etc/arcadedb/grpc.properties




# Using grpcurl
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 describe com.arcadedb.grpc.ArcadeDbService

# Health check
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check



# Basic Authentication
grpcurl -H "x-arcade-user: admin" \
        -H "x-arcade-password: admin" \
        -H "x-arcade-database: mydb" \
        -plaintext localhost:50051 \
        com.arcadedb.grpc.ArcadeDbService/Ping

# Bearer Token (for future implementation)
grpcurl -H "authorization: Bearer <token>" \
        -plaintext localhost:50051 \
        com.arcadedb.grpc.ArcadeDbService/Ping


server.registerPlugin(new GrpcServerPlugin());

grpc.enabled=true
grpc.port=50051
grpc.mode=standard
grpc.reflection.enabled=true
grpc.health.enabled=true



# Check health
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check

# List services
grpcurl -plaintext localhost:50051 list

# Test with authentication
grpcurl -H "x-arcade-user: admin" \
        -H "x-arcade-password: admin" \
        -H "x-arcade-database: mydb" \
        -plaintext localhost:50051 \
        com.arcadedb.grpc.ArcadeDbService/Ping




arcadedb.grpc.enabled=true
arcadedb.grpc.port=50051
arcadedb.grpc.host=0.0.0.0
arcadedb.grpc.mode=standard
arcadedb.grpc.reflection.enabled=true
arcadedb.grpc.health.enabled=true
arcadedb.grpc.maxMessageSize=100



java -Darcadedb.grpc.enabled=true \
     -Darcadedb.grpc.port=50051 \
     -Darcadedb.grpc.mode=standard \
     -jar arcadedb-server.jar

# Check if gRPC is running
grpcurl -plaintext localhost:50051 list

# Check health
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check





# Complete build with Docker images
mvn clean install -DskipTests && \
cd package && \
mvn package -Pdocker

# Multi-platform build (slower but supports ARM and x64)
mvn package -Pdocker,multiplatform

# Build and push to registry
mvn deploy -Pdocker -Ddocker.username=youruser -Ddocker.password=yourpass

# Skip tests and build quickly
mvn package -Pdocker -DskipTests -Dmaven.test.skip=true




# Run the Docker image with gRPC enabled
docker run -d \
  --name arcadedb-test \
  -p 2480:2480 \
  -p 50051:50051 \
  -e arcadedb.grpc.enabled=true \
  arcadedata/arcadedb:latest


# Run the Docker image with gRPC enabled
docker run -d \
  --name arcadedb-test \
  -p 2480:2480 \
  -p 50051:50051 \
  -e JAVA_OPTS="-Darcadedb.server.rootPassword=oY9uU2uJ8nD8iY7t -Darcadedb.server.name=ArcadeDB1 -Darcadedb.dumpConfigAtStartup=true -Darcadedb.server.mode=development -Darcadedb.server.plugins=GRPC:com.arcadedb.server.grpc.GrpcServerPlugin -Darcadedb.grpc.enabled=true -Darcadedb.grpc.mode=standard -Darcadedb.grpc.port=50051" \
  -e ARCADEDB_OPTS_MEMORY="-Xms4g -Xmx8g -XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=75.0" \
  arcadedata/arcadedb:25.8.1-SNAPSHOT



# Test gRPC connection
grpcurl -plaintext localhost:50051 list

# Check health
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check

# Stop and remove container
docker stop arcadedb-test
docker rm arcadedb-test







version: '3.8'

services:
  arcadedb:
    image: arcadedata/arcadedb:25.8.1-SNAPSHOT
    container_name: arcadedb-grpc
    ports:
      - "2480:2480"   # HTTP API
      - "2424:2424"   # Binary protocol
      - "50051:50051" # gRPC
      - "50052:50052" # gRPC XDS (if enabled)
    environment:
      - ARCADEDB_OPTS_MEMORY=-Xms2G -Xmx2G
      - arcadedb.grpc.enabled=true
      - arcadedb.grpc.port=50051
      - arcadedb.grpc.mode=standard
      - arcadedb.grpc.reflection.enabled=true
      - arcadedb.grpc.health.enabled=true
    volumes:
      - ./databases:/var/arcadedb/databases
      - ./config:/var/arcadedb/config
    networks:
      - arcadedb-network

  # Optional: gRPC UI for testing
  grpcui:
    image: fullstorydev/grpcui:latest
    container_name: grpcui
    ports:
      - "8080:8080"
    command:
      - -plaintext
      - -port
      - "8080"
      - arcadedb:50051
    depends_on:
      - arcadedb
    networks:
      - arcadedb-network

networks:
  arcadedb-network:
    driver: bridge




Docker:



grpcui -plaintext -bind 0.0.0.0 -port 8080 localhost:50051

mvn clean package -Pdocker
