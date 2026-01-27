# ArcadeDB Plugin Architecture

## Overview

ArcadeDB supports a plugin architecture that allows extending the server functionality through isolated plugins. Each plugin runs in its own class loader, enabling plugins to have different versions of dependencies without conflicts.

## Plugin Types

ArcadeDB includes the following built-in plugins:

- **Gremlin** - Apache TinkerPop Gremlin graph traversal language support
- **PostgreSQL Wire Protocol** - PostgreSQL protocol compatibility
- **MongoDB Wire Protocol** - MongoDB query language compatibility
- **Redis Wire Protocol** - Redis command compatibility
- **gRPC** - gRPC protocol support

## Architecture

### Class Loading

The plugin system uses isolated class loaders with the following strategy:

1. **Server API Classes** (`com.arcadedb.*`) - Loaded from parent class loader (shared across all plugins)
2. **Plugin Classes** - Loaded from plugin's own JAR first (isolated)
3. **Other Classes** - Fall back to parent class loader if not found in plugin JAR

This approach ensures:
- Plugins can use different versions of third-party libraries
- Server APIs are shared for consistency and communication
- Memory efficiency through shared core classes

### Components

#### PluginManager
- Discovers plugins from `lib/plugins/` directory
- Manages plugin lifecycle (start, stop)
- Coordinates plugin loading with server initialization

#### PluginClassLoader
- Custom class loader that isolates plugin dependencies
- Parent-first delegation for server API classes
- Child-first delegation for plugin-specific classes

#### PluginDescriptor
- Metadata container for each plugin
- Tracks plugin state and lifecycle
- Associates plugin with its class loader

## Plugin Lifecycle

1. **Discovery** - Scan `lib/plugins/` directory for JAR files
2. **Class Loading** - Create isolated class loader for each plugin JAR
3. **Service Discovery** - Use Java ServiceLoader to find `ServerPlugin` implementations
4. **Configuration** - Call `configure()` with server instance and configuration
5. **Starting** - Call `startService()` based on installation priority
6. **Running** - Plugin provides functionality
7. **Stopping** - Call `stopService()` in reverse order
8. **Cleanup** - Close class loaders and release resources

### Installation Priorities

Plugins are started in phases based on their installation priority:

1. `BEFORE_HTTP_ON` - Before HTTP server starts (default)
2. `AFTER_HTTP_ON` - After HTTP server starts
3. `AFTER_DATABASES_OPEN` - After databases are loaded

## Creating a Plugin

### 1. Implement ServerPlugin Interface

```java
package com.example.myplugin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;

public class MyPlugin implements ServerPlugin {
  private ArcadeDBServer server;
  private ContextConfiguration configuration;

  @Override
  public void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
    // Initialize your plugin configuration
  }

  @Override
  public void startService() {
    // Start your plugin services
    System.out.println("MyPlugin started!");
  }

  @Override
  public void stopService() {
    // Stop your plugin services and clean up resources
    System.out.println("MyPlugin stopped!");
  }

  @Override
  public INSTALLATION_PRIORITY getInstallationPriority() {
    return INSTALLATION_PRIORITY.AFTER_HTTP_ON;
  }
}
```

### 2. Create Service Provider Configuration

Create file: `src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin`

Content:
```
com.example.myplugin.MyPlugin
```

### 3. Build Plugin JAR

```bash
mvn clean package
```

### 4. Deploy Plugin

Copy the plugin JAR to the `lib/plugins/` directory in your ArcadeDB installation:

```bash
cp target/myplugin-1.0.0.jar $ARCADEDB_HOME/lib/plugins/
```

### 5. Start ArcadeDB

The plugin will be automatically discovered and loaded when ArcadeDB starts:

```bash
cd $ARCADEDB_HOME
bin/server.sh
```

Check the logs for:
```
[INFO] Discovered plugin: myplugin from myplugin-1.0.0.jar
[INFO] - myplugin plugin started
```

## Plugin Dependencies

### Server API Dependencies

Plugin POMs should include server dependencies with `provided` scope:

```xml
<dependency>
    <groupId>com.arcadedb</groupId>
    <artifactId>arcadedb-server</artifactId>
    <version>${arcadedb.version}</version>
    <scope>provided</scope>
</dependency>
```

### Plugin-Specific Dependencies

Plugin-specific dependencies use normal `compile` scope and will be packaged with the plugin:

```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>my-library</artifactId>
    <version>1.0.0</version>
    <scope>compile</scope>
</dependency>
```

## Building Distributions with Plugins

### Maven Assembly

The Maven assembly descriptor automatically places plugin JARs in `lib/plugins/`:

```xml
<dependencySet>
    <outputDirectory>lib/plugins</outputDirectory>
    <includes>
        <include>com.arcadedb:arcadedb-gremlin</include>
        <include>com.arcadedb:arcadedb-postgresw</include>
        <include>com.arcadedb:arcadedb-mongodbw</include>
        <include>com.arcadedb:arcadedb-redisw</include>
        <include>com.arcadedb:arcadedb-grpcw</include>
    </includes>
    <useTransitiveDependencies>false</useTransitiveDependencies>
</dependencySet>
```

## Advanced Topics

### Accessing Server Resources

Plugins have full access to the ArcadeDB server instance:

```java
@Override
public void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration) {
    this.server = arcadeDBServer;

    // Access databases
    ServerDatabase db = server.getDatabase("mydb");

    // Access HTTP server for custom endpoints
    HttpServer httpServer = server.getHttpServer();

    // Access security
    ServerSecurity security = server.getSecurity();
}
```

### Thread Context Class Loader

The PluginManager automatically sets the thread context class loader during plugin operations:

- During `configure()` - Set to plugin's class loader
- During `startService()` - Set to plugin's class loader
- During `stopService()` - Set to plugin's class loader

This ensures proper class loading for frameworks that use the thread context class loader.

### HTTP Endpoint Registration

Plugins can register custom HTTP endpoints:

```java
@Override
public void registerAPI(HttpServer httpServer, PathHandler routes) {
    routes.addExactPath("/api/myplugin", exchange -> {
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send("{\"status\":\"ok\"}");
    });
}
```

## Troubleshooting

### Plugin Not Discovered

Check that:
1. Plugin JAR is in `lib/plugins/` directory
2. `META-INF/services/com.arcadedb.server.ServerPlugin` file exists
3. Service file contains correct plugin class name
4. Plugin class implements `ServerPlugin` interface

### ClassNotFoundException

If you see `ClassNotFoundException` for server classes:
- Ensure server dependencies use `provided` scope
- Check that class is in `com.arcadedb.*` package

If you see `ClassNotFoundException` for plugin classes:
- Ensure dependency is included with `compile` scope
- Check that JAR contains the required class

### Plugin Conflicts

If two plugins have conflicting dependencies:
- This is the main benefit of isolated class loaders
- Each plugin can use its own version
- Ensure server API classes match across all plugins

## Migration from Legacy Plugin Loading

The new plugin system is backward compatible with the legacy configuration-based loading. Both systems can coexist:

### Legacy Method (still supported)
```properties
arcadedb.server.plugins=gremlin:com.arcadedb.server.gremlin.GremlinServerPlugin
```

### New Method (recommended)
1. Place plugin JAR in `lib/plugins/`
2. Include `META-INF/services` file
3. No configuration needed

## Best Practices

1. **Use Provided Scope** - Server dependencies should always use `provided` scope
2. **Clean Shutdown** - Implement proper cleanup in `stopService()`
3. **Thread Safety** - Make plugin implementations thread-safe
4. **Logging** - Use `LogManager.instance().log()` for consistent logging
5. **Error Handling** - Handle exceptions gracefully, don't crash the server
6. **Resource Management** - Close all resources in `stopService()`
7. **Configuration** - Use `ContextConfiguration` for plugin settings

## Examples

See the built-in plugins for complete examples:
- `gremlin/` - Complex plugin with custom graph manager
- `postgresw/` - Network protocol plugin
- `mongodbw/` - Query language compatibility plugin
- `redisw/` - Simple protocol plugin
- `grpcw/` - gRPC service plugin
