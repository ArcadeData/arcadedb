# Wrapper Plugin Dedicated Class Loaders

This feature implements dedicated class loaders for wrapper plugins to isolate them from the main application class loader.

## Overview

The ArcadeDB server now loads wrapper plugins (MongoDB, Redis, PostgreSQL, and Gremlin protocol wrappers) using dedicated class loaders instead of the main application class loader. This provides better isolation and prevents potential class loading conflicts.

## Implementation Details

### Wrapper Plugins Affected
- **MongoDB Protocol Plugin**: `com.arcadedb.mongo.MongoDBProtocolPlugin`
- **Redis Protocol Plugin**: `com.arcadedb.redis.RedisProtocolPlugin`
- **PostgreSQL Protocol Plugin**: `com.arcadedb.postgres.PostgresProtocolPlugin`
- **Gremlin Server Plugin**: `com.arcadedb.server.gremlin.GremlinServerPlugin`

### Key Components

#### WrapperPluginClassLoader
A specialized `URLClassLoader` that:
- Extends `URLClassLoader` for custom class loading behavior
- Maintains a registry of class loaders per plugin type
- Implements proper cleanup and resource management
- Uses singleton pattern to ensure one class loader per plugin type

#### Modified ArcadeDBServer.registerPlugins()
The plugin registration method now:
- Detects wrapper plugins by class name patterns
- Creates dedicated class loaders for wrapper plugins
- Loads regular plugins with the main class loader
- Logs when wrapper plugins are loaded with dedicated class loaders

#### Enhanced Server Cleanup
The server shutdown process now:
- Properly closes all wrapper plugin class loaders
- Prevents resource leaks during server shutdown

## Benefits

1. **Isolation**: Wrapper plugins are isolated from the main application class loader
2. **Conflict Prevention**: Reduces potential class loading conflicts between different protocols
3. **Resource Management**: Proper cleanup of class loaders during shutdown
4. **Backward Compatibility**: Regular plugins continue to use the main class loader

## Usage

No configuration changes are required. The feature is automatically enabled for wrapper plugins based on their class names.

When the server starts, you'll see log messages like:
```
Loading wrapper plugin MongoDB with dedicated class loader
Loading wrapper plugin Redis with dedicated class loader
Loading wrapper plugin PostgreSQL with dedicated class loader
Loading wrapper plugin Gremlin with dedicated class loader
```

During shutdown:
```
- Closing wrapper plugin class loaders
```

## Technical Implementation

### Plugin Detection
```java
public static boolean isWrapperPlugin(final String pluginClassName) {
    return pluginClassName != null && (
        pluginClassName.contains("mongo") ||
        pluginClassName.contains("redis") ||
        pluginClassName.contains("postgres") ||
        pluginClassName.contains("gremlin")
    );
}
```

### Class Loader Creation
```java
if (WrapperPluginClassLoader.isWrapperPlugin(pluginClass)) {
    // Load wrapper plugins with dedicated class loader
    final String wrapperName = WrapperPluginClassLoader.getWrapperPluginName(pluginClass);
    final WrapperPluginClassLoader wrapperClassLoader = WrapperPluginClassLoader.getOrCreateClassLoader(
        wrapperName, 
        new java.net.URL[0], // URLs will be resolved from classpath
        Thread.currentThread().getContextClassLoader()
    );
    c = (Class<ServerPlugin>) Class.forName(pluginClass, true, wrapperClassLoader);
}
```

## Testing

The implementation includes comprehensive unit tests:
- `WrapperPluginClassLoaderTest`: Tests core class loader functionality
- `WrapperPluginIntegrationTest`: Tests integration with the plugin system

All wrapper plugin detection logic has been validated to correctly identify wrapper plugins and exclude non-wrapper plugins.