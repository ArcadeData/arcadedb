# Issue #5796: Redis and MongoDB protocol plugins ignore their configured port and bind the default

## Root cause

Both `RedisProtocolPlugin` and `MongoDBProtocolPlugin` implement `ServerPlugin.configure(ArcadeDBServer, ContextConfiguration)`,
which is the server's per-instance configuration passed by `PluginManager.startPlugins()`. Postgres and Bolt read their
host/port from this `configuration` argument. Redis and MongoDB did not:

- `RedisProtocolPlugin` stored the `ContextConfiguration` in a field (used it correctly for `REDIS_TLS`), but its
  `startService()` read the host/port via the static accessors `GlobalConfiguration.REDIS_HOST.getValueAsString()` /
  `GlobalConfiguration.REDIS_PORT.getValueAsString()`, i.e. the hardcoded defaults, never the value set on the server's
  `ContextConfiguration`.
- `MongoDBProtocolPlugin.configure()` dropped the `configuration` argument entirely and `startService()` bound
  `GlobalConfiguration.MONGO_HOST` / `GlobalConfiguration.MONGO_PORT` directly, same defect.

`BoltProtocolPlugin` had exactly this bug and was fixed in #3809 by reading `BOLT_HOST`/`BOLT_PORT` off the `configuration`
argument in `configure()` and using the stored fields in `startService()`. This fix mirrors that pattern for Redis and Mongo.

Because `ContextConfiguration` falls back to the `GlobalConfiguration` static default whenever a key is not explicitly set on
the instance (see `ContextConfiguration` class Javadoc), reading through `configuration.getValueAsString/Integer(...)` is
strictly more correct and fully backward compatible with every existing test/deployment that never overrode the port.

## Fix

- `redisw/src/main/java/com/arcadedb/redis/RedisProtocolPlugin.java`: `startService()` now reads host/port via
  `configuration.getValueAsString(GlobalConfiguration.REDIS_HOST)` / `configuration.getValueAsString(GlobalConfiguration.REDIS_PORT)`
  instead of the static `GlobalConfiguration.REDIS_HOST`/`REDIS_PORT` accessors.
- `mongodbw/src/main/java/com/arcadedb/mongo/MongoDBProtocolPlugin.java`: `configure()` now captures `host`/`port` fields from
  the `configuration` argument (mirroring `PostgresProtocolPlugin`/`BoltProtocolPlugin`), and `startService()` binds those
  fields instead of the static `GlobalConfiguration.MONGO_HOST`/`MONGO_PORT` accessors.

## Tests (TDD)

New regression tests, one per affected module, extending `BaseGraphServerTest` and overriding `onServerConfiguration()` to set
a custom port (obtained from a `ServerSocket(0)` probe to avoid collisions) on the per-server `ContextConfiguration` - exactly
the object `PluginManager` hands to `ServerPlugin.configure()`:

- `redisw/src/test/java/com/arcadedb/redis/RedisPortConfigurationTest.java`
  - Connects a Jedis client to the configured custom port and checks `PING` succeeds.
  - Asserts the hardcoded default port (6379) is NOT accepting connections.
- `mongodbw/src/test/java/com/arcadedb/mongo/MongoDBPortConfigurationTest.java`
  - Connects a Mongo client to the configured custom port and checks a `ping` command succeeds.
  - Asserts the hardcoded default port (27017) is NOT accepting connections.

Both tests were verified to **fail** against the pre-fix code (proving they exercise the bug) before the fix was applied, then
verified to pass after the fix:

- Pre-fix `RedisPortConfigurationTest`: `JedisConnectionException: Failed to connect to localhost:<custom>` - the plugin was
  still bound to the default port.
- Pre-fix `MongoDBPortConfigurationTest`: `MongoTimeoutException: Timed out after 5000 ms while waiting to connect` to the
  custom port - same cause.

## Test results

Ran with the fix applied, module-scoped (dependencies installed first via `mvn -pl redisw,mongodbw -am install -DskipTests`,
then `mvn -pl redisw,mongodbw test`):

- `redisw`: 5 test classes, 46 tests, 0 failures, 0 errors (includes the new `RedisPortConfigurationTest`).
- `mongodbw`: 13 test classes, 84 tests, 0 failures, 0 errors (includes the new `MongoDBPortConfigurationTest`).

No existing tests were modified or deleted.

## Scope note

The issue also mentions, as a separate "worth considering" suggestion, that a plugin unable to honor a setting should log
that fact. That is a follow-up UX/observability improvement, not part of this fix, and was left out to keep the change
minimal and focused on the reported defect (silently binding the wrong port).

`PostgresProtocolPlugin` and `BoltProtocolPlugin` were inspected and already read host/port correctly from the `configuration`
argument - no changes needed there.

## Impact analysis

- No behavior change for any deployment that does not set `REDIS_PORT`/`REDIS_HOST`/`MONGO_PORT`/`MONGO_HOST` explicitly:
  `ContextConfiguration` falls back to the same static defaults as before.
- Deployments that DO configure a custom Redis or MongoDB plugin port will now get the port they asked for, instead of a
  silent bind to the default - fixing potential port collisions with a real Redis/MongoDB instance on the same host.
