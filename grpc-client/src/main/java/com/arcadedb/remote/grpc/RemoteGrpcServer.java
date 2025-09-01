package com.arcadedb.remote.grpc;

import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.CreateDatabaseRequest;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.DropDatabaseRequest;
import com.arcadedb.server.grpc.ListDatabasesRequest;
import com.arcadedb.server.grpc.ListDatabasesResponse;

import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.CallCredentials.MetadataApplier;
import io.grpc.CallCredentials.RequestInfo;
import io.grpc.stub.AbstractStub;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Minimal server-scope gRPC wrapper (HTTP RemoteServer equivalent), implemented
 * ONLY with RPCs present in your current proto.
 *
 * Features: - listDatabases() - existsDatabase(name) via listDatabases() -
 * createDatabase(name, type) - createDatabaseIfMissing(name, type) -
 * dropDatabase(name, force?) // 'force' only if defined in your proto;
 * otherwise ignored
 *
 * Add more methods later when you extend the proto (ping, serverInfo, user
 * mgmt, etc.).
 */
public class RemoteGrpcServer implements AutoCloseable {

	private final String host;
	private final int port;
	private final String userName;
	private final String userPassword;

	private final long defaultTimeoutMs;

	private final ManagedChannel channel;
	private final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub blocking;

	public RemoteGrpcServer(final String host, final int port, final String user, final String pass) {
		this(host, port, user, pass, 30_000);
	}

	public RemoteGrpcServer(final String host, final int port, final String user, final String pass, final long defaultTimeoutMs) {
		
		this.host = Objects.requireNonNull(host, "host");
		
		this.port = port;
		
		this.userName = Objects.requireNonNull(user, "user");
		
		this.userPassword = Objects.requireNonNull(pass, "pass");
		
		this.defaultTimeoutMs = defaultTimeoutMs > 0 ? defaultTimeoutMs : 30_000;

		this.channel = ManagedChannelBuilder.forAddress(this.host, this.port).usePlaintext() // switch to TLS if enabled server-side
				.build();

		this.blocking = ArcadeDbServiceGrpc.newBlockingStub(channel).withCallCredentials(createCallCredentials(userName, userPassword));
	}

	@Override
	public void close() {
		channel.shutdown();
		try {
			channel.awaitTermination(5, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private <S extends AbstractStub<S>> S withDeadline(S stub, long timeoutMs) {
		long t = (timeoutMs > 0) ? timeoutMs : defaultTimeoutMs;
		return stub.withDeadlineAfter(t, TimeUnit.MILLISECONDS);
	}

	public DatabaseCredentials buildCredentials() {		
		return DatabaseCredentials.newBuilder().setUsername(userName == null ? "" : userName).setPassword(userPassword == null ? "" : userPassword).build();
	}

	/** Returns the list of database names from the server. */
	public List<String> listDatabases() {
		
		ListDatabasesResponse resp = withDeadline(blocking, defaultTimeoutMs)
				.listDatabases(ListDatabasesRequest.newBuilder().setCredentials(buildCredentials()).build());
		
		return resp.getDatabasesList().stream().map(dbInfo -> dbInfo.getName()).toList();
	}

	/** Checks existence by listing (no ExistsDatabaseRequest needed). */
	public boolean existsDatabase(final String database) {
		return listDatabases().stream().anyMatch(n -> n.equalsIgnoreCase(database));
	}

	/** Creates a database with type "graph" or "document". */
	public void createDatabase(final String database) {
		withDeadline(blocking, defaultTimeoutMs).createDatabase(CreateDatabaseRequest.newBuilder()
				.setDatabaseName(database)
				.setCredentials(buildCredentials()).build());
	}

	/** No-op if already present; creates otherwise. */
	public void createDatabaseIfMissing(final String database) {
		if (!existsDatabase(database)) {
			createDatabase(database);
		}
	}

	/** Drops a database. If your proto supports 'force', add it here. */
	public void dropDatabase(final String database) {
		withDeadline(blocking, defaultTimeoutMs)
				.dropDatabase(DropDatabaseRequest.newBuilder().setDatabaseName(database).setCredentials(buildCredentials()).build());
	}

	public String endpoint() {
		return host + ":" + port;
	}

	@Override
	public String toString() {
		return "RemoteGrpcServer{endpoint=" + endpoint() + ", userName='" + userName + "'}";
	}
	
	/**
	 * Creates call credentials for authentication
	 */
	protected CallCredentials createCallCredentials(String userName, String userPassword) {
		return new CallCredentials() {
			@Override
			public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
				Metadata headers = new Metadata();
				headers.put(Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER), userName);
				headers.put(Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
				headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), userName);
				headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
				applier.apply(headers);
			}

			@Override
			public void thisUsesUnstableApi() {
				// Required by the interface
			}
		};
	}

	protected CallCredentials createCredentials() {
		return new CallCredentials() {
			@Override
			public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
				Metadata headers = new Metadata();
				headers.put(Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER), userName);
				headers.put(Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
				headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), userName);
				headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
				applier.apply(headers);
			}

			// x-arcade-user: root" -H "x-arcade-password: oY9uU2uJ8nD8iY7t" -H "x-arcade-database: local_shakeiq_curonix_poc-app"
			@Override
			public void thisUsesUnstableApi() {
				// Required by the interface
			}
		};
	}	
}