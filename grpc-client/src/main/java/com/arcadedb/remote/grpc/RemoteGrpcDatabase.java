package com.arcadedb.remote.grpc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteImmutableDocument;
import com.arcadedb.remote.RemoteImmutableEdge;
import com.arcadedb.remote.RemoteImmutableVertex;
import com.arcadedb.remote.RemoteSchema;
import com.arcadedb.remote.RemoteTransactionExplicitLock;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.BatchAck;
import com.arcadedb.server.grpc.BeginTransactionRequest;
import com.arcadedb.server.grpc.BeginTransactionResponse;
import com.arcadedb.server.grpc.Commit;
import com.arcadedb.server.grpc.CommitTransactionRequest;
import com.arcadedb.server.grpc.CommitTransactionResponse;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.DeleteRecordRequest;
import com.arcadedb.server.grpc.DeleteRecordResponse;
import com.arcadedb.server.grpc.ExecuteCommandRequest;
import com.arcadedb.server.grpc.ExecuteCommandResponse;
import com.arcadedb.server.grpc.ExecuteQueryRequest;
import com.arcadedb.server.grpc.ExecuteQueryResponse;
import com.arcadedb.server.grpc.InsertChunk;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertRequest;
import com.arcadedb.server.grpc.InsertResponse;
import com.arcadedb.server.grpc.InsertSummary;
import com.arcadedb.server.grpc.LookupByRidRequest;
import com.arcadedb.server.grpc.LookupByRidResponse;
import com.arcadedb.server.grpc.QueryResult;
import com.arcadedb.server.grpc.RollbackTransactionRequest;
import com.arcadedb.server.grpc.RollbackTransactionResponse;
import com.arcadedb.server.grpc.Start;
import com.arcadedb.server.grpc.StreamQueryRequest;
import com.arcadedb.server.grpc.TransactionContext;
import com.arcadedb.server.grpc.TransactionIsolation;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import io.grpc.CallCredentials;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.BlockingClientCall;

/**
 * Remote Database implementation using gRPC protocol instead of HTTP. Extends
 * RemoteDatabase to maintain compatibility while overriding network operations.
 * It's not thread safe. For multi-thread usage create one instance of
 * RemoteGrpcDatabase per thread.
 *
 * @author Oleg Cohen (oleg.cohen@gmail.com)
 */
public class RemoteGrpcDatabase extends RemoteDatabase {

	private final ManagedChannel channel;

	private final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub blockingStub;
	private final ArcadeDbServiceGrpc.ArcadeDbServiceStub asyncStub;

	private String transactionId;

	private final RemoteSchema schema;

	private final String userName;
	private final String userPassword;
	private String databaseName;
	private RemoteTransactionExplicitLock explicitLock;

	public RemoteGrpcDatabase(final String server, final int grpcPort, final int httpPort, final String databaseName, final String userName,
			final String userPassword) {
		this(server, grpcPort, httpPort, databaseName, userName, userPassword, new ContextConfiguration());
	}

	public RemoteGrpcDatabase(final String server, final int grpcPort, final int httpPort, final String databaseName, final String userName,
			final String userPassword, final ContextConfiguration configuration) {

		super(server, httpPort, databaseName, userName, userPassword, configuration);

		this.userName = userName;
		this.userPassword = userPassword;

		this.databaseName = databaseName;

		// Create gRPC channel
		this.channel = createChannel(server, grpcPort);

		// Create stubs with authentication
		CallCredentials credentials = createCallCredentials(userName, userPassword);

		this.blockingStub = createBlockingStub(channel);

		this.asyncStub = createAsyncStub(channel);

		// Create gRPC-specific schema
		this.schema = new RemoteSchema(this);
	}

	/**
	 * Creates the gRPC channel. Override this for custom channel configuration.
	 */
	protected ManagedChannel createChannel(String server, int port) {
		return ManagedChannelBuilder.forAddress(server, port).usePlaintext() // No TLS by default
				.build();
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
				headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), databaseName);
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
				headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), databaseName);
				applier.apply(headers);
			}

			// x-arcade-user: root" -H "x-arcade-password: oY9uU2uJ8nD8iY7t" -H
			// "x-arcade-database: local_shakeiq_curonix_poc-app"
			@Override
			public void thisUsesUnstableApi() {
				// Required by the interface
			}
		};
	}

	/**
	 * Override this method to customize blocking stub creation
	 */
	protected ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub createBlockingStub(ManagedChannel channel) {
		return ArcadeDbServiceGrpc.newBlockingV2Stub(channel).withCallCredentials(createCredentials());
	}

	protected ArcadeDbServiceGrpc.ArcadeDbServiceStub createAsyncStub(ManagedChannel channel) {
		return ArcadeDbServiceGrpc.newStub(channel).withCallCredentials(createCredentials()).withCompression("gzip");
	}

	@Override
	public String getDatabasePath() {
		return "grpc://" + channel.authority() + "/" + getName();
	}

	@Override
	public RemoteSchema getSchema() {
		return schema;
	}

	@Override
	public void close() {
		if (transactionId != null) {
			rollback();
		}
		super.close();

		// Shutdown gRPC channel
		channel.shutdown();
		try {
			if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
				channel.shutdownNow();
			}
		}
		catch (InterruptedException e) {
			channel.shutdownNow();
		}
	}

	@Override
	public void begin(final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel) {
		checkDatabaseIsOpen();
		if (transactionId != null)
			throw new TransactionException("Transaction already begun");

		BeginTransactionRequest request = BeginTransactionRequest.newBuilder().setDatabase(getName()).setCredentials(buildCredentials())
				.setIsolation(mapIsolationLevel(isolationLevel)).build();

		try {
			BeginTransactionResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).beginTransaction(request);
			transactionId = response.getTransactionId();
			// Store transaction ID in parent class session management
			setSessionId(transactionId);
		}
		catch (StatusRuntimeException e) {
			throw new TransactionException("Error on transaction begin", e);
		}
		catch (StatusException e) {
			throw new TransactionException("Error on transaction begin", e);
		}
	}

	@Override
	public void commit() {
		checkDatabaseIsOpen();
		stats.txCommits.incrementAndGet();

		if (transactionId == null)
			throw new TransactionException("Transaction not begun");

		CommitTransactionRequest request = CommitTransactionRequest.newBuilder()
				.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build())
				.setCredentials(buildCredentials()).build();

		try {
			CommitTransactionResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).commitTransaction(request);
			if (!response.getSuccess()) {
				throw new TransactionException("Failed to commit transaction: " + response.getMessage());
			}
		}
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
		}
		catch (StatusException e) {
			handleGrpcException(e);
		}
		finally {
			transactionId = null;
			setSessionId(null);
		}
	}

	@Override
	public void rollback() {
		checkDatabaseIsOpen();
		stats.txRollbacks.incrementAndGet();

		if (transactionId == null)
			throw new TransactionException("Transaction not begun");

		RollbackTransactionRequest request = RollbackTransactionRequest.newBuilder()
				.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build())
				.setCredentials(buildCredentials()).build();

		try {
			RollbackTransactionResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.rollbackTransaction(request);
			if (!response.getSuccess()) {
				throw new TransactionException("Failed to rollback transaction: " + response.getMessage());
			}
		}
		catch (StatusRuntimeException e) {
			throw new TransactionException("Error on transaction rollback", e);
		}
		catch (StatusException e) {
			throw new TransactionException("Error on transaction begin", e);
		}
		finally {
			transactionId = null;
			setSessionId(null);
		}
	}

	@Override
	public Record lookupByRID(final RID rid, final boolean loadContent) {

		checkDatabaseIsOpen();

		stats.readRecord.incrementAndGet();

		if (rid == null)
			throw new IllegalArgumentException("Record is null");

		LookupByRidRequest request = LookupByRidRequest.newBuilder().setDatabase(getName()).setRid(rid.toString())
				.setCredentials(buildCredentials()).build();

		try {

			LookupByRidResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).lookupByRid(request);

			if (!response.getFound()) {
				throw new RecordNotFoundException("Record " + rid + " not found", rid);
			}
			return grpcRecordToRecord(response.getRecord());
		}
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
			return null;
		}
		catch (StatusException e) {
			handleGrpcException(e);
			return null;
		}
	}

	@Override
	public void deleteRecord(final Record record) {

		checkDatabaseIsOpen();
		stats.deleteRecord.incrementAndGet();

		if (record.getIdentity() == null)
			throw new IllegalArgumentException("Cannot delete a non persistent record");

		DeleteRecordRequest request = DeleteRecordRequest.newBuilder().setDatabase(getName()).setRid(record.getIdentity().toString())
				.setCredentials(buildCredentials()).build();

		try {
			DeleteRecordResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).deleteRecord(request);
			if (!response.getSuccess()) {
				throw new DatabaseOperationException("Failed to delete record: " + response.getMessage());
			}
		}
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
		}
		catch (StatusException e) {
			handleGrpcException(e);
		}
	}

	@Override
	public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
		String query = "select from `" + typeName + "`";
		if (!polymorphic)
			query += " where @type = '" + typeName + "'";
		return streamQuery(query);
	}

	@Override
	public Iterator<Record> iterateBucket(final String bucketName) {
		return streamQuery("select from bucket:`" + bucketName + "`");
	}

	@Override
	public ResultSet query(final String language, final String query, final Map<String, Object> params) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		ExecuteQueryRequest.Builder requestBuilder = ExecuteQueryRequest.newBuilder().setDatabase(getName()).setQuery(query)
				.setCredentials(buildCredentials());

		if (transactionId != null) {
			requestBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());
		}

		if (params != null && !params.isEmpty()) {
			requestBuilder.putAllParameters(convertParamsToProto(params));
		}

		try {
			ExecuteQueryResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.executeQuery(requestBuilder.build());
			return createGrpcResultSet(response);
		}
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
			return new InternalResultSet();
		}
		catch (StatusException e) {
			handleGrpcException(e);
			return new InternalResultSet();
		}
	}

	@Override
	public ResultSet query(final String language, final String query, final Object... args) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		final Map<String, Object> params = mapArgs(args);

		return query(language, query, params);
	}

	@Override
	public ResultSet command(final String language, final String command, final Map<String, Object> params) {
		checkDatabaseIsOpen();
		stats.commands.incrementAndGet();

		ExecuteCommandRequest.Builder requestBuilder = ExecuteCommandRequest.newBuilder().setDatabase(getName()).setCommand(command)
				.setCredentials(buildCredentials());

		if (transactionId != null) {
			requestBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());
		}

		if (params != null && !params.isEmpty()) {
			requestBuilder.putAllParameters(convertParamsToProto(params));
		}

		try {
			ExecuteCommandResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.executeCommand(requestBuilder.build());

			// Create result set with command execution info
			InternalResultSet resultSet = new InternalResultSet();
			if (response.getAffectedRecords() > 0) {
				Map<String, Object> result = new HashMap<>();
				result.put("affected", response.getAffectedRecords());
				result.put("executionTime", response.getExecutionTimeMs());
				resultSet.add(new ResultInternal(result));
			}
			return resultSet;
		}
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
			return new InternalResultSet();
		}
		catch (StatusException e) {
			handleGrpcException(e);
			return new InternalResultSet();
		}
	}

	public com.arcadedb.server.grpc.ExecuteCommandResponse execSql(String db, String sql, Map<String, Object> params, long timeoutMs) {
		return executeCommand(db, "sql", sql, params, /* returnRows */ false, /* maxRows */ 0, txBeginCommit(), timeoutMs);
	}

	public com.arcadedb.server.grpc.ExecuteCommandResponse execSql(String sql, Map<String, Object> params, long timeoutMs) {
		return executeCommand(databaseName, "sql", sql, params, /* returnRows */ false, /* maxRows */ 0, txBeginCommit(), timeoutMs);
	}

	public com.arcadedb.server.grpc.ExecuteCommandResponse executeCommand(String language, String command, Map<String, Object> params,
			boolean returnRows, int maxRows, com.arcadedb.server.grpc.TransactionContext tx, long timeoutMs) {

		var reqB = com.arcadedb.server.grpc.ExecuteCommandRequest.newBuilder().setDatabase(databaseName).setCommand(command)
				.putAllParameters(convertParamsToProto(params)).setLanguage(langOrDefault(language)).setReturnRows(returnRows)
				.setMaxRows(maxRows > 0 ? maxRows : 0);

		if (tx != null)
			reqB.setTransaction(tx);
		// credentials: if your stub builds creds implicitly, set here if required
		reqB.setCredentials(buildCredentials());

		try {

			return blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS).executeCommand(reqB.build());
		}
		catch (StatusException e) {

			throw new RuntimeException("Failed to execute command: " + e.getMessage(), e);
		}
	}

	public com.arcadedb.server.grpc.ExecuteCommandResponse executeCommand(String database, String language, String command,
			Map<String, Object> params, boolean returnRows, int maxRows, com.arcadedb.server.grpc.TransactionContext tx, long timeoutMs) {

		var reqB = com.arcadedb.server.grpc.ExecuteCommandRequest.newBuilder().setDatabase(database).setCommand(command)
				.putAllParameters(convertParamsToProto(params)).setLanguage(langOrDefault(language)).setReturnRows(returnRows)
				.setMaxRows(maxRows > 0 ? maxRows : 0);

		if (tx != null)
			reqB.setTransaction(tx);
		// credentials: if your stub builds creds implicitly, set here if required
		reqB.setCredentials(buildCredentials());

		try {
			return blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS).executeCommand(reqB.build());
		}
		catch (StatusException e) {

			throw new RuntimeException("Failed to execute command: " + e.getMessage(), e);
		}
	}

	@Override
	protected RID saveRecord(final MutableDocument record) {
		stats.createRecord.incrementAndGet();

		final RID rid = record.getIdentity();

		if (rid != null) {

			// -------- UPDATE (partial) --------
			// Build the nested proto Record for the "partial" oneof
			com.arcadedb.server.grpc.Record partial = com.arcadedb.server.grpc.Record.newBuilder()
					.putAllProperties(convertParamsToProto(record.toMap(false))).build();

			com.arcadedb.server.grpc.UpdateRecordRequest request = com.arcadedb.server.grpc.UpdateRecordRequest.newBuilder()
					.setDatabase(getName()).setRid(rid.toString()).setRecord(partial).setDatabase(databaseName)
					.setCredentials(buildCredentials()).build();

			try {
				com.arcadedb.server.grpc.UpdateRecordResponse response = blockingStub
						.withDeadlineAfter(getTimeout(), java.util.concurrent.TimeUnit.MILLISECONDS).updateRecord(request);

				// If your proto has flags, you can check response.getSuccess()/getUpdated()
				// Otherwise, treat non-exception as success.
				return rid;
			}
			catch (io.grpc.StatusRuntimeException e) {
				handleGrpcException(e);
				return null;
			}
			catch (io.grpc.StatusException e) {
				handleGrpcException(e);
				return null;
			}
		}
		else {
			// -------- CREATE --------
			com.arcadedb.server.grpc.Record recMsg = com.arcadedb.server.grpc.Record.newBuilder()
					.putAllProperties(convertParamsToProto(record.toMap(false))).build();

			com.arcadedb.server.grpc.CreateRecordRequest request = com.arcadedb.server.grpc.CreateRecordRequest.newBuilder()
					.setDatabase(getName()).setType(record.getTypeName()).setRecord(recMsg) // nested Record payload
					.setCredentials(buildCredentials()).build();

			try {
				com.arcadedb.server.grpc.CreateRecordResponse response = blockingStub
						.withDeadlineAfter(getTimeout(), java.util.concurrent.TimeUnit.MILLISECONDS).createRecord(request);

				// Proto returns the newly created RID as a string
				final String ridStr = response.getRid();
				if (ridStr == null || ridStr.isEmpty()) {
					throw new com.arcadedb.exception.DatabaseOperationException("Failed to create record (empty RID)");
				}

				// Construct a RID from the returned string
				// Prefer the single-arg ctor if available; otherwise use the (Database, String)
				// ctor.
				try {
					return new com.arcadedb.database.RID(ridStr);
				}
				catch (NoSuchMethodError | IllegalArgumentException ex) {
					// Fallback for older APIs expecting (Database, String)
					return new com.arcadedb.database.RID(this, ridStr);
				}
			}
			catch (io.grpc.StatusRuntimeException e) {
				handleGrpcException(e);
				return null;
			}
			catch (io.grpc.StatusException e) {
				handleGrpcException(e);
				return null;
			}
		}
	}
//	/**
//	 * Executes a streaming query that returns results in batches.
//	 * Useful for processing large result sets without loading everything into memory.
//	 * 
//	 * @param language the query language (typically "sql")
//	 * @param query the query string
//	 * @param batchSize the number of records per batch (default 100 if <= 0)
//	 * @return Iterator of records that fetches results in batches
//	 */
//	public Iterator<Record> queryStream(final String language, final String query, final int batchSize) {
//	    checkDatabaseIsOpen();
//	    stats.queries.incrementAndGet();
//	    
//	    StreamQueryRequest request = StreamQueryRequest.newBuilder()
//	        .setDatabase(getName())
//	        .setQuery(query)
//	        .setCredentials(buildCredentials())
//	        .setBatchSize(batchSize > 0 ? batchSize : 100)
//	        .build();
//
//	    Iterator<QueryResult> responseIterator = blockingStub
//	        .withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
//	        .streamQuery(request);
//
//	    return new Iterator<Record>() {
//	        private Iterator<com.arcadedb.server.grpc.Record> currentBatch = Collections.emptyIterator();
//
//	        @Override
//	        public boolean hasNext() {
//	            if (currentBatch.hasNext()) {
//	                return true;
//	            }
//	            if (responseIterator.hasNext()) {
//	                QueryResult result = responseIterator.next();
//	                currentBatch = result.getRecordsList().iterator();
//	                return currentBatch.hasNext();
//	            }
//	            return false;
//	        }
//
//	        @Override
//	        public Record next() {
//	            if (!hasNext()) {
//	                throw new NoSuchElementException();
//	            }
//	            return grpcRecordToRecord(currentBatch.next());
//	        }
//	    };
//	}
//
//	/**
//	 * Executes a streaming query with parameters.
//	 */
//	public Iterator<Record> queryStream(final String language, final String query, 
//	                                   final Map<String, Object> params, final int batchSize) {
//	    checkDatabaseIsOpen();
//	    stats.queries.incrementAndGet();
//	    
//	    StreamQueryRequest.Builder requestBuilder = StreamQueryRequest.newBuilder()
//	        .setDatabase(getName())
//	        .setQuery(query)
//	        .setCredentials(buildCredentials())
//	        .setBatchSize(batchSize > 0 ? batchSize : 100);
//	    
//	    if (params != null && !params.isEmpty()) {
//	        requestBuilder.putAllParameters(convertParamsToProto(params));
//	    }
//
//	    Iterator<QueryResult> responseIterator = blockingStub
//	        .withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
//	        .streamQuery(requestBuilder.build());
//
//	    // Same iterator implementation as above
//	    return new Iterator<Record>() {
//	        private Iterator<com.arcadedb.server.grpc.Record> currentBatch = Collections.emptyIterator();
//
//	        @Override
//	        public boolean hasNext() {
//	            if (currentBatch.hasNext()) {
//	                return true;
//	            }
//	            if (responseIterator.hasNext()) {
//	                QueryResult result = responseIterator.next();
//	                currentBatch = result.getRecordsList().iterator();
//	                return currentBatch.hasNext();
//	            }
//	            return false;
//	        }
//
//	        @Override
//	        public Record next() {
//	            if (!hasNext()) {
//	                throw new NoSuchElementException();
//	            }
//	            return grpcRecordToRecord(currentBatch.next());
//	        }
//	    };
//	}
//
//	/**
//	 * Convenience method with default batch size
//	 */
//	public Iterator<Record> queryStream(final String language, final String query) {
//	    return queryStream(language, query, 100);
//	}

	/*
	 * StreamQueryRequest req = StreamQueryRequest.newBuilder() .setDatabase(db)
	 * .setQuery(sql) .setBatchSize(500)
	 * .setRetrievalMode(StreamQueryRequest.RetrievalMode.PAGED)
	 * .setTransaction(TransactionContext.newBuilder() .setBegin(true)
	 * .setCommit(true) // or setRollback(true) .build()) .build();
	 */

	// Convenience: default batch size stays 100, default mode = CURSOR
	public Iterator<Record> queryStream(final String language, final String query) {
		return queryStream(language, query, /* batchSize */100, StreamQueryRequest.RetrievalMode.CURSOR);
	}

	public Iterator<Record> queryStream(final String language, final String query, final int batchSize) {
		return queryStream(language, query, batchSize, StreamQueryRequest.RetrievalMode.CURSOR);
	}

	// NEW: choose retrieval mode
	public Iterator<Record> queryStream(final String language, final String query, final int batchSize,
			final StreamQueryRequest.RetrievalMode mode) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		StreamQueryRequest request = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query).setCredentials(buildCredentials())
				.setBatchSize(batchSize > 0 ? batchSize : 100).setRetrievalMode(mode) // <--- NEW
				.build();

		BlockingClientCall<?, QueryResult> responseIterator = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
				.streamQuery(request);

		return new Iterator<Record>() {

			private Iterator<com.arcadedb.server.grpc.Record> currentBatch = Collections.emptyIterator();

			@Override
			public boolean hasNext() {
				if (currentBatch.hasNext())
					return true;

				try {
					while (responseIterator.hasNext()) {

						QueryResult result = responseIterator.read();

						// Defensive: skip any empty batches the server might send
						if (result.getRecordsCount() == 0) {
							if (result.getIsLastBatch())
								return false;
							continue;
						}

						currentBatch = result.getRecordsList().iterator();
						return true;
					}
				}
				catch (InterruptedException e) {

					throw new RuntimeException("queryStream: interrupted", e);
				}
				catch (StatusException e) {

					throw new RuntimeException("queryStream: error", e);
				}

				return false;
			}

			@Override
			public Record next() {
				if (!hasNext())
					throw new NoSuchElementException();
				return grpcRecordToRecord(currentBatch.next());
			}
		};
	}

	// PARAMETERIZED variant with retrieval mode
	public Iterator<Record> queryStream(final String language, final String query, final Map<String, Object> params, final int batchSize,
			final StreamQueryRequest.RetrievalMode mode) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		StreamQueryRequest.Builder b = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query).setCredentials(buildCredentials())
				.setBatchSize(batchSize > 0 ? batchSize : 100).setRetrievalMode(mode); // <--- NEW

		if (params != null && !params.isEmpty()) {
			b.putAllParameters(convertParamsToProto(params));
		}

		BlockingClientCall<?, QueryResult> responseIterator = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
				.streamQuery(b.build());

		return new Iterator<Record>() {
			private Iterator<com.arcadedb.server.grpc.Record> currentBatch = Collections.emptyIterator();

			@Override
			public boolean hasNext() {
				if (currentBatch.hasNext())
					return true;

				try {
					while (responseIterator.hasNext()) {
						QueryResult result = responseIterator.read();

						if (result.getRecordsCount() == 0) {
							if (result.getIsLastBatch())
								return false;
							continue;
						}

						currentBatch = result.getRecordsList().iterator();
						return true;
					}
				}
				catch (InterruptedException e) {

					throw new RuntimeException("queryStream: interrupted", e);
				}
				catch (StatusException e) {

					throw new RuntimeException("queryStream: error", e);
				}

				return false;
			}

			@Override
			public Record next() {
				if (!hasNext())
					throw new NoSuchElementException();
				return grpcRecordToRecord(currentBatch.next());
			}
		};
	}

	// Keep the old signature working (defaults to CURSOR)
	public Iterator<Record> queryStream(final String language, final String query, final Map<String, Object> params, final int batchSize) {
		return queryStream(language, query, params, batchSize, StreamQueryRequest.RetrievalMode.CURSOR);
	}

	public static final class QueryBatch {
		private final List<Record> records;
		private final int totalInBatch;
		private final long runningTotal;
		private final boolean lastBatch;

		public QueryBatch(List<Record> list, int totalInBatch, long runningTotal, boolean lastBatch) {
			this.records = list;
			this.totalInBatch = totalInBatch;
			this.runningTotal = runningTotal;
			this.lastBatch = lastBatch;
		}

		public List<Record> records() {
			return records;
		}

		public int totalInBatch() {
			return totalInBatch;
		}

		public long runningTotal() {
			return runningTotal;
		}

		public boolean isLastBatch() {
			return lastBatch;
		}
	}

	public Iterator<QueryBatch> queryStreamBatches(final String language, final String query, final Map<String, Object> params,
			final int batchSize, final StreamQueryRequest.RetrievalMode mode) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		StreamQueryRequest.Builder b = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query).setCredentials(buildCredentials())
				.setBatchSize(batchSize > 0 ? batchSize : 100).setRetrievalMode(mode);

		if (params != null && !params.isEmpty()) {
			b.putAllParameters(convertParamsToProto(params));
		}

		BlockingClientCall<?, QueryResult> responseIterator = blockingStub.withWaitForReady() // optional, improves robustness
				.withDeadlineAfter(/* e.g. */ 10, TimeUnit.MINUTES).streamQuery(b.build());

		return new Iterator<QueryBatch>() {
			private QueryBatch nextBatch = null;
			private boolean drained = false;

			@Override
			public boolean hasNext() {
				if (nextBatch != null)
					return true;
				if (drained)
					return false;

				try {
					while (responseIterator.hasNext()) {
						QueryResult qr = responseIterator.read();
						int n = qr.getTotalRecordsInBatch(); // server-populated
						// Guard: some servers could omit this; fallback to list size.
						if (n == 0)
							n = qr.getRecordsCount();

						if (qr.getRecordsCount() == 0 && !qr.getIsLastBatch()) {
							// skip empty non-terminal batch
							continue;
						}

						List<Record> converted = new ArrayList<>(qr.getRecordsCount());
						for (com.arcadedb.server.grpc.Record gr : qr.getRecordsList()) {
							converted.add(grpcRecordToRecord(gr));
						}

						nextBatch = new QueryBatch(converted, n, qr.getRunningTotalEmitted(), qr.getIsLastBatch());

						if (qr.getIsLastBatch()) {
							drained = true; // no more after this (even if server sent empty terminal)
						}
						return true;
					}
				}
				catch (InterruptedException e) {

					throw new RuntimeException("queryStreamBatches: interrupted", e);
				}
				catch (StatusException e) {

					throw new RuntimeException("queryStreamBatches: error", e);
				}

				drained = true;
				return false;
			}

			@Override
			public QueryBatch next() {
				if (!hasNext())
					throw new NoSuchElementException();
				QueryBatch out = nextBatch;
				nextBatch = null;
				return out;
			}
		};
	}

	public Iterator<com.arcadedb.server.grpc.Record> queryStream(String database, String sql, Map<String, Object> params, int batchSize,
			com.arcadedb.server.grpc.StreamQueryRequest.RetrievalMode mode, com.arcadedb.server.grpc.TransactionContext tx, long timeoutMs) {

		var reqB = com.arcadedb.server.grpc.StreamQueryRequest.newBuilder().setDatabase(database).setQuery(sql)
				.putAllParameters(convertParamsToProto(params)).setCredentials(buildCredentials()).setBatchSize(batchSize > 0 ? batchSize : 100)
				.setRetrievalMode(mode);

		if (tx != null)
			reqB.setTransaction(tx);

		BlockingClientCall<?, QueryResult> it = blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
				.streamQuery(reqB.build());

		return new Iterator<>() {

			private Iterator<com.arcadedb.server.grpc.Record> curr = java.util.Collections.emptyIterator();

			public boolean hasNext() {

				if (curr.hasNext())
					return true;

				try {
					if (it.hasNext()) {
						curr = it.read().getRecordsList().iterator();
						return hasNext();
					}
				}
				catch (InterruptedException e) {

					throw new RuntimeException("queryStream: interrupted", e);
				}
				catch (StatusException e) {

					throw new RuntimeException("queryStream: error", e);
				}

				return false;
			}

			public com.arcadedb.server.grpc.Record next() {
				if (!hasNext())
					throw new java.util.NoSuchElementException();
				return curr.next();
			}
		};
	}

	public Iterator<QueryBatch> queryStreamBatches(String passLabel, String sql, Map<String, Object> params, int batchSize,
			com.arcadedb.server.grpc.StreamQueryRequest.RetrievalMode mode, com.arcadedb.server.grpc.TransactionContext tx, long timeoutMs) {

		var reqB = com.arcadedb.server.grpc.StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(sql)
				.putAllParameters(convertParamsToProto(params)).setCredentials(buildCredentials()).setBatchSize(batchSize > 0 ? batchSize : 100)
				.setRetrievalMode(mode);

		if (tx != null)
			reqB.setTransaction(tx);

		BlockingClientCall<?, QueryResult> respIter = blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
				.streamQuery(reqB.build());

		return new Iterator<>() {

			public boolean hasNext() {

				try {
					return respIter.hasNext();
				}
				catch (InterruptedException e) {

					throw new RuntimeException("queryStreamBatches: interrupted", e);
				}
				catch (StatusException e) {

					throw new RuntimeException("queryStreamBatches: error", e);
				}
			}

			public QueryBatch next() {

				QueryResult qr;

				try {
					qr = respIter.read();
					List<Record> converted = new ArrayList<>(qr.getRecordsCount());
					for (com.arcadedb.server.grpc.Record gr : qr.getRecordsList()) {
						converted.add(grpcRecordToRecord(gr));
					}

					return new QueryBatch(converted, qr.getTotalRecordsInBatch(), // int totalInBatch
							qr.getRunningTotalEmitted(), // long runningTotal
							qr.getIsLastBatch() // boolean lastBatch
					);
				}
				catch (InterruptedException e) {

					throw new RuntimeException("queryStreamBatches: interrupted", e);
				}
				catch (StatusException e) {

					throw new RuntimeException("queryStreamBatches: error", e);
				}
			}
		};
	}

	public String createVertex(String cls, Map<String, Object> props, long timeoutMs) {
		// Build the nested proto Record payload
		com.arcadedb.server.grpc.Record recMsg = com.arcadedb.server.grpc.Record.newBuilder().putAllProperties(convertParamsToProto(props))
				.build();

		// Build the Create request
		com.arcadedb.server.grpc.CreateRecordRequest req = com.arcadedb.server.grpc.CreateRecordRequest.newBuilder().setDatabase(getName())
				.setType(cls).setRecord(recMsg) // <<<< NESTED RECORD (not top-level properties)
				.setCredentials(buildCredentials()).build();

		// Call RPC
		com.arcadedb.server.grpc.CreateRecordResponse res;

		try {

			res = blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS).createRecord(req);

			// Response carries new RID string
			return res.getRid(); // e.g., "#12:0"
		}
		catch (StatusException e) {

			throw new RuntimeException("Failed to create vertex", e);
		}
	}

	public boolean updateRecord(String rid, Map<String, Object> props, long timeoutMs) {

		// Build the nested proto Record payload for a PATCH
		com.arcadedb.server.grpc.Record record = com.arcadedb.server.grpc.Record.newBuilder().putAllProperties(convertParamsToProto(props))
				.build();

		// Build the Update request (use setRecord(...) for full replace instead)
		com.arcadedb.server.grpc.UpdateRecordRequest req = com.arcadedb.server.grpc.UpdateRecordRequest.newBuilder().setDatabase(getName())
				.setRid(rid).setRecord(record) // <<<< oneof {record|partial}
				.setCredentials(buildCredentials()).build();

		// Call RPC
		com.arcadedb.server.grpc.UpdateRecordResponse res;

		try {
			res = blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS).updateRecord(req);

			// Choose the flag your proto defines. Most builds expose getSuccess().
			return res.getSuccess();
			// If your generated class exposes getUpdated() instead, use:
			// return res.getUpdated();
		}
		catch (StatusException e) {

			throw new RuntimeException("Failed to update record", e);
		}
	}

	public boolean deleteRecord(String rid, long timeoutMs) {
		var req = com.arcadedb.server.grpc.DeleteRecordRequest.newBuilder().setDatabase(getName()).setRid(rid).setCredentials(buildCredentials())
				.build();
		DeleteRecordResponse res;

		try {

			res = blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS).deleteRecord(req);
			return res.getDeleted();
		}
		catch (StatusException e) {

			throw new RuntimeException("Failed to delete record", e);
		}
	}

	@Override
	public RemoteTransactionExplicitLock acquireLock() {
		// Need gRPC-specific implementation
		if (explicitLock == null)
			explicitLock = new RemoteGrpcTransactionExplicitLock(this);

		return explicitLock;
	}

	@Override
	public long countBucket(final String bucketName) {
		checkDatabaseIsOpen();
		stats.countBucket.incrementAndGet();
		ResultSet result = query("sql", "select count(*) as count from bucket:" + bucketName);
		if (result.hasNext()) {
			Number count = result.next().getProperty("count");
			return count != null ? count.longValue() : 0;
		}
		return 0;
	}

	@Override
	public long countType(final String typeName, final boolean polymorphic) {
		checkDatabaseIsOpen();
		stats.countType.incrementAndGet();
		final String appendix = polymorphic ? "" : " where @type = '" + typeName + "'";
		ResultSet result = query("sql", "select count(*) as count from " + typeName + appendix);
		if (result.hasNext()) {
			Number count = result.next().getProperty("count");
			return count != null ? count.longValue() : 0;
		}
		return 0;
	}

	@Override
	public Record lookupByRID(final RID rid) {
		return lookupByRID(rid, true);
	}

	@Override
	public boolean existsRecord(RID rid) {
		stats.existsRecord.incrementAndGet();
		if (rid == null)
			throw new IllegalArgumentException("Record is null");
		try {
			return lookupByRID(rid, false) != null;
		}
		catch (RecordNotFoundException e) {
			return false;
		}
	}

	public com.arcadedb.server.grpc.InsertSummary insertBulk(final com.arcadedb.server.grpc.InsertOptions options,
			final java.util.List<com.arcadedb.server.grpc.Record> protoRows, final long timeoutMs) {

		// Ensure options carry DB + credentials as the server expects
		com.arcadedb.server.grpc.InsertOptions.Builder ob = options.toBuilder();

		if (options.getDatabase() == null || options.getDatabase().isEmpty()) {
			ob.setDatabase(getName()); // your wrapper's DB name
		}

		boolean missingCreds = !options.hasCredentials() || options.getCredentials().getUsername().isEmpty();
		if (missingCreds) {
			ob.setCredentials(buildCredentials()); // your existing helper used by queries
		}

		InsertOptions newOptions = ob.build();

		com.arcadedb.server.grpc.BulkInsertRequest req = com.arcadedb.server.grpc.BulkInsertRequest.newBuilder().setOptions(newOptions)
				.addAllRows(protoRows).build();

		try {
			return blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS).bulkInsert(req);
		}
		catch (StatusException e) {

			throw new RuntimeException("insertBulk() -> failed: " + e.getStatus());
		}
	}

	// Convenience overload that accepts domain rows (convert first)
	public com.arcadedb.server.grpc.InsertSummary insertBulkAsListOfMaps(final com.arcadedb.server.grpc.InsertOptions options,
			final java.util.List<java.util.Map<String, Object>> rows, final long timeoutMs) {

		java.util.List<com.arcadedb.server.grpc.Record> protoRows = rows.stream().map(this::toProtoRecordFromMap) // your converter
				.collect(java.util.stream.Collectors.toList());

		return insertBulk(options, protoRows, timeoutMs);
	}

	public com.arcadedb.server.grpc.InsertSummary ingestStream(final com.arcadedb.server.grpc.InsertOptions options,
			final java.util.List<com.arcadedb.server.grpc.Record> protoRows, final int chunkSize, final long timeoutMs)
			throws InterruptedException {

		final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.atomic.AtomicReference<com.arcadedb.server.grpc.InsertSummary> summaryRef = new java.util.concurrent.atomic.AtomicReference<>();

		// Ensure options carry DB + credentials as the server expects
		com.arcadedb.server.grpc.InsertOptions.Builder ob = options.toBuilder();

		if (options.getDatabase() == null || options.getDatabase().isEmpty()) {
			ob.setDatabase(getName()); // your wrapper's DB name
		}

		boolean missingCreds = !options.hasCredentials() || options.getCredentials().getUsername().isEmpty();

		if (missingCreds) {
			ob.setCredentials(buildCredentials()); // your existing helper used by queries
		}

		io.grpc.stub.StreamObserver<com.arcadedb.server.grpc.InsertSummary> resp = new io.grpc.stub.StreamObserver<>() {
			@Override
			public void onNext(com.arcadedb.server.grpc.InsertSummary value) {
				summaryRef.set(value);
			}

			@Override
			public void onError(Throwable t) {
				done.countDown();
			}

			@Override
			public void onCompleted() {
				done.countDown();
			}
		};

		// Use the write service async stub, per-call deadline
		var stub = this.asyncStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);

		final io.grpc.stub.StreamObserver<com.arcadedb.server.grpc.InsertChunk> req = stub.insertStream(resp);

		// Stream chunks
		final String sessionId = "sess-" + System.nanoTime();

		long seq = 1;

		for (int i = 0; i < protoRows.size(); i += chunkSize) {

			final int end = Math.min(i + chunkSize, protoRows.size());

			final com.arcadedb.server.grpc.InsertChunk chunk = com.arcadedb.server.grpc.InsertChunk.newBuilder().setSessionId(sessionId)
					.setOptions(ob.build()).setChunkSeq(seq++).addAllRows(protoRows.subList(i, end)).build();

			req.onNext(chunk);
		}

		req.onCompleted();

		done.await(timeoutMs + 5_000, java.util.concurrent.TimeUnit.MILLISECONDS);

		final com.arcadedb.server.grpc.InsertSummary s = summaryRef.get();
		return (s != null) ? s : com.arcadedb.server.grpc.InsertSummary.newBuilder().setReceived(protoRows.size()).build();
	}

	// Convenience overload
	public com.arcadedb.server.grpc.InsertSummary ingestStreamAsListOfMaps(final com.arcadedb.server.grpc.InsertOptions options,
			final java.util.List<java.util.Map<String, Object>> rows, final int chunkSize, final long timeoutMs) throws InterruptedException {

		java.util.List<com.arcadedb.server.grpc.Record> protoRows = rows.stream().map(this::toProtoRecordFromMap)
				.collect(java.util.stream.Collectors.toList());

		return ingestStream(options, protoRows, chunkSize, timeoutMs);
	}

	/**
	 * Core implementation of InsertBidirectional ingest: - Sends a START once with
	 * effective InsertOptions (db/creds enforced) - Streams chunks of mapped rows
	 * (using chunkSize) while respecting backpressure (maxInflight) - Collects
	 * per-batch ACKs and returns COMMITTED summary if present, else aggregates ACKs
	 *
	 * @param <T>         row type (e.g., com.arcadedb.database.Record or
	 *                    Map<String,Object>)
	 * @param rows        source rows
	 * @param options     base InsertOptions; db/credentials will be injected if
	 *                    missing
	 * @param chunkSize   max rows per chunk
	 * @param maxInflight max number of unacknowledged chunks allowed
	 * @param timeoutMs   overall client-side timeout
	 * @param mapper      maps T → grpc Record (must be pure & fast)
	 * @return InsertSummary as returned by COMMITTED, or aggregated from ACKs if
	 *         COMMITTED missing
	 * @throws InterruptedException if the await is interrupted
	 */
	private com.arcadedb.server.grpc.InsertSummary ingestBidiCore(final java.util.List<?> rows,
			final com.arcadedb.server.grpc.InsertOptions options, final int chunkSize, final int maxInflight, final long timeoutMs,
			final java.util.function.Function<Object, com.arcadedb.server.grpc.Record> mapper) throws InterruptedException {

		// 1) Ensure options carry DB + credentials the server expects
		final com.arcadedb.server.grpc.InsertOptions.Builder ob = options.toBuilder();
		if (options.getDatabase() == null || options.getDatabase().isEmpty()) {
			ob.setDatabase(getName());
		}
		final boolean missingCreds = !options.hasCredentials() || options.getCredentials().getUsername().isEmpty();
		if (missingCreds) {
			ob.setCredentials(buildCredentials());
		}
		final com.arcadedb.server.grpc.InsertOptions effectiveOpts = ob.build();

		// 2) Pre-map rows → proto once (outside the observer)
		final java.util.List<com.arcadedb.server.grpc.Record> protoRows = rows.stream().map(mapper)
				.collect(java.util.stream.Collectors.toList());

		// 3) Streaming state
		final String sessionId = "sess-" + System.nanoTime();
		final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.atomic.AtomicLong seq = new java.util.concurrent.atomic.AtomicLong(1);
		final java.util.concurrent.atomic.AtomicInteger cursor = new java.util.concurrent.atomic.AtomicInteger(0);
		final java.util.concurrent.atomic.AtomicInteger sent = new java.util.concurrent.atomic.AtomicInteger(0);
		final java.util.concurrent.atomic.AtomicInteger acked = new java.util.concurrent.atomic.AtomicInteger(0);
		final java.util.concurrent.atomic.AtomicReference<com.arcadedb.server.grpc.InsertSummary> committed = new java.util.concurrent.atomic.AtomicReference<>();
		final java.util.List<com.arcadedb.server.grpc.BatchAck> acks = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

		final io.grpc.stub.ClientResponseObserver<com.arcadedb.server.grpc.InsertRequest, com.arcadedb.server.grpc.InsertResponse> observer = new io.grpc.stub.ClientResponseObserver<>() {

			io.grpc.stub.ClientCallStreamObserver<com.arcadedb.server.grpc.InsertRequest> req;
			volatile boolean started = false;

			@Override
			public void beforeStart(io.grpc.stub.ClientCallStreamObserver<com.arcadedb.server.grpc.InsertRequest> r) {
				this.req = r;
				r.disableAutoInboundFlowControl();
				r.setOnReadyHandler(this::drain);
			}

			private void drain() {
				if (!req.isReady())
					return;

				// Send START exactly once
				if (!started) {
					req.onNext(com.arcadedb.server.grpc.InsertRequest.newBuilder()
							.setStart(com.arcadedb.server.grpc.Start.newBuilder().setOptions(effectiveOpts)).build());
					started = true;
				}

				// While there is capacity (backpressure) and in-flight < maxInflight → send
				// chunks
				while (req.isReady() && (sent.get() - acked.get()) < maxInflight) {
					final int start = cursor.get();
					if (start >= protoRows.size())
						break;

					final int end = Math.min(start + chunkSize, protoRows.size());
					final java.util.List<com.arcadedb.server.grpc.Record> slice = protoRows.subList(start, end);

					final com.arcadedb.server.grpc.InsertChunk chunk = com.arcadedb.server.grpc.InsertChunk.newBuilder().setSessionId(sessionId)
							.setChunkSeq(seq.getAndIncrement()).addAllRows(slice).build();

					req.onNext(com.arcadedb.server.grpc.InsertRequest.newBuilder().setChunk(chunk).build());

					cursor.set(end);
					sent.incrementAndGet();
				}

				// If we sent everything and all ACKed → send COMMIT and half-close
				if (cursor.get() >= protoRows.size() && acked.get() >= sent.get()) {
					req.onNext(com.arcadedb.server.grpc.InsertRequest.newBuilder()
							.setCommit(com.arcadedb.server.grpc.Commit.newBuilder().setSessionId(sessionId)).build());
					req.onCompleted();
				}
			}

			@Override
			public void onNext(com.arcadedb.server.grpc.InsertResponse v) {
				switch (v.getMsgCase()) {
				case STARTED -> {
					/* ok */ }
				case BATCH_ACK -> {
					acks.add(v.getBatchAck());
					acked.incrementAndGet();
					// Now that capacity freed up, try to send more
					drain();
				}
				case COMMITTED -> committed.set(v.getCommitted().getSummary());
				case MSG_NOT_SET -> {
					/* ignore */ }
				}
				// NOTE: no req.request(1) here; responses are server-pushed
			}

			@Override
			public void onError(Throwable t) {
				done.countDown();
			}

			@Override
			public void onCompleted() {
				done.countDown();
			}
		};

		// 4) Kick off the bidi call with deadline
		this.asyncStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS).insertBidirectional(observer);

		// 5) Wait for completion
		done.await(timeoutMs + 5_000, java.util.concurrent.TimeUnit.MILLISECONDS);

		// 6) Prefer COMMITTED summary if present; otherwise aggregate ACKs
		final com.arcadedb.server.grpc.InsertSummary finalSummary = committed.get();
		if (finalSummary != null)
			return finalSummary;

		final long ins = acks.stream().mapToLong(com.arcadedb.server.grpc.BatchAck::getInserted).sum();
		final long upd = acks.stream().mapToLong(com.arcadedb.server.grpc.BatchAck::getUpdated).sum();
		final long ign = acks.stream().mapToLong(com.arcadedb.server.grpc.BatchAck::getIgnored).sum();
		final long fail = acks.stream().mapToLong(com.arcadedb.server.grpc.BatchAck::getFailed).sum();

		return com.arcadedb.server.grpc.InsertSummary.newBuilder().setReceived(protoRows.size()).setInserted(ins).setUpdated(upd).setIgnored(ign)
				.setFailed(fail).build();
	}

	/**
	 * Pushes domain {@code com.arcadedb.database.Record} rows via
	 * InsertBidirectional with per-batch ACKs.
	 *
	 * <p>
	 * Behavior:
	 * <ul>
	 * <li>Sends a {@code START} with effective {@link InsertOptions}
	 * (DB/credentials auto-injected if missing).</li>
	 * <li>Streams chunks of rows (size = {@code chunkSize}) while respecting
	 * backpressure (at most {@code maxInflight} unacknowledged chunks).</li>
	 * <li>Each server {@code BATCH_ACK} reduces in-flight count and triggers
	 * sending more.</li>
	 * <li>When all chunks are acked, sends {@code COMMIT} and returns the final
	 * {@code InsertSummary} from the server if available; otherwise aggregates ACKs
	 * to form a summary.</li>
	 * </ul>
	 *
	 * @param rows        domain records to send
	 * @param opts        base InsertOptions (target class, conflict mode, tx mode).
	 *                    DB/credentials will be filled if missing
	 * @param chunkSize   number of rows per chunk (must be > 0)
	 * @param maxInflight max unacknowledged chunks allowed (backpressure window)
	 * @return InsertSummary from COMMITTED or aggregated from ACKs
	 * @throws InterruptedException if waiting for completion is interrupted
	 */
	public com.arcadedb.server.grpc.InsertSummary ingestBidi(final java.util.List<com.arcadedb.database.Record> rows,
			final com.arcadedb.server.grpc.InsertOptions opts, final int chunkSize, final int maxInflight, final long timeoutMs) throws InterruptedException {
		
		return ingestBidiCore(rows, opts, chunkSize, maxInflight, timeoutMs,
				(Object o) -> toProtoRecordFromDbRecord((com.arcadedb.database.Record) o));
	}

	public com.arcadedb.server.grpc.InsertSummary ingestBidi(final java.util.List<com.arcadedb.database.Record> rows,
			final com.arcadedb.server.grpc.InsertOptions opts, final int chunkSize, final int maxInflight) throws InterruptedException {
		
		return ingestBidiCore(rows, opts, chunkSize, maxInflight, /* timeoutMs */ 5 * 60_000L, 
				(Object o) -> toProtoRecordFromDbRecord((com.arcadedb.database.Record) o));
	}
	
	/**
	 * Pushes map-shaped rows (property map per row) via InsertBidirectional with
	 * per-batch ACKs.
	 *
	 * <p>
	 * Behavior is identical to {@link #ingestBidi(List, InsertOptions, int, int)}
	 * but accepts pre-materialized property maps. This is useful when ingesting
	 * from JSON/CSV or an HTTP pipeline.
	 *
	 * @param options     base InsertOptions (target class, conflict mode, tx mode).
	 *                    DB/credentials will be filled if missing
	 * @param rows        list of maps (one per record) to insert/update
	 * @param chunkSize   rows per chunk
	 * @param maxInflight max unacknowledged chunks allowed
	 * @param timeoutMs   overall client-side timeout
	 * @return InsertSummary from COMMITTED or aggregated from ACKs
	 * @throws InterruptedException if the await is interrupted
	 */
	public com.arcadedb.server.grpc.InsertSummary ingestBidi(final com.arcadedb.server.grpc.InsertOptions options,
			final java.util.List<java.util.Map<String, Object>> rows, final int chunkSize, final int maxInflight)
			throws InterruptedException {
		
		return ingestBidiCore(rows, options, chunkSize, maxInflight, /* timeoutMs */ 5 * 60_000L,
				(Object o) -> toProtoRecordFromMap((java.util.Map<String, Object>) o));
	}

	public com.arcadedb.server.grpc.InsertSummary ingestBidi(final com.arcadedb.server.grpc.InsertOptions options,
			final java.util.List<java.util.Map<String, Object>> rows, final int chunkSize, final int maxInflight, final long timeoutMs)
			throws InterruptedException {
		
		return ingestBidiCore(rows, options, chunkSize, maxInflight, timeoutMs,
				(Object o) -> toProtoRecordFromMap((java.util.Map<String, Object>) o));
	}
	
	// Map -> PROTO Value
	private com.arcadedb.server.grpc.Record toProtoRecord(Map<String, Object> row) {
		com.arcadedb.server.grpc.Record.Builder b = com.arcadedb.server.grpc.Record.newBuilder();
		// Example if your proto uses a map<string, google.protobuf.Value>
		// fields/properties
		row.forEach((k, v) -> b.putProperties(k, toProtoValue(v)));
		return b.build();
	}

	// Map -> PROTO Record
	private com.arcadedb.server.grpc.Record toProtoRecordFromMap(java.util.Map<String, Object> row) {
		com.arcadedb.server.grpc.Record.Builder b = com.arcadedb.server.grpc.Record.newBuilder();
		row.forEach((k, v) -> b.putProperties(k, toProtoValue(v)));
		return b.build();
	}

	// Domain Record (storage) -> PROTO Record
	private com.arcadedb.server.grpc.Record toProtoRecordFromDbRecord(com.arcadedb.database.Record rec) {

		// Promote to a read-only Document; null if this record isn't a document
		com.arcadedb.database.Document doc = rec.asDocument();
		if (doc == null) {
			return com.arcadedb.server.grpc.Record.getDefaultInstance();
		}

		com.arcadedb.server.grpc.Record.Builder b = com.arcadedb.server.grpc.Record.newBuilder();

		// Iterate document properties
		for (String name : doc.getPropertyNames()) {
			// Depending on your ArcadeDB version, use ONE of these:
			Object v = doc.get(name); // <-- common in ArcadeDB
			// Object v = doc.getProperty(name); // <-- use this if doc.get(...) doesn't
			// exist in your API
			b.putProperties(name, toProtoValue(v)); // or putFields(...) if your proto uses "fields"
		}
		return b.build();
	}

	// google.protobuf.Value -> Java (mirror of your toProtoValue)
	private static Object fromProtoValue(com.google.protobuf.Value v) {
		if (v == null)
			return null;
		switch (v.getKindCase()) {
		case NULL_VALUE:
			return null;
		case BOOL_VALUE:
			return v.getBoolValue();
		case NUMBER_VALUE:
			return v.getNumberValue();
		case STRING_VALUE:
			return v.getStringValue();
		case LIST_VALUE: {
			java.util.List<Object> out = new java.util.ArrayList<>();
			for (com.google.protobuf.Value item : v.getListValue().getValuesList()) {
				out.add(fromProtoValue(item));
			}
			return out;
		}
		case STRUCT_VALUE: {
			java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
			v.getStructValue().getFieldsMap().forEach((k, vv) -> m.put(k, fromProtoValue(vv)));
			return m; // server-side schema can later coerce EMBEDDED if needed
		}
		case KIND_NOT_SET:
		default:
			return null;
		}
	}

	// Query Result -> PROTO Record
	private com.arcadedb.server.grpc.Record toProtoRecordFromResult(Result res) {
		com.arcadedb.server.grpc.Record.Builder b = com.arcadedb.server.grpc.Record.newBuilder();
		for (String name : res.getPropertyNames()) {
			Object v = res.getProperty(name);
			b.putProperties(name, toProtoValue(v)); // or putFields(...)
		}
		return b.build();
	}

	// Convert Java object -> protobuf Value (extend as needed)
	private com.google.protobuf.Value toProtoValue(Object v) {
		com.google.protobuf.Value.Builder vb = com.google.protobuf.Value.newBuilder();
		if (v == null)
			return vb.setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();
		if (v instanceof String s)
			return vb.setStringValue(s).build();
		if (v instanceof Boolean b)
			return vb.setBoolValue(b).build();
		if (v instanceof Number n)
			return vb.setNumberValue(n.doubleValue()).build();
		if (v instanceof java.time.Instant t)
			return vb.setStringValue(t.toString()).build(); // or a Struct for Timestamp if you prefer
		if (v instanceof java.util.Map<?, ?> m) {
			var sb = com.google.protobuf.Struct.newBuilder();
			for (var e : m.entrySet())
				sb.putFields(String.valueOf(e.getKey()), toProtoValue(e.getValue()));
			return vb.setStructValue(sb.build()).build();
		}
		if (v instanceof java.util.List<?> list) {
			var lb = com.google.protobuf.ListValue.newBuilder();
			for (Object e : list)
				lb.addValues(toProtoValue(e));
			return vb.setListValue(lb.build()).build();
		}
		return vb.setStringValue(String.valueOf(v)).build();
	}

	// Override HTTP-specific methods to prevent usage
//	@Override
//	HttpRequest.Builder createRequestBuilder(String httpMethod, String url) {
//		throw new UnsupportedOperationException("HTTP operations not supported in gRPC implementation");
//	}
//
//	@Override
//	protected ResultSet createResultSet(JSONObject response) {
//		throw new UnsupportedOperationException("JSON operations not supported in gRPC implementation");
//	}
//
//	@Override
//	protected Result json2Result(JSONObject result) {
//		throw new UnsupportedOperationException("JSON operations not supported in gRPC implementation");
//	}
//
//	@Override
//	protected Record json2Record(JSONObject result) {
//		throw new UnsupportedOperationException("JSON operations not supported in gRPC implementation");
//	}

	// gRPC-specific helper methods

	public DatabaseCredentials buildCredentials() {
		return DatabaseCredentials.newBuilder().setUsername(getUserName()).setPassword(getUserPassword()).build();
	}

	private TransactionIsolation mapIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL level) {
		switch (level) {
//      case READ_UNCOMMITTED:
//        return TransactionIsolation.READ_UNCOMMITTED;
		case READ_COMMITTED:
			return TransactionIsolation.READ_COMMITTED;
		case REPEATABLE_READ:
			return TransactionIsolation.REPEATABLE_READ;
//      case SERIALIZABLE:
//        return TransactionIsolation.SERIALIZABLE;
		default:
			return TransactionIsolation.READ_COMMITTED;
		}
	}

	// ---- TX helpers -------------------------------------------------------------
	private static com.arcadedb.server.grpc.TransactionContext txBeginCommit() {
		return com.arcadedb.server.grpc.TransactionContext.newBuilder().setBegin(true).setCommit(true).build();
	}

	private static com.arcadedb.server.grpc.TransactionContext txNone() {
		return com.arcadedb.server.grpc.TransactionContext.getDefaultInstance();
	}

	// Optional: language defaulting (server defaults to "sql" too)
	private static String langOrDefault(String language) {
		return (language == null || language.isEmpty()) ? "sql" : language;
	}

	private Iterator<Record> streamQuery(final String query) {

		StreamQueryRequest request = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query).setCredentials(buildCredentials())
				.setBatchSize(100).build();

		final BlockingClientCall<?, QueryResult> blockingClientCall = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
				.streamQuery(request);

		return new Iterator<Record>() {
			private Iterator<com.arcadedb.server.grpc.Record> currentBatch = Collections.emptyIterator();

			@Override
			public boolean hasNext() {

				if (currentBatch.hasNext()) {
					return true;
				}

				try {

					if (blockingClientCall.hasNext()) {
						QueryResult result = blockingClientCall.read();
						currentBatch = result.getRecordsList().iterator();
						return currentBatch.hasNext();
					}
				}
				catch (InterruptedException e) {

					throw new RuntimeException("streamQuery: Interrupted", e);
				}
				catch (StatusException e) {

					throw new RuntimeException("streamQuery: error occured", e);
				}

				return false;
			}

			@Override
			public Record next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				return grpcRecordToRecord(currentBatch.next());
			}
		};
	}

	private ResultSet createGrpcResultSet(ExecuteQueryResponse response) {
		InternalResultSet resultSet = new InternalResultSet();
		for (QueryResult queryResult : response.getResultsList()) {
			for (com.arcadedb.server.grpc.Record record : queryResult.getRecordsList()) {
				resultSet.add(grpcRecordToResult(record));
			}
		}
		return resultSet;
	}

	private Result grpcRecordToResult(com.arcadedb.server.grpc.Record grpcRecord) {
		Record record = grpcRecordToRecord(grpcRecord);
		if (record == null) {
			Map<String, Object> properties = new HashMap<>();
			grpcRecord.getPropertiesMap().forEach((k, v) -> properties.put(k, valueToObject(v)));
			return new ResultInternal(properties);
		}
		return new ResultInternal(record);
	}

	private Record grpcRecordToRecord(com.arcadedb.server.grpc.Record grpcRecord) {
		Map<String, Object> map = new HashMap<>();

		// Convert properties
		grpcRecord.getPropertiesMap().forEach((k, v) -> map.put(k, valueToObject(v)));

		// Add metadata
		map.put("@rid", grpcRecord.getRid());
		map.put("@type", grpcRecord.getType());
		map.put("@cat", mapRecordType(grpcRecord));

		String cat = (String) map.get("@cat");
		switch (cat) {
		case "d":
			return new RemoteImmutableDocument(this, map);
		case "v":
			return new RemoteImmutableVertex(this, map);
		case "e":
			return new RemoteImmutableEdge(this, map);
		default:
			return null;
		}
	}

	private String mapRecordType(com.arcadedb.server.grpc.Record grpcRecord) {
		// Determine record category from type name
		String typeName = grpcRecord.getType();

		// Check schema to determine actual type
		try {
			if (getSchema().existsType(typeName)) {
				Object type = getSchema().getType(typeName);
				if (type instanceof com.arcadedb.schema.VertexType) {
					return "v";
				}
				else if (type instanceof com.arcadedb.schema.EdgeType) {
					return "e";
				}
			}
		}
		catch (Exception e) {
			// Fall back to name-based detection
		}

		// Fall back to name-based detection
		if (typeName.contains("Vertex") || typeName.startsWith("V_")) {
			return "v";
		}
		else if (typeName.contains("Edge") || typeName.startsWith("E_")) {
			return "e";
		}
		else {
			return "d";
		}
	}

	private Map<String, Value> convertParamsToProto(Map<String, Object> params) {
		Map<String, Value> protoParams = new HashMap<>();
		for (Map.Entry<String, Object> entry : params.entrySet()) {
			protoParams.put(entry.getKey(), objectToValue(entry.getValue()));
		}
		return protoParams;
	}

	private Value objectToValue(Object obj) {
		Value.Builder builder = Value.newBuilder();
		if (obj == null) {
			builder.setNullValue(com.google.protobuf.NullValue.NULL_VALUE);
		}
		else if (obj instanceof Boolean) {
			builder.setBoolValue((Boolean) obj);
		}
		else if (obj instanceof Number) {
			builder.setNumberValue(((Number) obj).doubleValue());
		}
		else if (obj instanceof String) {
			builder.setStringValue((String) obj);
		}
		else if (obj instanceof RID) {
			builder.setStringValue(obj.toString());
		}
		else if (obj instanceof Map) {
			Struct.Builder structBuilder = Struct.newBuilder();
			((Map<?, ?>) obj).forEach((k, v) -> structBuilder.putFields(k.toString(), objectToValue(v)));
			builder.setStructValue(structBuilder.build());
		}
		else if (obj instanceof Collection) {
			com.google.protobuf.ListValue.Builder listBuilder = com.google.protobuf.ListValue.newBuilder();
			((Collection<?>) obj).forEach(item -> listBuilder.addValues(objectToValue(item)));
			builder.setListValue(listBuilder.build());
		}
		else {
			builder.setStringValue(obj.toString());
		}
		return builder.build();
	}

	private Object valueToObject(Value value) {
		switch (value.getKindCase()) {
		case NULL_VALUE:
			return null;
		case NUMBER_VALUE:
			return value.getNumberValue();
		case STRING_VALUE:
			return value.getStringValue();
		case BOOL_VALUE:
			return value.getBoolValue();
		case STRUCT_VALUE:
			Map<String, Object> map = new HashMap<>();
			value.getStructValue().getFieldsMap().forEach((k, v) -> map.put(k, valueToObject(v)));
			return map;
		case LIST_VALUE:
			List<Object> list = new ArrayList<>();
			value.getListValue().getValuesList().forEach(v -> list.add(valueToObject(v)));
			return list;
		default:
			return null;
		}
	}

	private void handleGrpcException(StatusRuntimeException e) {
		Status status = e.getStatus();
		switch (status.getCode()) {
		case NOT_FOUND:
			throw new RecordNotFoundException(status.getDescription(), null);
		case ALREADY_EXISTS:
			throw new DuplicatedKeyException("", "", null);
		case ABORTED:
			throw new ConcurrentModificationException(status.getDescription());
		case DEADLINE_EXCEEDED:
			throw new TimeoutException(status.getDescription());
		case PERMISSION_DENIED:
			throw new SecurityException(status.getDescription());
		case UNAVAILABLE:
			throw new NeedRetryException(status.getDescription());
		default:
			throw new RemoteException("gRPC error: " + status.getDescription(), e);
		}
	}

	private void handleGrpcException(StatusException e) {
		Status status = e.getStatus();
		switch (status.getCode()) {
		case NOT_FOUND:
			throw new RecordNotFoundException(status.getDescription(), null);
		case ALREADY_EXISTS:
			throw new DuplicatedKeyException("", "", null);
		case ABORTED:
			throw new ConcurrentModificationException(status.getDescription());
		case DEADLINE_EXCEEDED:
			throw new TimeoutException(status.getDescription());
		case PERMISSION_DENIED:
			throw new SecurityException(status.getDescription());
		case UNAVAILABLE:
			throw new NeedRetryException(status.getDescription());
		default:
			throw new RemoteException("gRPC error: " + status.getDescription(), e);
		}
	}

	/**
	 * Extended version with compression support
	 */
	public static class RemoteGrpcDatabaseWithCompression extends RemoteGrpcDatabase {

		public RemoteGrpcDatabaseWithCompression(String server, int grpcPort, int httpPort, String databaseName, String userName,
				String userPassword, ContextConfiguration configuration) {
			super(server, grpcPort, httpPort, databaseName, userName, userPassword, configuration);
		}

		@Override
		protected ManagedChannel createChannel(String server, int port) {

			return ManagedChannelBuilder.forAddress(server, port).usePlaintext() // No TLS/SSL
					.compressorRegistry(CompressorRegistry.getDefaultInstance()).decompressorRegistry(DecompressorRegistry.getDefaultInstance())
					.maxInboundMessageSize(100 * 1024 * 1024) // 100MB max message size
					.keepAliveTime(30, TimeUnit.SECONDS) // Keep-alive configuration
					.keepAliveTimeout(10, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build();
		}
	}
}