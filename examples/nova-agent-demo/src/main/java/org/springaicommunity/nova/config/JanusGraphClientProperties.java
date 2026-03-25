package org.springaicommunity.nova.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code janusgraph-*.properties} / {@code janusgraph.*} keys for the
 * <strong>Gremlin driver client</strong> (host, port, pool, serializer).
 *
 * <p>NOVA only opens a Gremlin WebSocket; it does not connect to Cassandra.
 * {@code janusgraph.storage.*} mirrors the server's Janus storage profile
 * (same meaning as {@code janusgraph-cql.properties} / {@code janusgraph.properties}
 * on the Janus host): bound here for a single ops-facing file, logged and
 * validated in {@link JanusGraphConfig} when the Gremlin client connects.
 *
 * <p>Gremlin connection fields are intended to be set in
 * {@code classpath:janusgraph-client.properties} (listed in {@code application.properties}
 * as {@code spring.config.import=optional:classpath:janusgraph-client.properties}).
 * Registered via {@code @EnableConfigurationProperties} on {@code NovaApplication} so values are available
 * even when {@link JanusGraphConfig} is off.
 * {@link JanusGraphConfig} validates Gremlin fields when {@code janusgraph.enabled=true}
 * (also set in that file).
 */
@ConfigurationProperties(prefix = "janusgraph")
public class JanusGraphClientProperties {

	private final Gremlin gremlin = new Gremlin();

	private final Storage storage = new Storage();

	public Gremlin getGremlin() {
		return gremlin;
	}

	public Storage getStorage() {
		return storage;
	}

	public static class Gremlin {

		private String host;

		private int port;

		private String traversalSource;

		private String serializer;

		private int connectionPoolSize;

		private int minConnectionPoolSize;

		private int connectionSetupTimeoutMs;

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public String getTraversalSource() {
			return traversalSource;
		}

		public void setTraversalSource(String traversalSource) {
			this.traversalSource = traversalSource;
		}

		public String getSerializer() {
			return serializer;
		}

		public void setSerializer(String serializer) {
			this.serializer = serializer;
		}

		public int getConnectionPoolSize() {
			return connectionPoolSize;
		}

		public void setConnectionPoolSize(int connectionPoolSize) {
			this.connectionPoolSize = connectionPoolSize;
		}

		public int getMinConnectionPoolSize() {
			return minConnectionPoolSize;
		}

		public void setMinConnectionPoolSize(int minConnectionPoolSize) {
			this.minConnectionPoolSize = minConnectionPoolSize;
		}

		public int getConnectionSetupTimeoutMs() {
			return connectionSetupTimeoutMs;
		}

		public void setConnectionSetupTimeoutMs(int connectionSetupTimeoutMs) {
			this.connectionSetupTimeoutMs = connectionSetupTimeoutMs;
		}
	}

	public static class Storage {

		/** e.g. {@code cql} — must match JanusGraph Server config. */
		private String backend;

		/** Cassandra hostname or seed list as configured on the Janus host. */
		private String hostname;

		/** CQL port ({@code 0} = unset / omitted in logs). */
		private int port;

		private final Cql cql = new Cql();

		public String getBackend() {
			return backend;
		}

		public void setBackend(String backend) {
			this.backend = backend;
		}

		public String getHostname() {
			return hostname;
		}

		public void setHostname(String hostname) {
			this.hostname = hostname;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public Cql getCql() {
			return cql;
		}

		public static class Cql {

			private String keyspace;

			private String readConsistencyLevel;

			private String writeConsistencyLevel;

			private String localDatacenter;

			public String getKeyspace() {
				return keyspace;
			}

			public void setKeyspace(String keyspace) {
				this.keyspace = keyspace;
			}

			public String getReadConsistencyLevel() {
				return readConsistencyLevel;
			}

			public void setReadConsistencyLevel(String readConsistencyLevel) {
				this.readConsistencyLevel = readConsistencyLevel;
			}

			public String getWriteConsistencyLevel() {
				return writeConsistencyLevel;
			}

			public void setWriteConsistencyLevel(String writeConsistencyLevel) {
				this.writeConsistencyLevel = writeConsistencyLevel;
			}

			public String getLocalDatacenter() {
				return localDatacenter;
			}

			public void setLocalDatacenter(String localDatacenter) {
				this.localDatacenter = localDatacenter;
			}
		}
	}
}
