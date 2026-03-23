package org.springaicommunity.nova.pm.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

/**
 * Cassandra configuration for the PM Data Retrieval module.
 *
 * <p>All connection properties (host, port, keyspace, credentials, datacenter)
 * are managed by Spring Boot auto-configuration via {@code spring.cassandra.*}
 * entries in {@code application.properties}. No manual driver bean wiring needed.
 *
 * <p>This class exists to:
 * <ul>
 *   <li>Scope repository scanning to the PM package</li>
 *   <li>Serve as an extension point for future customization (codecs, pool tuning)</li>
 * </ul>
 *
 * <p>Schema management is NONE — PM tables are provisioned by the upstream
 * computation pipeline, not by this retrieval module.
 */
@Configuration
@EnableCassandraRepositories(basePackages = "org.springaicommunity.nova.pm.repository")
public class CassandraConfig {

}
