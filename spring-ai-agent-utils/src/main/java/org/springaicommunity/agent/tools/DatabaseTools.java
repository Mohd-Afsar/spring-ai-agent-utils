/*
* Copyright 2025 - 2025 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* https://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.springaicommunity.agent.tools;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.util.Assert;

/**
 * Spring AI tools for exploring and querying a relational database.
 *
 * <p>Provides five tools that give an AI agent full read-only visibility into any
 * JDBC-compatible database (H2, MySQL, PostgreSQL, Oracle, SQL Server, etc.):
 *
 * <ul>
 *   <li>{@code DbInfo} — database product name, version, JDBC URL</li>
 *   <li>{@code DbListTables} — all user tables in the schema</li>
 *   <li>{@code DbDescribeTable} — columns, types, primary keys, foreign keys</li>
 *   <li>{@code DbQuery} — execute a SELECT query and receive markdown results</li>
 *   <li>{@code DbSample} — preview the first N rows of any table</li>
 * </ul>
 *
 * <p>All write operations (INSERT, UPDATE, DELETE, DDL) are blocked at two levels:
 * SQL parsing (keyword check) and JDBC {@code connection.setReadOnly(true)}.
 *
 * @author Spring AI Community
 */
public class DatabaseTools {

	private static final Logger logger = LoggerFactory.getLogger(DatabaseTools.class);

	// DML / DDL keywords that are never allowed
	private static final Pattern WRITE_KEYWORDS = Pattern.compile(
			"\\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|EXEC|EXECUTE|CALL|GRANT|REVOKE|MERGE|REPLACE|LOAD|COPY)\\b",
			Pattern.CASE_INSENSITIVE);

	private static final Pattern COMMENT_SINGLE = Pattern.compile("--[^\\n]*");

	private static final Pattern COMMENT_BLOCK = Pattern.compile("/\\*[\\s\\S]*?\\*/");

	private final DataSource dataSource;

	private final int maxRows;

	private final int maxCellLength;

	private DatabaseTools(DataSource dataSource, int maxRows, int maxCellLength) {
		this.dataSource = dataSource;
		this.maxRows = maxRows;
		this.maxCellLength = maxCellLength;
	}

	// @formatter:off
	@Tool(name = "DbInfo", description = """
		Returns general information about the connected database: product name, version,
		JDBC URL, driver name, default schema/catalog, and supported SQL features.
		Use this first to understand what kind of database you are working with.
		""")
	public String getDatabaseInfo() { // @formatter:on
		try (Connection conn = this.dataSource.getConnection()) {
			DatabaseMetaData meta = conn.getMetaData();
			return String.format("""
					## Database Info

					- **Product**: %s %s
					- **JDBC URL**: %s
					- **Driver**: %s %s
					- **Schema**: %s
					- **Catalog**: %s
					- **Read Only**: %s
					- **Max Connections**: %d
					""",
					meta.getDatabaseProductName(),
					meta.getDatabaseProductVersion(),
					meta.getURL(),
					meta.getDriverName(),
					meta.getDriverVersion(),
					conn.getSchema() != null ? conn.getSchema() : "(default)",
					conn.getCatalog() != null ? conn.getCatalog() : "(default)",
					meta.isReadOnly() ? "yes" : "no",
					meta.getMaxConnections());
		}
		catch (SQLException e) {
			return "Error getting database info: " + e.getMessage();
		}
	}

	// @formatter:off
	@Tool(name = "DbListTables", description = """
		Lists all user tables in the database (optionally filtered by schema).
		Returns table name, schema, and type for each table.
		Call this first to discover what data is available before querying.

		Parameters:
		- schemaPattern: optional schema/database name filter (e.g. 'public', 'mydb').
		  Leave empty to list tables in all schemas.
		""")
	public String listTables( // @formatter:on
			@ToolParam(description = "Schema/database name to filter by. Leave blank for all schemas.",
					required = false) String schemaPattern) {

		try (Connection conn = this.dataSource.getConnection()) {
			DatabaseMetaData meta = conn.getMetaData();
			String schema = (schemaPattern != null && !schemaPattern.isBlank()) ? schemaPattern : null;

			try (ResultSet rs = meta.getTables(null, schema, "%", new String[] { "TABLE" })) {
				StringBuilder sb = new StringBuilder("## Database Tables\n\n");
				int count = 0;
				while (rs.next()) {
					String tableName = rs.getString("TABLE_NAME");
					String tableSchema = rs.getString("TABLE_SCHEM");
					String remarks = rs.getString("REMARKS");
					sb.append(String.format("- **%s**", tableName));
					if (tableSchema != null && !tableSchema.isBlank()) {
						sb.append(String.format(" (schema: %s)", tableSchema));
					}
					if (remarks != null && !remarks.isBlank()) {
						sb.append(String.format(" — %s", remarks));
					}
					sb.append("\n");
					count++;
				}
				if (count == 0) {
					sb.append("_No tables found._\n");
				}
				else {
					sb.append(String.format("\n**Total: %d tables**\n", count));
				}
				return sb.toString();
			}
		}
		catch (SQLException e) {
			return "Error listing tables: " + e.getMessage();
		}
	}

	// @formatter:off
	@Tool(name = "DbDescribeTable", description = """
		Returns the full schema definition of a table: column names, data types,
		nullability, default values, primary key columns, and foreign key relationships.

		Use this to understand the structure of a table before writing a query.
		For example, to know which column to join on or what values are allowed.

		Parameters:
		- tableName: name of the table to describe (case-insensitive for most databases).
		  May include a schema prefix, e.g. 'public.orders' or just 'orders'.
		""")
	public String describeTable( // @formatter:on
			@ToolParam(description = "Name of the table to describe (e.g. 'orders' or 'public.orders')") String tableName) {

		try (Connection conn = this.dataSource.getConnection()) {
			DatabaseMetaData meta = conn.getMetaData();

			// Handle schema-qualified names like "schema.table"
			String schema = null;
			String table = tableName;
			if (tableName.contains(".")) {
				String[] parts = tableName.split("\\.", 2);
				schema = parts[0];
				table = parts[1];
			}

			// Collect primary keys
			List<String> primaryKeys = new ArrayList<>();
			try (ResultSet pkRs = meta.getPrimaryKeys(null, schema, table)) {
				while (pkRs.next()) {
					primaryKeys.add(pkRs.getString("COLUMN_NAME"));
				}
			}

			// Collect foreign keys: column -> referenced table.column
			Map<String, String> foreignKeys = new LinkedHashMap<>();
			try (ResultSet fkRs = meta.getImportedKeys(null, schema, table)) {
				while (fkRs.next()) {
					String fkCol = fkRs.getString("FKCOLUMN_NAME");
					String refTable = fkRs.getString("PKTABLE_NAME");
					String refCol = fkRs.getString("PKCOLUMN_NAME");
					foreignKeys.put(fkCol, refTable + "." + refCol);
				}
			}

			// Describe columns
			StringBuilder sb = new StringBuilder();
			sb.append(String.format("## Table: %s\n\n", tableName.toUpperCase()));
			sb.append("| Column | Type | Nullable | Default | Key |\n");
			sb.append("|--------|------|----------|---------|-----|\n");

			int columnCount = 0;
			try (ResultSet colRs = meta.getColumns(null, schema, table, "%")) {
				while (colRs.next()) {
					String colName = colRs.getString("COLUMN_NAME");
					String colType = colRs.getString("TYPE_NAME");
					int colSize = colRs.getInt("COLUMN_SIZE");
					String nullable = "YES".equals(colRs.getString("IS_NULLABLE")) ? "YES" : "NO";
					String defaultVal = colRs.getString("COLUMN_DEF");

					// Build key indicator
					List<String> keyParts = new ArrayList<>();
					if (primaryKeys.contains(colName)) {
						keyParts.add("PK");
					}
					if (foreignKeys.containsKey(colName)) {
						keyParts.add("FK→" + foreignKeys.get(colName));
					}
					String keyInfo = keyParts.isEmpty() ? "" : String.join(", ", keyParts);

					sb.append(String.format("| %s | %s(%d) | %s | %s | %s |\n",
							colName,
							colType,
							colSize,
							nullable,
							defaultVal != null ? defaultVal : "",
							keyInfo));
					columnCount++;
				}
			}

			if (columnCount == 0) {
				return "Table not found or has no columns: " + tableName;
			}

			// Primary key summary
			if (!primaryKeys.isEmpty()) {
				sb.append(String.format("\n**Primary Key**: %s\n", String.join(", ", primaryKeys)));
			}

			// Foreign key summary
			if (!foreignKeys.isEmpty()) {
				sb.append("\n**Foreign Keys**:\n");
				foreignKeys.forEach(
						(col, ref) -> sb.append(String.format("- %s → %s\n", col, ref)));
			}

			return sb.toString();
		}
		catch (SQLException e) {
			return "Error describing table '" + tableName + "': " + e.getMessage();
		}
	}

	// @formatter:off
	@Tool(name = "DbQuery", description = """
		Executes a read-only SQL SELECT query and returns the results as a markdown table.

		IMPORTANT: Only SELECT statements are allowed. Any attempt to run INSERT, UPDATE,
		DELETE, DROP, CREATE, ALTER, or other write operations will be rejected.

		Results are capped at the configured maxRows limit unless maxRows is 0 (unlimited).
		For very large tables you may still add LIMIT in SQL to bound response size.

		Tips for writing good queries:
		- Use DbListTables to discover table names first
		- Before the first DbQuery against a table in this turn, call DbDescribeTable on that table
		  (avoids guessed columns and repeated failed SELECTs)
		- Use DbSample only after describe if you need example values for filters
		- Use WHERE, GROUP BY, ORDER BY, and JOIN just like regular SQL
		- Use LIMIT to control result size

		Parameters:
		- sql: a valid SELECT SQL statement
		""")
	public String executeQuery( // @formatter:on
			@ToolParam(description = "A SELECT SQL query to execute") String sql) {

		try {
			validateSelectOnly(sql);
		}
		catch (IllegalArgumentException e) {
			return "Query rejected: " + e.getMessage();
		}

		try (Connection conn = this.dataSource.getConnection()) {
			conn.setReadOnly(true);
			try (Statement stmt = conn.createStatement()) {
				if (this.maxRows > 0) {
					stmt.setMaxRows(this.maxRows);
				}
				stmt.setQueryTimeout(30);

				// Always log the SQL so operators can trace report numbers to their data source.
				logger.info("[DbQuery] Executing SQL: {}", sql);

				try (ResultSet rs = stmt.executeQuery(sql)) {
					return formatAsMarkdownTable(rs);
				}
			}
		}
		catch (SQLException e) {
			return "SQL error: " + e.getMessage();
		}
	}

	// @formatter:off
	@Tool(name = "DbSample", description = """
		Returns a sample of rows from a table to help you understand its contents.
		Equivalent to running: SELECT * FROM <tableName> LIMIT <limit>

		Use after DbDescribeTable to see actual data values (e.g. status enums) before writing precise queries;
		do not use DbSample instead of DbDescribeTable for column discovery.

		Parameters:
		- tableName: name of the table to sample (use DbListTables to find table names)
		- limit: number of rows to return (default 5, max capped at maxRows setting)
		""")
	public String sampleTable( // @formatter:on
			@ToolParam(description = "Name of the table to sample") String tableName,
			@ToolParam(description = "Number of rows to return (default 5)", required = false) Integer limit) {

		String sanitized = sanitizeIdentifier(tableName);
		if (sanitized == null) {
			return "Invalid table name: " + tableName;
		}

		int requested = (limit != null && limit > 0) ? limit : 5;
		int effectiveLimit = this.maxRows > 0 ? Math.min(requested, this.maxRows) : requested;
		logger.info("[DbSample] Sampling table: {} limit: {} (effectiveLimit)", sanitized, effectiveLimit);
		return executeQuery("SELECT * FROM " + sanitized + " LIMIT " + effectiveLimit);
	}

	// @formatter:off
	@Tool(name = "DbQueryPaged", description = """
		Executes a SELECT query in pages (chunks) and returns one page at a time,
		with a progress header showing how many rows have been processed.

		Use this instead of DbQuery when the dataset is large and you need to process
		it in manageable chunks without hitting token limits.

		How it works:
		- Page 1 first counts total matching rows so progress can be shown
		- Each page returns 'pageSize' rows starting at 'pageNumber * pageSize'
		- Returns a progress header: "Page 2/5 | rows 26–50 of 124 (40% processed)"
		- Keep calling with incrementing pageNumber until hasMore = false

		Parameters:
		- sql:        a valid SELECT statement WITHOUT any LIMIT/OFFSET clauses
		- pageSize:   rows per page (default 25, max capped at maxRows setting)
		- pageNumber: zero-based page index (0 = first page, 1 = second, ...)

		Example workflow:
		  DbQueryPaged("SELECT * FROM alarms WHERE severity='CRITICAL'", 25, 0) -- first page
		  DbQueryPaged("SELECT * FROM alarms WHERE severity='CRITICAL'", 25, 1) -- second page
		""")
	public String executeQueryPaged( // @formatter:on
			@ToolParam(description = "A SELECT SQL query WITHOUT LIMIT or OFFSET — pagination is added automatically") String sql,
			@ToolParam(description = "Number of rows per page (default 25)", required = false) Integer pageSize,
			@ToolParam(description = "Zero-based page number (0 = first page)", required = false) Integer pageNumber) {

		try {
			validateSelectOnly(sql);
		}
		catch (IllegalArgumentException e) {
			return "Query rejected: " + e.getMessage();
		}

		int requestedSize = (pageSize != null && pageSize > 0) ? pageSize : 25;
		int size = this.maxRows > 0 ? Math.min(requestedSize, this.maxRows) : requestedSize;
		int page = (pageNumber != null && pageNumber >= 0) ? pageNumber : 0;
		int offset = page * size;

		try (Connection conn = this.dataSource.getConnection()) {
			conn.setReadOnly(true);

			// Step 1: count total rows for progress reporting
			long totalRows = -1;
			String countSql = "SELECT COUNT(*) FROM (" + sql + ") AS _count_subq";
			try (Statement countStmt = conn.createStatement();
					ResultSet countRs = countStmt.executeQuery(countSql)) {
				if (countRs.next()) {
					totalRows = countRs.getLong(1);
				}
			}
			catch (SQLException ignored) {
				// COUNT(*) wrap failed (e.g. complex CTE) — proceed without total
			}

			// Step 2: fetch the page using LIMIT/OFFSET
			String pagedSql = sql + " LIMIT " + size + " OFFSET " + offset;
			// Always log the paged query parameters and SQL for traceability.
			logger.info("[DbQueryPaged] Executing SQL (paged). pageSize={}, pageNumber={}, page={}, size={}, offset={}, sql={}",
					pageSize, pageNumber, page, size, offset, sql);

			try (Statement stmt = conn.createStatement()) {
				stmt.setQueryTimeout(30);
				try (ResultSet rs = stmt.executeQuery(pagedSql)) {
					String tableResult = formatAsMarkdownTable(rs);

					// Build progress header
					StringBuilder header = new StringBuilder();
					long rowStart = offset + 1;
					long rowEnd = offset + size; // approximate; exact from table row count below

					if (totalRows >= 0) {
						long totalPages = (long) Math.ceil((double) totalRows / size);
						long currentPage = page + 1;
						rowEnd = Math.min(offset + size, totalRows);
						int pct = (int) Math.round(100.0 * rowEnd / totalRows);
						boolean hasMore = rowEnd < totalRows;
						header.append(String.format(
								"**Progress: Page %d/%d | rows %d–%d of %d (%d%% processed) | hasMore: %s**%n%n",
								currentPage, totalPages, rowStart, rowEnd, totalRows, pct, hasMore));
					}
					else {
						header.append(String.format(
								"**Progress: Page %d | rows starting at %d (total unknown) | pageSize: %d**%n%n",
								page + 1, rowStart, size));
					}

					return header + tableResult;
				}
			}
		}
		catch (SQLException e) {
			return "SQL error: " + e.getMessage();
		}
	}

	// --- Helpers ---

	/**
	 * Validates that the SQL is a read-only SELECT. Strips comments first to prevent
	 * injection like: {@code SELECT 1; -- DROP TABLE users}.
	 */
	private void validateSelectOnly(String sql) {
		if (sql == null || sql.isBlank()) {
			throw new IllegalArgumentException("SQL must not be empty.");
		}

		// Strip comments
		String stripped = COMMENT_SINGLE.matcher(sql).replaceAll(" ");
		stripped = COMMENT_BLOCK.matcher(stripped).replaceAll(" ");
		stripped = stripped.strip();

		// Must start with SELECT or WITH (CTEs)
		String upper = stripped.toUpperCase();
		if (!upper.startsWith("SELECT") && !upper.startsWith("WITH")) {
			throw new IllegalArgumentException(
					"Only SELECT (or WITH … SELECT) statements are allowed. Got: " + stripped.substring(0, Math.min(50, stripped.length())));
		}

		// Must not contain any write keywords
		if (WRITE_KEYWORDS.matcher(stripped).find()) {
			throw new IllegalArgumentException(
					"Query contains a disallowed keyword (INSERT/UPDATE/DELETE/DROP/etc.). Only read-only queries are permitted.");
		}
	}

	/** Formats a ResultSet as a GitHub-flavored markdown table. */
	private String formatAsMarkdownTable(ResultSet rs) throws SQLException {
		ResultSetMetaData rsMeta = rs.getMetaData();
		int colCount = rsMeta.getColumnCount();

		// Collect column names
		List<String> headers = new ArrayList<>();
		for (int i = 1; i <= colCount; i++) {
			headers.add(rsMeta.getColumnLabel(i));
		}

		// Collect all rows (up to maxRows)
		List<List<String>> rows = new ArrayList<>();
		while (rs.next()) {
			List<String> row = new ArrayList<>();
			for (int i = 1; i <= colCount; i++) {
				String val = rs.getString(i);
				if (val == null) {
					val = "NULL";
				}
				else if (val.length() > this.maxCellLength) {
					val = val.substring(0, this.maxCellLength) + "…";
				}
				row.add(val.replace("|", "\\|").replace("\n", " ").replace("\r", ""));
			}
			rows.add(row);
		}

		if (rows.isEmpty()) {
			return "_Query returned no rows._\n";
		}

		// Build markdown table
		StringBuilder sb = new StringBuilder();
		sb.append("| ").append(String.join(" | ", headers)).append(" |\n");
		sb.append("|").append(" --- |".repeat(colCount)).append("\n");
		for (List<String> row : rows) {
			sb.append("| ").append(String.join(" | ", row)).append(" |\n");
		}
		sb.append(String.format("\n_(%d rows)_\n", rows.size()));

		return sb.toString();
	}

	/**
	 * Sanitizes a table name to prevent SQL injection in DbSample.
	 * Allows only alphanumeric, underscore, and dot (for schema.table).
	 * Returns null if the name contains disallowed characters.
	 */
	private String sanitizeIdentifier(String name) {
		if (name == null || name.isBlank()) {
			return null;
		}
		if (!name.matches("[\\w.]+")) {
			return null;
		}
		return name;
	}

	public static Builder builder(DataSource dataSource) {
		return new Builder(dataSource);
	}

	public static class Builder {

		private final DataSource dataSource;

		private int maxRows = 50;

		private int maxCellLength = 200;

		private Builder(DataSource dataSource) {
			Assert.notNull(dataSource, "dataSource must not be null");
			this.dataSource = dataSource;
		}

		/**
		 * Maximum rows returned by DbQuery / DbSample page caps. Use {@code 0} for no JDBC row limit (unlimited).
		 */
		public Builder maxRows(int maxRows) {
			Assert.isTrue(maxRows >= 0, "maxRows must be non-negative (0 = unlimited)");
			this.maxRows = maxRows;
			return this;
		}

		/** Maximum characters per cell value before truncation. Default: 200. */
		public Builder maxCellLength(int maxCellLength) {
			Assert.isTrue(maxCellLength > 0, "maxCellLength must be positive");
			this.maxCellLength = maxCellLength;
			return this;
		}

		public DatabaseTools build() {
			return new DatabaseTools(this.dataSource, this.maxRows, this.maxCellLength);
		}

	}

}
