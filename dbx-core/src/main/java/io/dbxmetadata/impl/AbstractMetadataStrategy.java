package io.dbxmetadata.impl;

import io.dbxmetadata.exception.MetadataExtractionException;
import io.dbxmetadata.model.*;
import io.dbxmetadata.strategy.MetadataStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public abstract class AbstractMetadataStrategy implements MetadataStrategy {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final List<String> warnings = new ArrayList<>();

    @Override
    public DatabaseMetadata explore(Connection connection) throws MetadataExtractionException {
        warnings.clear();

        try {
            DatabaseMetaData dbMeta = connection.getMetaData();

            DatabaseMetadata.Builder builder = DatabaseMetadata.builder()
                    .productName(dbMeta.getDatabaseProductName())
                    .productVersion(dbMeta.getDatabaseProductVersion())
                    .driverName(dbMeta.getDriverName())
                    .driverVersion(dbMeta.getDriverVersion())
                    .url(dbMeta.getURL())
                    .userName(dbMeta.getUserName());

            // Extract schemas
            List<String> schemaNames = listSchemas(connection);
            for (String schemaName : schemaNames) {
                try {
                    SchemaMetadata schema = extractSchema(connection, schemaName);
                    builder.addSchema(schema);
                } catch (MetadataExtractionException e) {
                    if (e.isPermissionError()) {
                        warnings.add("Permission denied for schema: " + schemaName);
                        log.warn("Skipping schema due to permission error: {}", schemaName);
                    } else {
                        throw e;
                    }
                }
            }

            builder.warnings(warnings);
            return builder.build();

        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to explore database", e);
        }
    }

    @Override
    public List<String> listSchemas(Connection connection) throws MetadataExtractionException {
        List<String> schemas = new ArrayList<>();

        try {
            DatabaseMetaData dbMeta = connection.getMetaData();

            try (ResultSet rs = dbMeta.getSchemas()) {
                while (rs.next()) {
                    String schemaName = rs.getString("TABLE_SCHEM");
                    if (shouldIncludeSchema(schemaName)) {
                        schemas.add(schemaName);
                    }
                }
            }

            // If no schemas found, try catalogs (for MySQL-like databases)
            if (schemas.isEmpty()) {
                try (ResultSet rs = dbMeta.getCatalogs()) {
                    while (rs.next()) {
                        String catalogName = rs.getString("TABLE_CAT");
                        if (shouldIncludeSchema(catalogName)) {
                            schemas.add(catalogName);
                        }
                    }
                }
            }

            Collections.sort(schemas);
            return schemas;

        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to list schemas", e);
        }
    }

    @Override
    public SchemaMetadata extractSchema(Connection connection, String schemaName) throws MetadataExtractionException {
        try {
            DatabaseMetaData dbMeta = connection.getMetaData();
            String catalog = getCatalogForSchema(connection, schemaName);
            String schema = getSchemaForQuery(schemaName);

            SchemaMetadata.Builder builder = SchemaMetadata.builder()
                    .name(schemaName)
                    .catalog(catalog);

            // Extract tables
            List<String[]> tableInfoList = new ArrayList<>();
            try (ResultSet rs = dbMeta.getTables(catalog, schema, null, new String[]{"TABLE"})) {
                while (rs.next()) {
                    tableInfoList.add(new String[]{
                            rs.getString("TABLE_CAT"),
                            rs.getString("TABLE_SCHEM"),
                            rs.getString("TABLE_NAME")
                    });
                }
            }

            for (String[] tableInfo : tableInfoList) {
                try {
                    TableMetadata table = extractTable(connection, tableInfo[0], tableInfo[1], tableInfo[2]);
                    builder.addTable(table);
                } catch (MetadataExtractionException e) {
                    warnings.add("Failed to extract table " + tableInfo[2] + ": " + e.getMessage());
                    log.warn("Skipping table due to error: {}", tableInfo[2], e);
                }
            }

            // Extract views
            List<String[]> viewInfoList = new ArrayList<>();
            try (ResultSet rs = dbMeta.getTables(catalog, schema, null, new String[]{"VIEW"})) {
                while (rs.next()) {
                    viewInfoList.add(new String[]{
                            rs.getString("TABLE_CAT"),
                            rs.getString("TABLE_SCHEM"),
                            rs.getString("TABLE_NAME")
                    });
                }
            }

            for (String[] viewInfo : viewInfoList) {
                try {
                    ViewMetadata view = extractView(connection, viewInfo[0], viewInfo[1], viewInfo[2]);
                    builder.addView(view);
                } catch (MetadataExtractionException e) {
                    warnings.add("Failed to extract view " + viewInfo[2] + ": " + e.getMessage());
                    log.warn("Skipping view due to error: {}", viewInfo[2], e);
                }
            }

            // Extract procedures
            try {
                List<ProcedureMetadata> procedures = extractProcedures(connection, catalog, schema);
                for (ProcedureMetadata proc : procedures) {
                    builder.addProcedure(proc);
                }
            } catch (MetadataExtractionException e) {
                warnings.add("Failed to extract procedures for schema " + schemaName + ": " + e.getMessage());
                log.warn("Skipping procedures due to error", e);
            }

            return builder.build();

        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to extract schema: " + schemaName, e);
        }
    }

    @Override
    public TableMetadata extractTable(Connection connection, String catalog, String schema, String tableName)
            throws MetadataExtractionException {
        try {
            TableMetadata.Builder builder = TableMetadata.builder()
                    .name(tableName)
                    .type(TableMetadata.TableType.TABLE);

            // Extract columns
            List<ColumnMetadata> columns = extractColumns(connection, catalog, schema, tableName);
            builder.columns(columns);

            // Extract primary key
            PrimaryKeyMetadata primaryKey = extractPrimaryKey(connection, catalog, schema, tableName);
            if (primaryKey != null) {
                builder.primaryKey(primaryKey);
                // Mark primary key columns
                Set<String> pkColumns = new HashSet<>(primaryKey.getColumns());
                List<ColumnMetadata> updatedColumns = new ArrayList<>();
                for (ColumnMetadata col : columns) {
                    if (pkColumns.contains(col.getName())) {
                        updatedColumns.add(ColumnMetadata.builder()
                                .name(col.getName())
                                .dataType(col.getDataType())
                                .size(col.getSize())
                                .precision(col.getPrecision())
                                .scale(col.getScale())
                                .nullable(col.isNullable())
                                .primaryKey(true)
                                .autoIncrement(col.isAutoIncrement())
                                .defaultValue(col.getDefaultValue())
                                .comment(col.getComment())
                                .ordinalPosition(col.getOrdinalPosition())
                                .build());
                    } else {
                        updatedColumns.add(col);
                    }
                }
                builder.columns(updatedColumns);
            }

            // Extract foreign keys
            List<ForeignKeyMetadata> foreignKeys = extractForeignKeys(connection, catalog, schema, tableName);
            builder.foreignKeys(foreignKeys);

            // Extract indexes
            List<IndexMetadata> indexes = extractIndexes(connection, catalog, schema, tableName);
            builder.indexes(indexes);

            // Extract triggers (vendor-specific)
            try {
                List<TriggerMetadata> triggers = extractTriggersForTable(connection, catalog, schema, tableName);
                builder.triggers(triggers);
            } catch (Exception e) {
                log.debug("Could not extract triggers for table {}: {}", tableName, e.getMessage());
            }

            // Extract table comment (vendor-specific)
            String comment = extractTableComment(connection, catalog, schema, tableName);
            builder.comment(comment);

            return builder.build();

        } catch (Exception e) {
            throw new MetadataExtractionException("Failed to extract table: " + tableName,
                    "extractTable", tableName, e);
        }
    }

    @Override
    public List<ColumnMetadata> extractColumns(Connection connection, String catalog, String schema, String tableName)
            throws MetadataExtractionException {
        List<ColumnMetadata> columns = new ArrayList<>();

        try {
            DatabaseMetaData dbMeta = connection.getMetaData();

            try (ResultSet rs = dbMeta.getColumns(catalog, schema, tableName, null)) {
                while (rs.next()) {
                    ColumnMetadata.Builder builder = ColumnMetadata.builder()
                            .name(rs.getString("COLUMN_NAME"))
                            .dataType(rs.getString("TYPE_NAME"))
                            .size(rs.getInt("COLUMN_SIZE"))
                            .precision(rs.getInt("COLUMN_SIZE"))
                            .scale(getIntOrDefault(rs, "DECIMAL_DIGITS", 0))
                            .nullable("YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")))
                            .ordinalPosition(rs.getInt("ORDINAL_POSITION"));

                    // Default value
                    String defaultValue = rs.getString("COLUMN_DEF");
                    if (defaultValue != null) {
                        builder.defaultValue(defaultValue);
                    }

                    // Auto-increment (if available)
                    try {
                        String autoIncrement = rs.getString("IS_AUTOINCREMENT");
                        builder.autoIncrement("YES".equalsIgnoreCase(autoIncrement));
                    } catch (SQLException ignored) {
                        // Column not available in this driver
                    }

                    // Comment (if available in JDBC result)
                    try {
                        String comment = rs.getString("REMARKS");
                        if (comment != null && !comment.isEmpty()) {
                            builder.comment(comment);
                        }
                    } catch (SQLException ignored) {
                        // Column not available
                    }

                    columns.add(builder.build());
                }
            }

            // Sort by ordinal position
            columns.sort(Comparator.comparingInt(ColumnMetadata::getOrdinalPosition));

            // Enhance with vendor-specific comments if needed
            enhanceColumnComments(connection, catalog, schema, tableName, columns);

            return columns;

        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to extract columns for table: " + tableName,
                    "extractColumns", tableName, e);
        }
    }

    protected PrimaryKeyMetadata extractPrimaryKey(Connection connection, String catalog, String schema, String tableName)
            throws MetadataExtractionException {
        try {
            DatabaseMetaData dbMeta = connection.getMetaData();
            Map<Integer, String> keyColumns = new TreeMap<>();
            String pkName = null;

            try (ResultSet rs = dbMeta.getPrimaryKeys(catalog, schema, tableName)) {
                while (rs.next()) {
                    pkName = rs.getString("PK_NAME");
                    int keySeq = rs.getInt("KEY_SEQ");
                    String columnName = rs.getString("COLUMN_NAME");
                    keyColumns.put(keySeq, columnName);
                }
            }

            if (keyColumns.isEmpty()) {
                return null;
            }

            return PrimaryKeyMetadata.builder()
                    .name(pkName)
                    .columns(new ArrayList<>(keyColumns.values()))
                    .build();

        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to extract primary key for table: " + tableName,
                    "extractPrimaryKey", tableName, e);
        }
    }

    @Override
    public List<ForeignKeyMetadata> extractForeignKeys(Connection connection, String catalog, String schema, String tableName)
            throws MetadataExtractionException {
        Map<String, ForeignKeyMetadata.Builder> fkBuilders = new LinkedHashMap<>();

        try {
            DatabaseMetaData dbMeta = connection.getMetaData();

            try (ResultSet rs = dbMeta.getImportedKeys(catalog, schema, tableName)) {
                while (rs.next()) {
                    String fkName = rs.getString("FK_NAME");
                    if (fkName == null) {
                        fkName = "FK_" + tableName + "_" + rs.getString("FKCOLUMN_NAME");
                    }

                    String finalFkName = fkName;
                    ForeignKeyMetadata.Builder builder = fkBuilders.computeIfAbsent(fkName,
                            k -> {
                                try {
                                    return ForeignKeyMetadata.builder()
                                            .name(finalFkName)
                                            .referencedSchema(rs.getString("PKTABLE_SCHEM"))
                                            .referencedTable(rs.getString("PKTABLE_NAME"))
                                            .onUpdate(mapForeignKeyAction(rs.getShort("UPDATE_RULE")))
                                            .onDelete(mapForeignKeyAction(rs.getShort("DELETE_RULE")));
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            });

                    builder.addColumn(rs.getString("FKCOLUMN_NAME"));
                    builder.addReferencedColumn(rs.getString("PKCOLUMN_NAME"));
                }
            }

            List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();
            for (ForeignKeyMetadata.Builder builder : fkBuilders.values()) {
                foreignKeys.add(builder.build());
            }

            return foreignKeys;

        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to extract foreign keys for table: " + tableName,
                    "extractForeignKeys", tableName, e);
        }
    }

    @Override
    public List<IndexMetadata> extractIndexes(Connection connection, String catalog, String schema, String tableName)
            throws MetadataExtractionException {
        Map<String, IndexMetadata.Builder> indexBuilders = new LinkedHashMap<>();

        try {
            DatabaseMetaData dbMeta = connection.getMetaData();

            try (ResultSet rs = dbMeta.getIndexInfo(catalog, schema, tableName, false, true)) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    if (indexName == null) {
                        continue; // Skip table statistics rows
                    }

                    IndexMetadata.Builder builder = indexBuilders.computeIfAbsent(indexName,
                            k -> {
                                try {
                                    return IndexMetadata.builder()
                                            .name(indexName)
                                            .unique(!rs.getBoolean("NON_UNIQUE"))
                                            .type(mapIndexType(rs.getShort("TYPE")));
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            });

                    String columnName = rs.getString("COLUMN_NAME");
                    if (columnName != null) {
                        String ascDesc = rs.getString("ASC_OR_DESC");
                        IndexMetadata.SortOrder sortOrder = "D".equalsIgnoreCase(ascDesc)
                                ? IndexMetadata.SortOrder.DESC
                                : IndexMetadata.SortOrder.ASC;
                        int position = rs.getInt("ORDINAL_POSITION");
                        builder.addColumn(columnName, sortOrder, position);
                    }
                }
            }

            List<IndexMetadata> indexes = new ArrayList<>();
            for (IndexMetadata.Builder builder : indexBuilders.values()) {
                indexes.add(builder.build());
            }

            return indexes;

        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to extract indexes for table: " + tableName,
                    "extractIndexes", tableName, e);
        }
    }

    @Override
    public List<TriggerMetadata> extractTriggers(Connection connection, String catalog, String schema)
            throws MetadataExtractionException {
        // Default implementation returns empty list; vendor-specific implementations override
        return new ArrayList<>();
    }

    protected List<TriggerMetadata> extractTriggersForTable(Connection connection, String catalog,
                                                            String schema, String tableName) throws MetadataExtractionException {
        return new ArrayList<>();
    }

    @Override
    public List<ProcedureMetadata> extractProcedures(Connection connection, String catalog, String schema)
            throws MetadataExtractionException {
        List<ProcedureMetadata> procedures = new ArrayList<>();

        try {
            DatabaseMetaData dbMeta = connection.getMetaData();

            try (ResultSet rs = dbMeta.getProcedures(catalog, schema, null)) {
                while (rs.next()) {
                    String procName = rs.getString("PROCEDURE_NAME");
                    short procType = rs.getShort("PROCEDURE_TYPE");

                    ProcedureMetadata.Builder builder = ProcedureMetadata.builder()
                            .name(procName)
                            .type(procType == DatabaseMetaData.procedureReturnsResult
                                    ? ProcedureMetadata.ProcedureType.FUNCTION
                                    : ProcedureMetadata.ProcedureType.PROCEDURE);

                    try {
                        String remarks = rs.getString("REMARKS");
                        if (remarks != null) {
                            builder.comment(remarks);
                        }
                    } catch (SQLException ignored) {}

                    // Extract parameters
                    try {
                        extractProcedureParameters(connection, catalog, schema, procName, builder);
                    } catch (Exception e) {
                        log.debug("Could not extract parameters for procedure {}: {}", procName, e.getMessage());
                    }

                    procedures.add(builder.build());
                }
            }

            procedures.sort(Comparator.comparing(ProcedureMetadata::getName));
            return procedures;

        } catch (SQLException e) {
            throw new MetadataExtractionException("Failed to extract procedures",
                    "extractProcedures", schema, e);
        }
    }

    protected void extractProcedureParameters(Connection connection, String catalog, String schema,
                                              String procedureName, ProcedureMetadata.Builder builder) throws SQLException {
        DatabaseMetaData dbMeta = connection.getMetaData();

        try (ResultSet rs = dbMeta.getProcedureColumns(catalog, schema, procedureName, null)) {
            while (rs.next()) {
                String paramName = rs.getString("COLUMN_NAME");
                String dataType = rs.getString("TYPE_NAME");
                short columnType = rs.getShort("COLUMN_TYPE");
                int position = rs.getInt("ORDINAL_POSITION");

                ProcedureMetadata.ParameterMode mode = switch (columnType) {
                    case DatabaseMetaData.procedureColumnIn -> ProcedureMetadata.ParameterMode.IN;
                    case DatabaseMetaData.procedureColumnOut -> ProcedureMetadata.ParameterMode.OUT;
                    case DatabaseMetaData.procedureColumnInOut -> ProcedureMetadata.ParameterMode.INOUT;
                    case DatabaseMetaData.procedureColumnReturn -> ProcedureMetadata.ParameterMode.RETURN;
                    default -> ProcedureMetadata.ParameterMode.IN;
                };

                builder.addParameter(paramName, dataType, mode, position);
            }
        }
    }

    @Override
    public ViewMetadata extractView(Connection connection, String catalog, String schema, String viewName)
            throws MetadataExtractionException {
        try {
            ViewMetadata.Builder builder = ViewMetadata.builder().name(viewName);

            // Extract columns (same as table)
            List<ColumnMetadata> columns = extractColumns(connection, catalog, schema, viewName);
            builder.columns(columns);

            // Extract view definition (vendor-specific)
            String definition = extractViewDefinition(connection, catalog, schema, viewName);
            builder.definition(definition);

            return builder.build();

        } catch (Exception e) {
            throw new MetadataExtractionException("Failed to extract view: " + viewName,
                    "extractView", viewName, e);
        }
    }

    protected boolean shouldIncludeSchema(String schemaName) {
        return schemaName != null && !schemaName.isEmpty();
    }


    // Override for databases that use catalogs differently.
    protected String getCatalogForSchema(Connection connection, String schemaName) throws SQLException {
        return null;
    }

    protected String getSchemaForQuery(String schemaName) {
        return schemaName;
    }

    protected String extractTableComment(Connection connection, String catalog, String schema, String tableName) {
        return null;
    }

    // Override with vendor-specific implementation.
    protected void enhanceColumnComments(Connection connection, String catalog, String schema,
                                         String tableName, List<ColumnMetadata> columns) {
        // Default: no enhancement
    }

    protected String extractViewDefinition(Connection connection, String catalog, String schema, String viewName) {
        return null;
    }

    protected void addWarning(String warning) {
        warnings.add(warning);
    }

    protected int getIntOrDefault(ResultSet rs, String columnName, int defaultValue) {
        try {
            int value = rs.getInt(columnName);
            return rs.wasNull() ? defaultValue : value;
        } catch (SQLException e) {
            return defaultValue;
        }
    }

    protected ForeignKeyMetadata.ForeignKeyAction mapForeignKeyAction(short rule) {
        return switch (rule) {
            case DatabaseMetaData.importedKeyCascade -> ForeignKeyMetadata.ForeignKeyAction.CASCADE;
            case DatabaseMetaData.importedKeySetNull -> ForeignKeyMetadata.ForeignKeyAction.SET_NULL;
            case DatabaseMetaData.importedKeySetDefault -> ForeignKeyMetadata.ForeignKeyAction.SET_DEFAULT;
            case DatabaseMetaData.importedKeyRestrict -> ForeignKeyMetadata.ForeignKeyAction.RESTRICT;
            default -> ForeignKeyMetadata.ForeignKeyAction.NO_ACTION;
        };
    }

    protected IndexMetadata.IndexType mapIndexType(short type) {
        return switch (type) {
            case DatabaseMetaData.tableIndexHashed -> IndexMetadata.IndexType.HASH;
            case DatabaseMetaData.tableIndexClustered -> IndexMetadata.IndexType.CLUSTERED;
            default -> IndexMetadata.IndexType.BTREE;
        };
    }
}
