package io.dbxmetadata.strategy;

import io.dbxmetadata.exception.MetadataExtractionException;
import io.dbxmetadata.model.*;

import java.sql.Connection;
import java.util.List;

public interface MetadataStrategy {
    boolean supports(String databaseProductName);

    DatabaseMetadata explore(Connection connection) throws MetadataExtractionException;

    SchemaMetadata extractSchema(Connection connection, String schemaName) throws MetadataExtractionException;

    List<String> listSchemas(Connection connection) throws MetadataExtractionException;

    TableMetadata extractTable(Connection connection, String catalog, String schema, String tableName)
            throws MetadataExtractionException;

    List<ColumnMetadata> extractColumns(Connection connection, String catalog, String schema, String tableName)
            throws MetadataExtractionException;

    List<ForeignKeyMetadata> extractForeignKeys(Connection connection, String catalog, String schema, String tableName)
            throws MetadataExtractionException;

    List<IndexMetadata> extractIndexes(Connection connection, String catalog, String schema, String tableName)
            throws MetadataExtractionException;

    List<TriggerMetadata> extractTriggers(Connection connection, String catalog, String schema)
            throws MetadataExtractionException;

    List<ProcedureMetadata> extractProcedures(Connection connection, String catalog, String schema)
            throws MetadataExtractionException;

    ViewMetadata extractView(Connection connection, String catalog, String schema, String viewName)
            throws MetadataExtractionException;

    String getVendorName();
}
