package io.dbxmetadata.api;

import io.dbxmetadata.exception.MetadataExtractionException;
import io.dbxmetadata.model.DatabaseMetadata;
import io.dbxmetadata.model.ExportOptions;
import io.dbxmetadata.model.ExportResult;
import io.dbxmetadata.model.SchemaMetadata;

import java.util.List;
import java.util.Optional;

public interface DatabaseExplorer {

    DatabaseMetadata explore() throws MetadataExtractionException;
    Optional<SchemaMetadata> getSchema(String schemaName) throws MetadataExtractionException;
    List<String> listSchemas() throws MetadataExtractionException;
    ExportResult export(ExportOptions options) throws MetadataExtractionException;
    String getDatabaseProductName();
    String getDatabaseProductVersion();
}
