package io.dbxmetadata.impl;

import io.dbxmetadata.api.DatabaseExplorer;
import io.dbxmetadata.exception.MetadataExtractionException;
import io.dbxmetadata.model.DatabaseMetadata;
import io.dbxmetadata.model.ExportOptions;
import io.dbxmetadata.model.ExportResult;
import io.dbxmetadata.model.SchemaMetadata;
import io.dbxmetadata.strategy.MetadataStrategy;
import io.dbxmetadata.util.JsonExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DefaultDatabaseExplorer implements DatabaseExplorer {

    private static final Logger log = LoggerFactory.getLogger(DefaultDatabaseExplorer.class);

    private final Connection connection;
    private final MetadataStrategy strategy;
    private final String productName;
    private final String productVersion;

    // Cached metadata (optional - populated after first explore() call)
    private volatile DatabaseMetadata cachedMetadata;

    public DefaultDatabaseExplorer(Connection connection, MetadataStrategy strategy,
                                   String productName, String productVersion) {
        this.connection = Objects.requireNonNull(connection, "Connection cannot be null");
        this.strategy = Objects.requireNonNull(strategy, "Strategy cannot be null");
        this.productName = productName;
        this.productVersion = productVersion;

        log.info("Created DatabaseExplorer for {} {} using {} strategy",
                productName, productVersion, strategy.getVendorName());
    }

    @Override
    public DatabaseMetadata explore() throws MetadataExtractionException {
        log.debug("Starting metadata exploration for {} {}", productName, productVersion);
        long startTime = System.currentTimeMillis();

        try {
            DatabaseMetadata metadata = strategy.explore(connection);
            cachedMetadata = metadata;

            long elapsed = System.currentTimeMillis() - startTime;
            log.info("Metadata exploration completed in {}ms - {} schemas, {} tables, {} views",
                    elapsed,
                    metadata.getSchemas().size(),
                    metadata.getTotalTableCount(),
                    metadata.getTotalViewCount());

            if (!metadata.getWarnings().isEmpty()) {
                log.warn("Exploration completed with {} warnings", metadata.getWarnings().size());
                for (String warning : metadata.getWarnings()) {
                    log.warn("  - {}", warning);
                }
            }

            return metadata;

        } catch (MetadataExtractionException e) {
            log.error("Metadata exploration failed: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error during metadata exploration", e);
            throw new MetadataExtractionException("Metadata exploration failed", e);
        }
    }

    @Override
    public Optional<SchemaMetadata> getSchema(String schemaName) throws MetadataExtractionException {
        Objects.requireNonNull(schemaName, "Schema name cannot be null");
        log.debug("Extracting metadata for schema: {}", schemaName);

        try {
            SchemaMetadata schema = strategy.extractSchema(connection, schemaName);
            return Optional.ofNullable(schema);
        } catch (MetadataExtractionException e) {
            if (e.isPermissionError()) {
                log.warn("Permission denied for schema: {}", schemaName);
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public List<String> listSchemas() throws MetadataExtractionException {
        log.debug("Listing schemas");
        return strategy.listSchemas(connection);
    }

    @Override
    public ExportResult export(ExportOptions options) throws MetadataExtractionException {
        Objects.requireNonNull(options, "Export options cannot be null");
        log.debug("Exporting metadata to {} format", options.getFormat());

        // Get metadata (use cached if available)
        DatabaseMetadata metadata = cachedMetadata;
        if (metadata == null) {
            metadata = explore();
        }

        try {
            return switch (options.getFormat()) {
                case JSON -> JsonExporter.export(metadata, options);
            };
        } catch (Exception e) {
            log.error("Export failed: {}", e.getMessage());
            return ExportResult.failure(e.getMessage());
        }
    }

    @Override
    public String getDatabaseProductName() {
        return productName;
    }

    @Override
    public String getDatabaseProductVersion() {
        return productVersion;
    }
}
