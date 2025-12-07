package io.dbxmetadata.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dbxmetadata.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class JsonExporter {
    private static final Logger log = LoggerFactory.getLogger(JsonExporter.class);

    private static final ObjectMapper MAPPER = createObjectMapper();

    private JsonExporter() {
        // Utility class
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        return mapper;
    }

    public static ExportResult export(DatabaseMetadata metadata, ExportOptions options) {
        try {
            ObjectMapper mapper = options.isPrettyPrint()
                    ? MAPPER
                    : MAPPER.copy().configure(SerializationFeature.INDENT_OUTPUT, false);

            // Filter metadata based on options
            DatabaseMetadata filteredMetadata = filterMetadata(metadata, options);

            if (options.getOutputPath() != null) {
                // Write to file
                Path outputPath = options.getOutputPath();
                if (outputPath.getParent() != null) {
                    Files.createDirectories(outputPath.getParent());
                }

                byte[] jsonBytes = mapper.writeValueAsBytes(filteredMetadata);
                Files.write(outputPath, jsonBytes);

                log.info("Exported metadata to {}", outputPath);
                return ExportResult.success(outputPath, jsonBytes.length);
            } else {
                // Return as string (for in-memory use)
                String json = mapper.writeValueAsString(filteredMetadata);
                log.debug("Generated JSON metadata ({} chars)", json.length());
                return ExportResult.builder()
                        .success(true)
                        .bytesWritten(json.length())
                        .build();
            }

        } catch (IOException e) {
            log.error("Failed to export metadata to JSON: {}", e.getMessage());
            return ExportResult.failure("JSON export failed: " + e.getMessage());
        }
    }

    public static String toJson(DatabaseMetadata metadata, boolean prettyPrint) {
        try {
            ObjectMapper mapper = prettyPrint
                    ? MAPPER
                    : MAPPER.copy().configure(SerializationFeature.INDENT_OUTPUT, false);
            return mapper.writeValueAsString(metadata);
        } catch (IOException e) {
            throw new RuntimeException("Failed to convert metadata to JSON", e);
        }
    }

    public static String toJson(DatabaseMetadata metadata) {
        return toJson(metadata, true);
    }

    public static ObjectMapper getObjectMapper() {
        return MAPPER;
    }

    private static DatabaseMetadata filterMetadata(DatabaseMetadata metadata, ExportOptions options) {
        // If all options are enabled, return as-is
        if (options.isIncludeProcedures() && options.isIncludeTriggers() &&
                options.isIncludeIndexDetails() && options.isIncludeComments() &&
                options.isIncludeViewDefinitions()) {
            return metadata;
        }

        // Build filtered metadata
        DatabaseMetadata.Builder builder = DatabaseMetadata.builder()
                .productName(metadata.getProductName())
                .productVersion(metadata.getProductVersion())
                .driverName(metadata.getDriverName())
                .driverVersion(metadata.getDriverVersion())
                .url(metadata.getUrl())
                .userName(metadata.getUserName())
                .extractedAt(metadata.getExtractedAt())
                .warnings(metadata.getWarnings());

        for (SchemaMetadata schema : metadata.getSchemas()) {
            builder.addSchema(filterSchema(schema, options));
        }

        return builder.build();
    }

    private static SchemaMetadata filterSchema(SchemaMetadata schema, ExportOptions options) {
        SchemaMetadata.Builder builder = SchemaMetadata.builder()
                .name(schema.getName())
                .catalog(schema.getCatalog())
                .owner(schema.getOwner());

        // Filter tables
        for (TableMetadata table : schema.getTables()) {
            builder.addTable(filterTable(table, options));
        }

        // Filter views
        for (ViewMetadata view : schema.getViews()) {
            builder.addView(filterView(view, options));
        }

        // Include procedures if enabled
        if (options.isIncludeProcedures()) {
            for (ProcedureMetadata proc : schema.getProcedures()) {
                builder.addProcedure(proc);
            }
        }

        return builder.build();
    }

    private static TableMetadata filterTable(TableMetadata table, ExportOptions options) {
        TableMetadata.Builder builder = TableMetadata.builder()
                .name(table.getName())
                .type(table.getType())
                .primaryKey(table.getPrimaryKey())
                .foreignKeys(table.getForeignKeys())
                .rowCount(table.getRowCount());

        // Filter columns
        for (ColumnMetadata col : table.getColumns()) {
            builder.addColumn(filterColumn(col, options));
        }

        // Include indexes if enabled
        if (options.isIncludeIndexDetails()) {
            builder.indexes(table.getIndexes());
        }

        // Include triggers if enabled
        if (options.isIncludeTriggers()) {
            builder.triggers(table.getTriggers());
        }

        // Include comment if enabled
        if (options.isIncludeComments()) {
            builder.comment(table.getComment());
        }

        return builder.build();
    }

    private static ColumnMetadata filterColumn(ColumnMetadata column, ExportOptions options) {
        ColumnMetadata.Builder builder = ColumnMetadata.builder()
                .name(column.getName())
                .dataType(column.getDataType())
                .size(column.getSize())
                .precision(column.getPrecision())
                .scale(column.getScale())
                .nullable(column.isNullable())
                .primaryKey(column.isPrimaryKey())
                .autoIncrement(column.isAutoIncrement())
                .defaultValue(column.getDefaultValue())
                .ordinalPosition(column.getOrdinalPosition());

        // Include comment if enabled
        if (options.isIncludeComments()) {
            builder.comment(column.getComment());
        }

        return builder.build();
    }

    private static ViewMetadata filterView(ViewMetadata view, ExportOptions options) {
        ViewMetadata.Builder builder = ViewMetadata.builder()
                .name(view.getName())
                .updatable(view.isUpdatable());

        // Filter columns
        for (ColumnMetadata col : view.getColumns()) {
            builder.addColumn(filterColumn(col, options));
        }

        // Include definition if enabled
        if (options.isIncludeViewDefinitions()) {
            builder.definition(view.getDefinition());
        }

        // Include comment if enabled
        if (options.isIncludeComments()) {
            builder.comment(view.getComment());
        }

        return builder.build();
    }
}
