package io.dbxmetadata.strategy;

import io.dbxmetadata.exception.MetadataExtractionException;
import io.dbxmetadata.impl.AbstractMetadataStrategy;
import io.dbxmetadata.model.ColumnMetadata;
import io.dbxmetadata.model.ProcedureMetadata;
import io.dbxmetadata.model.TriggerMetadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MySqlMetadataStrategy extends AbstractMetadataStrategy {

    private static final Set<String> SYSTEM_SCHEMAS = Set.of(
            "information_schema", "mysql", "performance_schema", "sys"
    );

    @Override
    public boolean supports(String databaseProductName) {
        if (databaseProductName == null) {
            return false;
        }
        String lower = databaseProductName.toLowerCase();
        return lower.contains("mysql") || lower.contains("mariadb");
    }

    @Override
    public String getVendorName() {
        return "MySQL";
    }

    @Override
    protected boolean shouldIncludeSchema(String schemaName) {
        if (schemaName == null || schemaName.isEmpty()) {
            return false;
        }
        return !SYSTEM_SCHEMAS.contains(schemaName.toLowerCase());
    }

    @Override
    protected String getCatalogForSchema(Connection connection, String schemaName) throws SQLException {
        // MySQL uses catalogs as databases/schemas
        return schemaName;
    }

    @Override
    protected String getSchemaForQuery(String schemaName) {
        // MySQL uses null for schema in JDBC calls, catalog is the database
        return null;
    }

    @Override
    public List<String> listSchemas(Connection connection) throws MetadataExtractionException {
        List<String> schemas = new ArrayList<>();

        try {
            DatabaseMetaData dbMeta = connection.getMetaData();

            // MySQL uses catalogs as databases
            try (ResultSet rs = dbMeta.getCatalogs()) {
                while (rs.next()) {
                    String catalogName = rs.getString("TABLE_CAT");
                    if (shouldIncludeSchema(catalogName)) {
                        schemas.add(catalogName);
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
    protected String extractTableComment(Connection connection, String catalog, String schema, String tableName) {
        String sql = """
                SELECT TABLE_COMMENT 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, catalog);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String comment = rs.getString("TABLE_COMMENT");
                    return (comment != null && !comment.isEmpty()) ? comment : null;
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract table comment for {}.{}: {}", catalog, tableName, e.getMessage());
        }

        return null;
    }

    @Override
    protected void enhanceColumnComments(Connection connection, String catalog, String schema,
                                         String tableName, List<ColumnMetadata> columns) {

        String sql = """
                SELECT COLUMN_NAME, COLUMN_COMMENT 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_COMMENT != ''
                """;

        Map<String, String> comments = new HashMap<>();

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, catalog);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    comments.put(rs.getString("COLUMN_NAME"), rs.getString("COLUMN_COMMENT"));
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract column comments for {}.{}: {}", catalog, tableName, e.getMessage());
            return;
        }

        if (!comments.isEmpty()) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnMetadata col = columns.get(i);
                String comment = comments.get(col.getName());
                if (comment != null && (col.getComment() == null || col.getComment().isEmpty())) {
                    columns.set(i, ColumnMetadata.builder()
                            .name(col.getName())
                            .dataType(col.getDataType())
                            .size(col.getSize())
                            .precision(col.getPrecision())
                            .scale(col.getScale())
                            .nullable(col.isNullable())
                            .primaryKey(col.isPrimaryKey())
                            .autoIncrement(col.isAutoIncrement())
                            .defaultValue(col.getDefaultValue())
                            .comment(comment)
                            .ordinalPosition(col.getOrdinalPosition())
                            .build());
                }
            }
        }
    }

    @Override
    protected String extractViewDefinition(Connection connection, String catalog, String schema, String viewName) {
        String sql = """
                SELECT VIEW_DEFINITION 
                FROM information_schema.VIEWS 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, catalog);
            stmt.setString(2, viewName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("VIEW_DEFINITION");
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract view definition for {}.{}: {}", catalog, viewName, e.getMessage());
        }

        return null;
    }

    @Override
    public List<TriggerMetadata> extractTriggers(Connection connection, String catalog, String schema)
            throws MetadataExtractionException {
        List<TriggerMetadata> triggers = new ArrayList<>();

        String sql = """
                SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION,
                       ACTION_STATEMENT
                FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA = ?
                ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, catalog);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    TriggerMetadata.TriggerTiming timing = switch (rs.getString("ACTION_TIMING")) {
                        case "BEFORE" -> TriggerMetadata.TriggerTiming.BEFORE;
                        case "AFTER" -> TriggerMetadata.TriggerTiming.AFTER;
                        default -> TriggerMetadata.TriggerTiming.AFTER;
                    };

                    TriggerMetadata.TriggerEvent event = switch (rs.getString("EVENT_MANIPULATION")) {
                        case "INSERT" -> TriggerMetadata.TriggerEvent.INSERT;
                        case "UPDATE" -> TriggerMetadata.TriggerEvent.UPDATE;
                        case "DELETE" -> TriggerMetadata.TriggerEvent.DELETE;
                        default -> TriggerMetadata.TriggerEvent.INSERT;
                    };

                    triggers.add(TriggerMetadata.builder()
                            .name(rs.getString("TRIGGER_NAME"))
                            .tableName(rs.getString("EVENT_OBJECT_TABLE"))
                            .timing(timing)
                            .event(event)
                            .definition(rs.getString("ACTION_STATEMENT"))
                            .enabled(true)
                            .build());
                }
            }
        } catch (SQLException e) {
            log.warn("Could not extract triggers for schema {}: {}", catalog, e.getMessage());
            addWarning("Failed to extract triggers: " + e.getMessage());
        }

        return triggers;
    }

    @Override
    protected List<TriggerMetadata> extractTriggersForTable(Connection connection, String catalog,
                                                            String schema, String tableName) throws MetadataExtractionException {
        List<TriggerMetadata> triggers = new ArrayList<>();

        String sql = """
                SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
                FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?
                ORDER BY TRIGGER_NAME
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, catalog);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    TriggerMetadata.TriggerTiming timing = switch (rs.getString("ACTION_TIMING")) {
                        case "BEFORE" -> TriggerMetadata.TriggerTiming.BEFORE;
                        case "AFTER" -> TriggerMetadata.TriggerTiming.AFTER;
                        default -> TriggerMetadata.TriggerTiming.AFTER;
                    };

                    TriggerMetadata.TriggerEvent event = switch (rs.getString("EVENT_MANIPULATION")) {
                        case "INSERT" -> TriggerMetadata.TriggerEvent.INSERT;
                        case "UPDATE" -> TriggerMetadata.TriggerEvent.UPDATE;
                        case "DELETE" -> TriggerMetadata.TriggerEvent.DELETE;
                        default -> TriggerMetadata.TriggerEvent.INSERT;
                    };

                    triggers.add(TriggerMetadata.builder()
                            .name(rs.getString("TRIGGER_NAME"))
                            .tableName(tableName)
                            .timing(timing)
                            .event(event)
                            .definition(rs.getString("ACTION_STATEMENT"))
                            .enabled(true)
                            .build());
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract triggers for table {}.{}: {}", catalog, tableName, e.getMessage());
        }

        return triggers;
    }

    @Override
    public List<ProcedureMetadata> extractProcedures(Connection connection, String catalog, String schema)
            throws MetadataExtractionException {
        List<ProcedureMetadata> procedures = new ArrayList<>();

        String sql = """
                SELECT ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION, ROUTINE_COMMENT,
                       DATA_TYPE AS RETURN_TYPE
                FROM information_schema.ROUTINES
                WHERE ROUTINE_SCHEMA = ?
                ORDER BY ROUTINE_NAME
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, catalog);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ProcedureMetadata.ProcedureType type = "FUNCTION".equals(rs.getString("ROUTINE_TYPE"))
                            ? ProcedureMetadata.ProcedureType.FUNCTION
                            : ProcedureMetadata.ProcedureType.PROCEDURE;

                    ProcedureMetadata.Builder builder = ProcedureMetadata.builder()
                            .name(rs.getString("ROUTINE_NAME"))
                            .type(type)
                            .definition(rs.getString("ROUTINE_DEFINITION"))
                            .comment(rs.getString("ROUTINE_COMMENT"));

                    if (type == ProcedureMetadata.ProcedureType.FUNCTION) {
                        builder.returnType(rs.getString("RETURN_TYPE"));
                    }

                    // Extract parameters
                    extractMySqlParameters(connection, catalog, rs.getString("ROUTINE_NAME"), builder);

                    procedures.add(builder.build());
                }
            }
        } catch (SQLException e) {
            log.warn("Could not extract procedures for schema {}: {}", catalog, e.getMessage());
            addWarning("Failed to extract procedures: " + e.getMessage());
        }

        return procedures;
    }

    private void extractMySqlParameters(Connection connection, String catalog, String routineName,
                                        ProcedureMetadata.Builder builder) {
        String sql = """
                SELECT PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE, ORDINAL_POSITION
                FROM information_schema.PARAMETERS
                WHERE SPECIFIC_SCHEMA = ? AND SPECIFIC_NAME = ?
                ORDER BY ORDINAL_POSITION
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, catalog);
            stmt.setString(2, routineName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String paramName = rs.getString("PARAMETER_NAME");
                    if (paramName == null) {
                        continue; // Return value
                    }

                    ProcedureMetadata.ParameterMode mode = switch (rs.getString("PARAMETER_MODE")) {
                        case "IN" -> ProcedureMetadata.ParameterMode.IN;
                        case "OUT" -> ProcedureMetadata.ParameterMode.OUT;
                        case "INOUT" -> ProcedureMetadata.ParameterMode.INOUT;
                        default -> ProcedureMetadata.ParameterMode.IN;
                    };

                    builder.addParameter(paramName, rs.getString("DATA_TYPE"),
                            mode, rs.getInt("ORDINAL_POSITION"));
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract parameters for routine {}: {}", routineName, e.getMessage());
        }
    }
}
