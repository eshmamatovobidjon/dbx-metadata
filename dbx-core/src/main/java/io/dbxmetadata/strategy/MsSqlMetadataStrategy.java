package io.dbxmetadata.strategy;

import io.dbxmetadata.exception.MetadataExtractionException;
import io.dbxmetadata.impl.AbstractMetadataStrategy;
import io.dbxmetadata.model.ColumnMetadata;
import io.dbxmetadata.model.ProcedureMetadata;
import io.dbxmetadata.model.TriggerMetadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class MsSqlMetadataStrategy extends AbstractMetadataStrategy {

    private static final Set<String> SYSTEM_SCHEMAS = Set.of(
            "db_accessadmin", "db_backupoperator", "db_datareader", "db_datawriter",
            "db_ddladmin", "db_denydatareader", "db_denydatawriter", "db_owner",
            "db_securityadmin", "guest", "INFORMATION_SCHEMA", "sys"
    );

    @Override
    public boolean supports(String databaseProductName) {
        if (databaseProductName == null) {
            return false;
        }
        String lower = databaseProductName.toLowerCase();
        return lower.contains("microsoft sql server") || lower.contains("sql server");
    }

    @Override
    public String getVendorName() {
        return "MSSQL";
    }

    @Override
    protected boolean shouldIncludeSchema(String schemaName) {
        if (schemaName == null || schemaName.isEmpty()) {
            return false;
        }
        return !SYSTEM_SCHEMAS.contains(schemaName);
    }

    @Override
    protected String extractTableComment(Connection connection, String catalog, String schema, String tableName) {
        String sql = """
                SELECT CAST(ep.value AS NVARCHAR(MAX)) AS comment
                FROM sys.extended_properties ep
                JOIN sys.tables t ON ep.major_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE ep.minor_id = 0 
                  AND ep.name = 'MS_Description'
                  AND s.name = ? 
                  AND t.name = ?
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("comment");
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract table comment for {}.{}: {}", schema, tableName, e.getMessage());
        }

        return null;
    }

    @Override
    protected void enhanceColumnComments(Connection connection, String catalog, String schema,
                                         String tableName, List<ColumnMetadata> columns) {

        String sql = """
                SELECT c.name AS column_name, 
                       CAST(ep.value AS NVARCHAR(MAX)) AS comment
                FROM sys.extended_properties ep
                JOIN sys.columns c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id
                JOIN sys.tables t ON c.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE ep.name = 'MS_Description'
                  AND s.name = ? 
                  AND t.name = ?
                """;

        Map<String, String> comments = new HashMap<>();

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    comments.put(rs.getString("column_name"), rs.getString("comment"));
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract column comments for {}.{}: {}", schema, tableName, e.getMessage());
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
                SELECT OBJECT_DEFINITION(OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))) AS definition
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, viewName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("definition");
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract view definition for {}.{}: {}", schema, viewName, e.getMessage());
        }

        return null;
    }

    @Override
    public List<TriggerMetadata> extractTriggers(Connection connection, String catalog, String schema)
            throws MetadataExtractionException {
        List<TriggerMetadata> triggers = new ArrayList<>();

        String sql = """
                SELECT t.name AS trigger_name,
                       OBJECT_NAME(t.parent_id) AS table_name,
                       CASE 
                           WHEN t.is_instead_of_trigger = 1 THEN 'INSTEAD_OF'
                           ELSE 'AFTER'
                       END AS timing,
                       CASE 
                           WHEN te.type_desc = 'INSERT' THEN 'INSERT'
                           WHEN te.type_desc = 'UPDATE' THEN 'UPDATE'
                           WHEN te.type_desc = 'DELETE' THEN 'DELETE'
                           ELSE 'INSERT'
                       END AS event,
                       t.is_disabled,
                       OBJECT_DEFINITION(t.object_id) AS definition
                FROM sys.triggers t
                JOIN sys.trigger_events te ON t.object_id = te.object_id
                JOIN sys.tables tbl ON t.parent_id = tbl.object_id
                JOIN sys.schemas s ON tbl.schema_id = s.schema_id
                WHERE s.name = ?
                ORDER BY tbl.name, t.name
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    TriggerMetadata.TriggerTiming timing = switch (rs.getString("timing")) {
                        case "INSTEAD_OF" -> TriggerMetadata.TriggerTiming.INSTEAD_OF;
                        default -> TriggerMetadata.TriggerTiming.AFTER;
                    };

                    TriggerMetadata.TriggerEvent event = switch (rs.getString("event")) {
                        case "INSERT" -> TriggerMetadata.TriggerEvent.INSERT;
                        case "UPDATE" -> TriggerMetadata.TriggerEvent.UPDATE;
                        case "DELETE" -> TriggerMetadata.TriggerEvent.DELETE;
                        default -> TriggerMetadata.TriggerEvent.INSERT;
                    };

                    triggers.add(TriggerMetadata.builder()
                            .name(rs.getString("trigger_name"))
                            .tableName(rs.getString("table_name"))
                            .timing(timing)
                            .event(event)
                            .enabled(!rs.getBoolean("is_disabled"))
                            .definition(rs.getString("definition"))
                            .build());
                }
            }
        } catch (SQLException e) {
            log.warn("Could not extract triggers for schema {}: {}", schema, e.getMessage());
            addWarning("Failed to extract triggers: " + e.getMessage());
        }

        return triggers;
    }

    @Override
    protected List<TriggerMetadata> extractTriggersForTable(Connection connection, String catalog,
                                                            String schema, String tableName) throws MetadataExtractionException {
        List<TriggerMetadata> triggers = new ArrayList<>();

        String sql = """
                SELECT t.name AS trigger_name,
                       CASE 
                           WHEN t.is_instead_of_trigger = 1 THEN 'INSTEAD_OF'
                           ELSE 'AFTER'
                       END AS timing,
                       CASE 
                           WHEN te.type_desc = 'INSERT' THEN 'INSERT'
                           WHEN te.type_desc = 'UPDATE' THEN 'UPDATE'
                           WHEN te.type_desc = 'DELETE' THEN 'DELETE'
                           ELSE 'INSERT'
                       END AS event,
                       t.is_disabled,
                       OBJECT_DEFINITION(t.object_id) AS definition
                FROM sys.triggers t
                JOIN sys.trigger_events te ON t.object_id = te.object_id
                JOIN sys.tables tbl ON t.parent_id = tbl.object_id
                JOIN sys.schemas s ON tbl.schema_id = s.schema_id
                WHERE s.name = ? AND tbl.name = ?
                ORDER BY t.name
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    TriggerMetadata.TriggerTiming timing = switch (rs.getString("timing")) {
                        case "INSTEAD_OF" -> TriggerMetadata.TriggerTiming.INSTEAD_OF;
                        default -> TriggerMetadata.TriggerTiming.AFTER;
                    };

                    TriggerMetadata.TriggerEvent event = switch (rs.getString("event")) {
                        case "INSERT" -> TriggerMetadata.TriggerEvent.INSERT;
                        case "UPDATE" -> TriggerMetadata.TriggerEvent.UPDATE;
                        case "DELETE" -> TriggerMetadata.TriggerEvent.DELETE;
                        default -> TriggerMetadata.TriggerEvent.INSERT;
                    };

                    triggers.add(TriggerMetadata.builder()
                            .name(rs.getString("trigger_name"))
                            .tableName(tableName)
                            .timing(timing)
                            .event(event)
                            .enabled(!rs.getBoolean("is_disabled"))
                            .definition(rs.getString("definition"))
                            .build());
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract triggers for table {}.{}: {}", schema, tableName, e.getMessage());
        }

        return triggers;
    }

    @Override
    public List<ProcedureMetadata> extractProcedures(Connection connection, String catalog, String schema)
            throws MetadataExtractionException {
        List<ProcedureMetadata> procedures = new ArrayList<>();

        String sql = """
                SELECT o.name AS name,
                       CASE o.type
                           WHEN 'P' THEN 'PROCEDURE'
                           WHEN 'FN' THEN 'FUNCTION'
                           WHEN 'IF' THEN 'FUNCTION'
                           WHEN 'TF' THEN 'FUNCTION'
                           ELSE 'PROCEDURE'
                       END AS type,
                       OBJECT_DEFINITION(o.object_id) AS definition,
                       CAST(ep.value AS NVARCHAR(MAX)) AS comment
                FROM sys.objects o
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                LEFT JOIN sys.extended_properties ep ON ep.major_id = o.object_id 
                    AND ep.minor_id = 0 AND ep.name = 'MS_Description'
                WHERE o.type IN ('P', 'FN', 'IF', 'TF')
                  AND s.name = ?
                ORDER BY o.name
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ProcedureMetadata.ProcedureType type = "FUNCTION".equals(rs.getString("type"))
                            ? ProcedureMetadata.ProcedureType.FUNCTION
                            : ProcedureMetadata.ProcedureType.PROCEDURE;

                    ProcedureMetadata.Builder builder = ProcedureMetadata.builder()
                            .name(rs.getString("name"))
                            .type(type)
                            .definition(rs.getString("definition"))
                            .comment(rs.getString("comment"));

                    // Extract parameters
                    extractMsSqlParameters(connection, schema, rs.getString("name"), builder);

                    procedures.add(builder.build());
                }
            }
        } catch (SQLException e) {
            log.warn("Could not extract procedures for schema {}: {}", schema, e.getMessage());
            addWarning("Failed to extract procedures: " + e.getMessage());
        }

        return procedures;
    }

    private void extractMsSqlParameters(Connection connection, String schema, String routineName,
                                        ProcedureMetadata.Builder builder) {
        String sql = """
                SELECT p.name AS parameter_name,
                       TYPE_NAME(p.user_type_id) AS data_type,
                       CASE 
                           WHEN p.is_output = 1 AND p.name != '' THEN 'OUT'
                           WHEN p.is_output = 1 AND p.name = '' THEN 'RETURN'
                           ELSE 'IN'
                       END AS mode,
                       p.parameter_id AS position
                FROM sys.parameters p
                JOIN sys.objects o ON p.object_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE s.name = ? AND o.name = ?
                ORDER BY p.parameter_id
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, routineName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String paramName = rs.getString("parameter_name");
                    if (paramName == null || paramName.isEmpty()) {
                        continue;
                    }
                    // Remove @ prefix from SQL Server parameter names
                    if (paramName.startsWith("@")) {
                        paramName = paramName.substring(1);
                    }

                    ProcedureMetadata.ParameterMode mode = switch (rs.getString("mode")) {
                        case "OUT" -> ProcedureMetadata.ParameterMode.OUT;
                        case "RETURN" -> ProcedureMetadata.ParameterMode.RETURN;
                        default -> ProcedureMetadata.ParameterMode.IN;
                    };

                    builder.addParameter(paramName, rs.getString("data_type"),
                            mode, rs.getInt("position"));
                }
            }
        } catch (SQLException e) {
            log.debug("Could not extract parameters for routine {}: {}", routineName, e.getMessage());
        }
    }
}
