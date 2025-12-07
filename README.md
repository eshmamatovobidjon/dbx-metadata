# DBX Metadata

A Java library for extracting unified database metadata from MSSQL, MySQL, and PostgreSQL databases.

## Features

- **Unified API**: Single interface for multiple database vendors
- **Comprehensive Metadata**: Tables, columns, primary keys, foreign keys, indexes, views, triggers, and stored procedures
- **Vendor-Specific Enhancements**: Uses native system catalogs for enhanced metadata (comments, definitions)
- **Spring Boot Integration**: Auto-configuration for seamless Spring Boot usage
- **JSON Export**: Export metadata to JSON format
- **Graceful Error Handling**: Returns partial results on permission issues instead of failing
- **Extensible**: Easy to add support for new database vendors

## Requirements

- Java 17+
- Maven 3.6+

## Installation

Add the following dependencies to your `pom.xml`:

```xml
<!-- Core library -->
<dependency>
    <groupId>io.dbxmetadata</groupId>
    <artifactId>dbx-core</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Spring Boot Starter (optional) -->
<dependency>
    <groupId>io.dbxmetadata</groupId>
    <artifactId>dbx-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Quick Start

### Basic Usage

```java
import com.example.dbxmetadata.api.DatabaseExplorer;
import com.example.dbxmetadata.api.DatabaseExplorerFactory;
import com.example.dbxmetadata.model.*;

try (Connection conn = dataSource.getConnection()) {
    // Create explorer - vendor is auto-detected
    DatabaseExplorer explorer = DatabaseExplorerFactory.create(conn);
    
    // Extract all metadata
    DatabaseMetadata metadata = explorer.explore();
    
    // Access schema information
    for (SchemaMetadata schema : metadata.getSchemas()) {
        System.out.println("Schema: " + schema.getName());
        
        for (TableMetadata table : schema.getTables()) {
            System.out.println("  Table: " + table.getName());
            
            for (ColumnMetadata column : table.getColumns()) {
                System.out.println("    Column: " + column.getName() + 
                                   " (" + column.getDataType() + ")");
            }
        }
    }
}
```

### Export to JSON

```java
DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);

ExportOptions options = new ExportOptions(
    ExportOptions.ExportFormat.JSON, 
    Paths.get("db-metadata.json")
);

ExportResult result = explorer.export(options);
if (result.isSuccess()) {
    System.out.println("Exported to: " + result.getOutputPath());
}
```

### Spring Boot Integration

Simply add the starter dependency and configure your DataSource:

```yaml
# application.yml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/mydb
    username: user
    password: secret

# Optional DBX configuration
dbx:
  metadata:
    enabled: true
    include-procedures: true
    include-triggers: true
```

Then inject the explorer:

```java
@Service
public class SchemaService {
    
    private final DatabaseExplorer explorer;
    
    public SchemaService(DatabaseExplorer explorer) {
        this.explorer = explorer;
    }
    
    public List<String> getTableNames() {
        return explorer.explore().getSchemas().stream()
            .flatMap(s -> s.getTables().stream())
            .map(TableMetadata::getName)
            .toList();
    }
}
```

## API Reference

### DatabaseExplorerFactory

Factory for creating `DatabaseExplorer` instances:

```java
// From Connection
DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection);

// From DataSource
DatabaseExplorer explorer = DatabaseExplorerFactory.create(dataSource);

// With custom strategy
DatabaseExplorer explorer = DatabaseExplorerFactory.create(connection, customStrategy);
```

### DatabaseExplorer

Main interface for metadata extraction:

| Method | Description |
|--------|-------------|
| `explore()` | Extract complete database metadata |
| `getSchema(name)` | Get metadata for a specific schema |
| `listSchemas()` | List all accessible schema names |
| `export(options)` | Export metadata to file |

### Model Classes

| Class | Description |
|-------|-------------|
| `DatabaseMetadata` | Root object with database info and schemas |
| `SchemaMetadata` | Schema with tables, views, and procedures |
| `TableMetadata` | Table with columns, keys, indexes, triggers |
| `ColumnMetadata` | Column properties (type, size, nullable, etc.) |
| `PrimaryKeyMetadata` | Primary key constraint |
| `ForeignKeyMetadata` | Foreign key with referenced table |
| `IndexMetadata` | Index with columns and properties |
| `ViewMetadata` | View with columns and definition |
| `ProcedureMetadata` | Stored procedure/function |
| `TriggerMetadata` | Trigger with timing and event |

## Supported Databases

| Database | Strategy | Comments | Triggers | Procedures |
|----------|----------|----------|----------|------------|
| PostgreSQL | `PostgresMetadataStrategy` | ✅ | ✅ | ✅ |
| MySQL/MariaDB | `MySqlMetadataStrategy` | ✅ | ✅ | ✅ |
| SQL Server | `MsSqlMetadataStrategy` | ✅ | ✅ | ✅ |
| Others | `GenericJdbcMetadataStrategy` | ⚠️ | ⚠️ | ⚠️ |

## Extending the Library

### Adding a New Database Provider

1. Create a new strategy class extending `AbstractMetadataStrategy`:

```java
public class OracleMetadataStrategy extends AbstractMetadataStrategy {
    
    @Override
    public boolean supports(String databaseProductName) {
        return databaseProductName != null && 
               databaseProductName.toLowerCase().contains("oracle");
    }
    
    @Override
    public String getVendorName() {
        return "Oracle";
    }
    
    @Override
    protected boolean shouldIncludeSchema(String schemaName) {
        // Filter system schemas
        return !schemaName.startsWith("SYS");
    }
    
    @Override
    protected String extractTableComment(Connection conn, String catalog, 
            String schema, String tableName) {
        // Oracle-specific query for table comments
        String sql = "SELECT comments FROM all_tab_comments " +
                     "WHERE owner = ? AND table_name = ?";
        // ... execute query
    }
    
    // Override other methods as needed
}
```

2. Register the strategy:

```java
DatabaseExplorerFactory.registerStrategy(new OracleMetadataStrategy());
```

## Configuration Properties

When using Spring Boot:

| Property | Default | Description |
|----------|---------|-------------|
| `dbx.metadata.enabled` | `true` | Enable/disable auto-configuration |
| `dbx.metadata.cache-enabled` | `false` | Cache metadata after first exploration |
| `dbx.metadata.include-procedures` | `true` | Include stored procedures |
| `dbx.metadata.include-triggers` | `true` | Include triggers |
| `dbx.metadata.include-view-definitions` | `true` | Include view SQL definitions |

## Building from Source

```bash
git clone https://github.com/example/dbx-metadata.git
cd dbx-metadata
mvn clean install
```

Run tests:
```bash
mvn test
```

## License

This project is licensed under the Apache-2.0 license.
