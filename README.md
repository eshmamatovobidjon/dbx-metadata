# DBX Metadata

[![](https://jitpack.io/v/eshmamatovobidjon/dbx-metadata.svg)](https://jitpack.io/#eshmamatovobidjon/dbx-metadata)

A Java library for extracting unified database metadata from PostgreSQL, MySQL, and SQL Server databases.

## Features

- **Unified API** - Single interface for multiple database vendors
- **Comprehensive Metadata** - Tables, columns, primary keys, foreign keys, indexes, views, triggers, stored procedures
- **Vendor-Specific Enhancements** - Uses native system catalogs for comments and definitions
- **Spring Boot Integration** - Auto-configuration included
- **JSON Export** - Export metadata to JSON format
- **Graceful Error Handling** - Returns partial results on permission issues

## Installation

Add JitPack repository and dependency to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependencies>
    <!-- Core library -->
    <dependency>
        <groupId>com.github.eshmamatovobidjon.dbx-metadata</groupId>
        <artifactId>dbx-core</artifactId>
        <version>v1.0.2</version>
    </dependency>
    
    <!-- Spring Boot Starter (optional) -->
    <dependency>
        <groupId>com.github.eshmamatovobidjon.dbx-metadata</groupId>
        <artifactId>dbx-spring-boot-starter</artifactId>
        <version>v1.0.2</version>
    </dependency>
</dependencies>
```

**Don't forget to add your database driver:**

```xml
<!-- PostgreSQL -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>{postgresql.version}</version>
</dependency>

<!-- MySQL -->
<dependency>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
    <version>{mysql.version}</version>
</dependency>

<!-- SQL Server -->
<dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc</artifactId>
    <version>{mssql.version}</version>
</dependency>
```

### Gradle

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.eshmamatovobidjon:dbx-core:v1.0.0'
}
```

## Quick Start

```java
import io.dbxmetadata.api.DatabaseExplorer;
import io.dbxmetadata.api.DatabaseExplorerFactory;
import io.dbxmetadata.model.*;

import java.sql.DriverManager;

public class Example {
    public static void main(String[] args) throws Exception {
        try (var conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/mydb", 
                "user", 
                "password")) {
            
            // Create explorer - auto-detects database type
            DatabaseExplorer explorer = DatabaseExplorerFactory.create(conn);
            
            // Extract all metadata
            DatabaseMetadata metadata = explorer.explore();
            
            // Print info
            System.out.println("Database: " + metadata.getProductName());
            System.out.println("Version: " + metadata.getProductVersion());
            
            for (SchemaMetadata schema : metadata.getSchemas()) {
                System.out.println("\nSchema: " + schema.getName());
                
                for (TableMetadata table : schema.getTables()) {
                    System.out.println("  Table: " + table.getName());
                    
                    for (ColumnMetadata column : table.getColumns()) {
                        System.out.printf("    - %s %s%s%n",
                            column.getName(),
                            column.getDataType(),
                            column.isPrimaryKey() ? " (PK)" : "");
                    }
                }
            }
        }
    }
}
```

## Export to JSON

```java
DatabaseExplorer explorer = DatabaseExplorerFactory.create(conn);

ExportOptions options = new ExportOptions(
    ExportOptions.ExportFormat.JSON,
    Paths.get("schema.json")
);

ExportResult result = explorer.export(options);
System.out.println("Exported: " + result.isSuccess());
```

## Spring Boot Usage

Just add the starter dependency and configure your datasource:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/mydb
    username: user
    password: secret
```

Then inject and use:

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

## Supported Databases

| Database | Comments | Triggers | Procedures |
|----------|----------|----------|------------|
| PostgreSQL | ✅ | ✅ | ✅ |
| MySQL/MariaDB | ✅ | ✅ | ✅ |
| SQL Server | ✅ | ✅ | ✅ |
| Others (JDBC) | ⚠️ | ⚠️ | ⚠️ |

## Requirements

- Java 17+
- Maven or Gradle

## License

MIT License

## Author

[Obidjon Eshmamatov](https://github.com/eshmamatovobidjon)
