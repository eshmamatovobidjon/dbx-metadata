package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatabaseMetadata {

    private final String productName;
    private final String productVersion;
    private final String driverName;
    private final String driverVersion;
    private final String url;
    private final String userName;
    private final List<SchemaMetadata> schemas;
    private final List<String> warnings;
    private final Instant extractedAt;

    private DatabaseMetadata(Builder builder) {
        this.productName = builder.productName;
        this.productVersion = builder.productVersion;
        this.driverName = builder.driverName;
        this.driverVersion = builder.driverVersion;
        this.url = builder.url;
        this.userName = builder.userName;
        this.schemas = List.copyOf(builder.schemas);
        this.warnings = List.copyOf(builder.warnings);
        this.extractedAt = builder.extractedAt != null ? builder.extractedAt : Instant.now();
    }

    public String getProductName() {
        return productName;
    }

    public String getProductVersion() {
        return productVersion;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getDriverVersion() {
        return driverVersion;
    }

    public String getUrl() {
        return url;
    }

    public String getUserName() {
        return userName;
    }

    public List<SchemaMetadata> getSchemas() {
        return schemas;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public Instant getExtractedAt() {
        return extractedAt;
    }

    public Optional<SchemaMetadata> findSchema(String schemaName) {
        return schemas.stream()
                .filter(s -> Objects.equals(s.getName(), schemaName))
                .findFirst();
    }

    public int getTotalTableCount() {
        return schemas.stream()
                .mapToInt(s -> s.getTables().size())
                .sum();
    }

    public int getTotalViewCount() {
        return schemas.stream()
                .mapToInt(s -> s.getViews().size())
                .sum();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseMetadata that = (DatabaseMetadata) o;
        return Objects.equals(productName, that.productName) &&
                Objects.equals(productVersion, that.productVersion) &&
                Objects.equals(driverName, that.driverName) &&
                Objects.equals(driverVersion, that.driverVersion) &&
                Objects.equals(url, that.url) &&
                Objects.equals(userName, that.userName) &&
                Objects.equals(schemas, that.schemas) &&
                Objects.equals(warnings, that.warnings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productName, productVersion, driverName, driverVersion,
                            url, userName, schemas, warnings);
    }

    @Override
    public String toString() {
        return "DatabaseMetadata{" +
                "productName='" + productName + '\'' +
                ", productVersion='" + productVersion + '\'' +
                ", schemas=" + schemas +
                ", tables=" + getTotalTableCount() +
                ", views=" + getTotalViewCount() +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String productName;
        private String productVersion;
        private String driverName;
        private String driverVersion;
        private String url;
        private String userName;
        private List<SchemaMetadata> schemas = new ArrayList<>();
        private List<String> warnings = new ArrayList<>();
        private Instant extractedAt;

        private Builder() {}

        public Builder productName(String productName) {
            this.productName = productName;
            return this;
        }

        public Builder productVersion(String productVersion) {
            this.productVersion = productVersion;
            return this;
        }

        public Builder driverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public Builder driverVersion(String driverVersion) {
            this.driverVersion = driverVersion;
            return this;
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public Builder schemas(List<SchemaMetadata> schemas) {
            this.schemas = new ArrayList<>(schemas);
            return this;
        }

        public Builder addSchema(SchemaMetadata schema) {
            this.schemas.add(schema);
            return this;
        }

        public Builder warnings(List<String> warnings) {
            this.warnings = new ArrayList<>(warnings);
            return this;
        }

        public Builder addWarning(String warning) {
            this.warnings.add(warning);
            return this;
        }

        public Builder extractedAt(Instant extractedAt) {
            this.extractedAt = extractedAt;
            return this;
        }

        public DatabaseMetadata build() {
            return new DatabaseMetadata(this);
        }
    }
}
