package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ColumnMetadata {

    private final String name;
    private final String dataType;
    private final int size;
    private final int precision;
    private final int scale;
    private final boolean nullable;
    private final boolean primaryKey;
    private final boolean autoIncrement;
    private final String defaultValue;
    private final String comment;
    private final int ordinalPosition;

    private ColumnMetadata(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "Column name cannot be null");
        this.dataType = builder.dataType;
        this.size = builder.size;
        this.precision = builder.precision;
        this.scale = builder.scale;
        this.nullable = builder.nullable;
        this.primaryKey = builder.primaryKey;
        this.autoIncrement = builder.autoIncrement;
        this.defaultValue = builder.defaultValue;
        this.comment = builder.comment;
        this.ordinalPosition = builder.ordinalPosition;
    }

    public String getName() {
        return name;
    }

    public String getDataType() {
        return dataType;
    }

    public int getSize() {
        return size;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getComment() {
        return comment;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnMetadata that = (ColumnMetadata) o;
        return size == that.size &&
                precision == that.precision &&
                scale == that.scale &&
                nullable == that.nullable &&
                primaryKey == that.primaryKey &&
                autoIncrement == that.autoIncrement &&
                ordinalPosition == that.ordinalPosition &&
                Objects.equals(name, that.name) &&
                Objects.equals(dataType, that.dataType) &&
                Objects.equals(defaultValue, that.defaultValue) &&
                Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, size, precision, scale, nullable,
                primaryKey, autoIncrement, defaultValue, comment, ordinalPosition);
    }

    @Override
    public String toString() {
        return "ColumnMetadata{" +
                "name='" + name + '\'' +
                ", dataType='" + dataType + '\'' +
                ", size=" + size +
                ", nullable=" + nullable +
                ", primaryKey=" + primaryKey +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(String name) {
        return new Builder().name(name);
    }

    public static final class Builder {
        private String name;
        private String dataType;
        private int size;
        private int precision;
        private int scale;
        private boolean nullable = true;
        private boolean primaryKey;
        private boolean autoIncrement;
        private String defaultValue;
        private String comment;
        private int ordinalPosition;

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder dataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder size(int size) {
            this.size = size;
            return this;
        }

        public Builder precision(int precision) {
            this.precision = precision;
            return this;
        }

        public Builder scale(int scale) {
            this.scale = scale;
            return this;
        }

        public Builder nullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public Builder primaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder autoIncrement(boolean autoIncrement) {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder ordinalPosition(int ordinalPosition) {
            this.ordinalPosition = ordinalPosition;
            return this;
        }

        public ColumnMetadata build() {
            return new ColumnMetadata(this);
        }
    }
}
