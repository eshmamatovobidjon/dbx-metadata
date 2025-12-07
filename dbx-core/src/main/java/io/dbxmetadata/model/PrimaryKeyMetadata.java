package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class PrimaryKeyMetadata {

    private final String name;
    private final List<String> columns;

    private PrimaryKeyMetadata(Builder builder) {
        this.name = builder.name;
        this.columns = List.copyOf(builder.columns);
    }

    public String getName() {
        return name;
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrimaryKeyMetadata that = (PrimaryKeyMetadata) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columns);
    }

    @Override
    public String toString() {
        return "PrimaryKeyMetadata{" +
                "name='" + name + '\'' +
                ", columns=" + columns +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String name;
        private List<String> columns = new ArrayList<>();

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder columns(List<String> columns) {
            this.columns = new ArrayList<>(columns);
            return this;
        }

        public Builder addColumn(String column) {
            this.columns.add(column);
            return this;
        }

        public PrimaryKeyMetadata build() {
            return new PrimaryKeyMetadata(this);
        }
    }
}
