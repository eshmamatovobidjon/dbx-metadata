package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ViewMetadata {

    private final String name;
    private final List<ColumnMetadata> columns;
    private final String definition;
    private final String comment;
    private final boolean updatable;

    private ViewMetadata(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "View name cannot be null");
        this.columns = List.copyOf(builder.columns);
        this.definition = builder.definition;
        this.comment = builder.comment;
        this.updatable = builder.updatable;
    }

    public String getName() {
        return name;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    public String getDefinition() {
        return definition;
    }

    public String getComment() {
        return comment;
    }

    public boolean isUpdatable() {
        return updatable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ViewMetadata that = (ViewMetadata) o;
        return updatable == that.updatable &&
                Objects.equals(name, that.name) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(definition, that.definition) &&
                Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columns, definition, comment, updatable);
    }

    @Override
    public String toString() {
        return "ViewMetadata{" +
                "name='" + name + '\'' +
                ", columns=" + columns.size() +
                ", updatable=" + updatable +
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
        private List<ColumnMetadata> columns = new ArrayList<>();
        private String definition;
        private String comment;
        private boolean updatable;

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder columns(List<ColumnMetadata> columns) {
            this.columns = new ArrayList<>(columns);
            return this;
        }

        public Builder addColumn(ColumnMetadata column) {
            this.columns.add(column);
            return this;
        }

        public Builder definition(String definition) {
            this.definition = definition;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder updatable(boolean updatable) {
            this.updatable = updatable;
            return this;
        }

        public ViewMetadata build() {
            return new ViewMetadata(this);
        }
    }
}
