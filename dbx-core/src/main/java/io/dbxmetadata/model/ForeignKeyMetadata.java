package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ForeignKeyMetadata {

    private final String name;
    private final List<String> columns;
    private final String referencedSchema;
    private final String referencedTable;
    private final List<String> referencedColumns;
    private final ForeignKeyAction onUpdate;
    private final ForeignKeyAction onDelete;

    private ForeignKeyMetadata(Builder builder) {
        this.name = builder.name;
        this.columns = List.copyOf(builder.columns);
        this.referencedSchema = builder.referencedSchema;
        this.referencedTable = builder.referencedTable;
        this.referencedColumns = List.copyOf(builder.referencedColumns);
        this.onUpdate = builder.onUpdate;
        this.onDelete = builder.onDelete;
    }

    public String getName() {
        return name;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getReferencedSchema() {
        return referencedSchema;
    }

    public String getReferencedTable() {
        return referencedTable;
    }

    public List<String> getReferencedColumns() {
        return referencedColumns;
    }

    public ForeignKeyAction getOnUpdate() {
        return onUpdate;
    }

    public ForeignKeyAction getOnDelete() {
        return onDelete;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForeignKeyMetadata that = (ForeignKeyMetadata) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(referencedSchema, that.referencedSchema) &&
                Objects.equals(referencedTable, that.referencedTable) &&
                Objects.equals(referencedColumns, that.referencedColumns) &&
                onUpdate == that.onUpdate &&
                onDelete == that.onDelete;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columns, referencedSchema, referencedTable,
                referencedColumns, onUpdate, onDelete);
    }

    @Override
    public String toString() {
        return "ForeignKeyMetadata{" +
                "name='" + name + '\'' +
                ", columns=" + columns +
                ", referencedTable='" + referencedTable + '\'' +
                ", referencedColumns=" + referencedColumns +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    // Represents foreign key referential actions.
    public enum ForeignKeyAction {
        CASCADE,
        SET_NULL,
        SET_DEFAULT,
        RESTRICT,
        NO_ACTION
    }

    public static final class Builder {
        private String name;
        private List<String> columns = new ArrayList<>();
        private String referencedSchema;
        private String referencedTable;
        private List<String> referencedColumns = new ArrayList<>();
        private ForeignKeyAction onUpdate = ForeignKeyAction.NO_ACTION;
        private ForeignKeyAction onDelete = ForeignKeyAction.NO_ACTION;

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

        public Builder referencedSchema(String referencedSchema) {
            this.referencedSchema = referencedSchema;
            return this;
        }

        public Builder referencedTable(String referencedTable) {
            this.referencedTable = referencedTable;
            return this;
        }

        public Builder referencedColumns(List<String> referencedColumns) {
            this.referencedColumns = new ArrayList<>(referencedColumns);
            return this;
        }

        public Builder addReferencedColumn(String column) {
            this.referencedColumns.add(column);
            return this;
        }

        public Builder onUpdate(ForeignKeyAction onUpdate) {
            this.onUpdate = onUpdate;
            return this;
        }

        public Builder onDelete(ForeignKeyAction onDelete) {
            this.onDelete = onDelete;
            return this;
        }

        public ForeignKeyMetadata build() {
            return new ForeignKeyMetadata(this);
        }
    }
}
