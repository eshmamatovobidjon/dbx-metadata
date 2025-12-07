package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class TableMetadata {

    private final String name;
    private final TableType type;
    private final List<ColumnMetadata> columns;
    private final PrimaryKeyMetadata primaryKey;
    private final List<ForeignKeyMetadata> foreignKeys;
    private final List<IndexMetadata> indexes;
    private final List<TriggerMetadata> triggers;
    private final String comment;
    private final Long rowCount;

    private TableMetadata(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "Table name cannot be null");
        this.type = builder.type;
        this.columns = List.copyOf(builder.columns);
        this.primaryKey = builder.primaryKey;
        this.foreignKeys = List.copyOf(builder.foreignKeys);
        this.indexes = List.copyOf(builder.indexes);
        this.triggers = List.copyOf(builder.triggers);
        this.comment = builder.comment;
        this.rowCount = builder.rowCount;
    }

    public String getName() {
        return name;
    }

    public TableType getType() {
        return type;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    public PrimaryKeyMetadata getPrimaryKey() {
        return primaryKey;
    }

    public List<ForeignKeyMetadata> getForeignKeys() {
        return foreignKeys;
    }

    public List<IndexMetadata> getIndexes() {
        return indexes;
    }

    public List<TriggerMetadata> getTriggers() {
        return triggers;
    }

    public String getComment() {
        return comment;
    }

    public Long getRowCount() {
        return rowCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableMetadata that = (TableMetadata) o;
        return Objects.equals(name, that.name) &&
                type == that.type &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(primaryKey, that.primaryKey) &&
                Objects.equals(foreignKeys, that.foreignKeys) &&
                Objects.equals(indexes, that.indexes) &&
                Objects.equals(triggers, that.triggers) &&
                Objects.equals(comment, that.comment) &&
                Objects.equals(rowCount, that.rowCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, columns, primaryKey, foreignKeys,
                indexes, triggers, comment, rowCount);
    }

    @Override
    public String toString() {
        return "TableMetadata{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", columns=" + columns.size() +
                ", foreignKeys=" + foreignKeys.size() +
                ", indexes=" + indexes.size() +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(String name) {
        return new Builder().name(name);
    }

    public enum TableType {
        TABLE,
        VIEW,
        SYSTEM_TABLE,
        GLOBAL_TEMPORARY,
        LOCAL_TEMPORARY,
        ALIAS,
        SYNONYM
    }

    public static final class Builder {
        private String name;
        private TableType type = TableType.TABLE;
        private List<ColumnMetadata> columns = new ArrayList<>();
        private PrimaryKeyMetadata primaryKey;
        private List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();
        private List<IndexMetadata> indexes = new ArrayList<>();
        private List<TriggerMetadata> triggers = new ArrayList<>();
        private String comment;
        private Long rowCount;

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder type(TableType type) {
            this.type = type;
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

        public Builder primaryKey(PrimaryKeyMetadata primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder foreignKeys(List<ForeignKeyMetadata> foreignKeys) {
            this.foreignKeys = new ArrayList<>(foreignKeys);
            return this;
        }

        public Builder addForeignKey(ForeignKeyMetadata foreignKey) {
            this.foreignKeys.add(foreignKey);
            return this;
        }

        public Builder indexes(List<IndexMetadata> indexes) {
            this.indexes = new ArrayList<>(indexes);
            return this;
        }

        public Builder addIndex(IndexMetadata index) {
            this.indexes.add(index);
            return this;
        }

        public Builder triggers(List<TriggerMetadata> triggers) {
            this.triggers = new ArrayList<>(triggers);
            return this;
        }

        public Builder addTrigger(TriggerMetadata trigger) {
            this.triggers.add(trigger);
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder rowCount(Long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public TableMetadata build() {
            return new TableMetadata(this);
        }
    }
}
