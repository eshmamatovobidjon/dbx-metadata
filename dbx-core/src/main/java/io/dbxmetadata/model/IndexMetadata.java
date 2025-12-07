package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class IndexMetadata {

    private final String name;
    private final List<IndexColumn> columns;
    private final boolean unique;
    private final IndexType type;
    private final String filterCondition;

    private IndexMetadata(Builder builder) {
        this.name = builder.name;
        this.columns = List.copyOf(builder.columns);
        this.unique = builder.unique;
        this.type = builder.type;
        this.filterCondition = builder.filterCondition;
    }

    public String getName() {
        return name;
    }

    public List<IndexColumn> getColumns() {
        return columns;
    }

    public boolean isUnique() {
        return unique;
    }

    public IndexType getType() {
        return type;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexMetadata that = (IndexMetadata) o;
        return unique == that.unique &&
                Objects.equals(name, that.name) &&
                Objects.equals(columns, that.columns) &&
                type == that.type &&
                Objects.equals(filterCondition, that.filterCondition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columns, unique, type, filterCondition);
    }

    @Override
    public String toString() {
        return "IndexMetadata{" +
                "name='" + name + '\'' +
                ", columns=" + columns +
                ", unique=" + unique +
                ", type=" + type +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public enum IndexType {
        BTREE,
        HASH,
        FULLTEXT,
        SPATIAL,
        CLUSTERED,
        NONCLUSTERED,
        OTHER
    }

    // Represents a column in an index with its sort order.
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record IndexColumn(String name, SortOrder sortOrder, int position) {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexColumn that = (IndexColumn) o;
            return position == that.position &&
                    Objects.equals(name, that.name) &&
                    sortOrder == that.sortOrder;
        }

        @Override
        public String toString() {
            return name + " " + sortOrder;
        }
    }

    public enum SortOrder {
        ASC,
        DESC,
        UNKNOWN
    }

    public static final class Builder {
        private String name;
        private List<IndexColumn> columns = new ArrayList<>();
        private boolean unique;
        private IndexType type = IndexType.OTHER;
        private String filterCondition;

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder columns(List<IndexColumn> columns) {
            this.columns = new ArrayList<>(columns);
            return this;
        }

        public Builder addColumn(String columnName, SortOrder sortOrder, int position) {
            this.columns.add(new IndexColumn(columnName, sortOrder, position));
            return this;
        }

        public Builder addColumn(String columnName, SortOrder sortOrder) {
            this.columns.add(new IndexColumn(columnName, sortOrder, this.columns.size() + 1));
            return this;
        }

        public Builder unique(boolean unique) {
            this.unique = unique;
            return this;
        }

        public Builder type(IndexType type) {
            this.type = type;
            return this;
        }

        public Builder filterCondition(String filterCondition) {
            this.filterCondition = filterCondition;
            return this;
        }

        public IndexMetadata build() {
            return new IndexMetadata(this);
        }
    }
}
