package io.dbxmetadata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SchemaMetadata {

    private final String name;
    private final String catalog;
    private final List<TableMetadata> tables;
    private final List<ViewMetadata> views;
    private final List<ProcedureMetadata> procedures;
    private final String owner;

    private SchemaMetadata (Builder builder) {
        this.name = builder.name;
        this.catalog = builder.catalog;
        this.tables = List.copyOf(builder.tables);
        this.views = List.copyOf(builder.views);
        this.procedures = List.copyOf(builder.procedures);
        this.owner = builder.owner;
    }

    public String getName() {
        return name;
    }

    public String getCatalog() {
        return catalog;
    }

    public List<TableMetadata> getTables() {
        return tables;
    }

    public List<ViewMetadata> getViews() {
        return views;
    }

    public List<ProcedureMetadata> getProcedures() {
        return procedures;
    }

    public String getOwner() {
        return owner;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaMetadata that = (SchemaMetadata) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(catalog, that.catalog) &&
                Objects.equals(tables, that.tables) &&
                Objects.equals(views, that.views) &&
                Objects.equals(procedures, that.procedures) &&
                Objects.equals(owner, that.owner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, catalog, tables, views, procedures, owner);
    }

    @Override
    public String toString() {
        return "SchemaMetadata{" +
                "name='" + name + '\'' +
                ", catalog='" + catalog + '\'' +
                ", tables=" + tables.size() +
                ", views=" + views.size() +
                ", procedures=" + procedures.size() +
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
        private String catalog;
        private List<TableMetadata> tables;
        private List<ViewMetadata> views;
        private List<ProcedureMetadata> procedures;
        private String owner;

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder tables(List<TableMetadata> tables) {
            this.tables = tables;
            return this;
        }

        public Builder addTable(TableMetadata table) {
            this.tables.add(table);
            return this;
        }

        public Builder views(List<ViewMetadata> views) {
            this.views = views;
            return this;
        }

        public Builder addView(ViewMetadata view) {
            this.views.add(view);
            return this;
        }

        public Builder procedures(List<ProcedureMetadata> procedures) {
            this.procedures = procedures;
            return this;
        }

        public Builder addProcedure(ProcedureMetadata procedure) {
            this.procedures.add(procedure);
            return this;
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public SchemaMetadata build() {
            return new SchemaMetadata(this);
        }
    }
}
