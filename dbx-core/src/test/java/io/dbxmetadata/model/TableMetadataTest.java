package io.dbxmetadata.model;

import io.dbxmetadata.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TableMetadata Tests")
class TableMetadataTest {

    @Test
    @DisplayName("Should build table with all components")
    void shouldBuildTableWithAllComponents() {
        ColumnMetadata idColumn = ColumnMetadata.builder("id")
                .dataType("INTEGER")
                .primaryKey(true)
                .build();
        
        ColumnMetadata nameColumn = ColumnMetadata.builder("name")
                .dataType("VARCHAR")
                .size(100)
                .build();

        PrimaryKeyMetadata pk = PrimaryKeyMetadata.builder()
                .name("pk_users")
                .addColumn("id")
                .build();

        ForeignKeyMetadata fk = ForeignKeyMetadata.builder()
                .name("fk_user_role")
                .addColumn("role_id")
                .referencedTable("roles")
                .addReferencedColumn("id")
                .build();

        IndexMetadata idx = IndexMetadata.builder()
                .name("idx_name")
                .addColumn("name", IndexMetadata.SortOrder.ASC)
                .unique(false)
                .build();

        TableMetadata table = TableMetadata.builder("users")
                .type(TableMetadata.TableType.TABLE)
                .addColumn(idColumn)
                .addColumn(nameColumn)
                .primaryKey(pk)
                .addForeignKey(fk)
                .addIndex(idx)
                .comment("User accounts")
                .rowCount(1000L)
                .build();

        assertAll("Table properties",
                () -> assertEquals("users", table.getName()),
                () -> assertEquals(TableMetadata.TableType.TABLE, table.getType()),
                () -> assertEquals(2, table.getColumns().size()),
                () -> assertNotNull(table.getPrimaryKey()),
                () -> assertEquals(1, table.getForeignKeys().size()),
                () -> assertEquals(1, table.getIndexes().size()),
                () -> assertEquals("User accounts", table.getComment()),
                () -> assertEquals(1000L, table.getRowCount())
        );
    }

    @Test
    @DisplayName("Should create empty table")
    void shouldCreateEmptyTable() {
        TableMetadata table = TableMetadata.builder("empty_table").build();

        assertAll("Empty table",
                () -> assertEquals("empty_table", table.getName()),
                () -> assertEquals(TableMetadata.TableType.TABLE, table.getType()),
                () -> assertTrue(table.getColumns().isEmpty()),
                () -> assertNull(table.getPrimaryKey()),
                () -> assertTrue(table.getForeignKeys().isEmpty()),
                () -> assertTrue(table.getIndexes().isEmpty()),
                () -> assertTrue(table.getTriggers().isEmpty())
        );
    }

    @Test
    @DisplayName("Should return immutable collections")
    void shouldReturnImmutableCollections() {
        ColumnMetadata col = ColumnMetadata.builder("id").dataType("INT").build();
        TableMetadata table = TableMetadata.builder("test")
                .addColumn(col)
                .build();

        assertThrows(UnsupportedOperationException.class, () ->
                table.getColumns().add(col));
        assertThrows(UnsupportedOperationException.class, () ->
                table.getForeignKeys().add(null));
        assertThrows(UnsupportedOperationException.class, () ->
                table.getIndexes().add(null));
    }

    @Test
    @DisplayName("Should throw exception when name is null")
    void shouldThrowExceptionWhenNameIsNull() {
        assertThrows(NullPointerException.class, () ->
                TableMetadata.builder().build());
    }
}
