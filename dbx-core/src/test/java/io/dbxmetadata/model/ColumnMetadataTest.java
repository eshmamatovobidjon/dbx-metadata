package io.dbxmetadata.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ColumnMetadata Tests")
class ColumnMetadataTest {

    @Test
    @DisplayName("Should build column with all properties")
    void shouldBuildColumnWithAllProperties() {
        ColumnMetadata column = ColumnMetadata.builder()
                .name("user_id")
                .dataType("INTEGER")
                .size(11)
                .precision(11)
                .scale(0)
                .nullable(false)
                .primaryKey(true)
                .autoIncrement(true)
                .defaultValue("0")
                .comment("Primary key")
                .ordinalPosition(1)
                .build();

        assertAll("Column properties",
                () -> assertEquals("user_id", column.getName()),
                () -> assertEquals("INTEGER", column.getDataType()),
                () -> assertEquals(11, column.getSize()),
                () -> assertEquals(11, column.getPrecision()),
                () -> assertEquals(0, column.getScale()),
                () -> assertFalse(column.isNullable()),
                () -> assertTrue(column.isPrimaryKey()),
                () -> assertTrue(column.isAutoIncrement()),
                () -> assertEquals("0", column.getDefaultValue()),
                () -> assertEquals("Primary key", column.getComment()),
                () -> assertEquals(1, column.getOrdinalPosition())
        );
    }

    @Test
    @DisplayName("Should build column with minimal properties")
    void shouldBuildColumnWithMinimalProperties() {
        ColumnMetadata column = ColumnMetadata.builder("email")
                .dataType("VARCHAR")
                .size(255)
                .build();

        assertAll("Column properties",
                () -> assertEquals("email", column.getName()),
                () -> assertEquals("VARCHAR", column.getDataType()),
                () -> assertEquals(255, column.getSize()),
                () -> assertTrue(column.isNullable()), // Default
                () -> assertFalse(column.isPrimaryKey()),
                () -> assertFalse(column.isAutoIncrement()),
                () -> assertNull(column.getDefaultValue()),
                () -> assertNull(column.getComment())
        );
    }

    @Test
    @DisplayName("Should throw exception when name is null")
    void shouldThrowExceptionWhenNameIsNull() {
        assertThrows(NullPointerException.class, () -> 
                ColumnMetadata.builder().dataType("VARCHAR").build());
    }

    @Test
    @DisplayName("Should implement equals correctly")
    void shouldImplementEqualsCorrectly() {
        ColumnMetadata col1 = ColumnMetadata.builder("id")
                .dataType("INTEGER")
                .size(11)
                .nullable(false)
                .build();

        ColumnMetadata col2 = ColumnMetadata.builder("id")
                .dataType("INTEGER")
                .size(11)
                .nullable(false)
                .build();

        ColumnMetadata col3 = ColumnMetadata.builder("id")
                .dataType("BIGINT")
                .size(20)
                .nullable(false)
                .build();

        assertEquals(col1, col2);
        assertNotEquals(col1, col3);
        assertEquals(col1.hashCode(), col2.hashCode());
    }

    @Test
    @DisplayName("Should generate meaningful toString")
    void shouldGenerateMeaningfulToString() {
        ColumnMetadata column = ColumnMetadata.builder("email")
                .dataType("VARCHAR")
                .size(255)
                .nullable(true)
                .primaryKey(false)
                .build();

        String str = column.toString();
        assertTrue(str.contains("email"));
        assertTrue(str.contains("VARCHAR"));
        assertTrue(str.contains("255"));
    }
}
