package com.gigaspaces.sql;

import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;

public class DdlParser {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String CREATE_TABLE_PREFIX = "CREATE TABLE ";
    private static final String ALTER_TABLE_PREFIX = "ALTER TABLE ";

    public Collection<SpaceTypeDescriptorBuilder> parse(Path path) throws IOException {
        byte[] data = Files.readAllBytes(path);
        String s = new String(data);
        return parse(s);
    }

    public Collection<SpaceTypeDescriptorBuilder> parse(String sql) {
        Collection<SpaceTypeDescriptorBuilder> result = new ArrayList<>();
        String[] commands = sql.split(";");
        for (String command : commands) {
            StringWrapper sw = new StringWrapper(command.trim());
            if (sw.startsWith(CREATE_TABLE_PREFIX)) {
                result.add(parseCreateTableCommand(sw));
            } else if (sw.startsWith(ALTER_TABLE_PREFIX)) {
                parseAlterTableCommand(sw);
            } else if (!sw.s.isEmpty()) {
                logger.info("Skipping unsupported command: [{}]", sw.getAbbreviation(30));
            }
        }

        return result;
    }

    protected SpaceTypeDescriptorBuilder parseCreateTableCommand(StringWrapper sql) {
        // Remove create table prefix:
        sql.skip(CREATE_TABLE_PREFIX);
        // Read type name:
        String typeName = sql.readUntilWhiteSpace();
        SpaceTypeDescriptorBuilder builder = new SpaceTypeDescriptorBuilder(typeName);
        // Remove everything outside 'create table (...) ...'
        sql.trimOutsideEnclosingPair('(', ')');
        // Split and parse table columns:
        Collection<String> columns = sql.splitExcludingEnclosingPair(',', '(', ')');
        for (String column : columns) {
            parseColumn(column, builder);
        }

        return builder;
    }

    private void parseColumn(String column, SpaceTypeDescriptorBuilder builder) {
        StringWrapper sw = new StringWrapper(column);
        String name = sw.readUntilWhiteSpace();
        sw.trim();
        String sqlType = sw.readUntilWhiteSpace();
        Class<?> javaType = parseSqlType(sqlType);
        builder.addFixedProperty(name, javaType);
    }

    protected void parseAlterTableCommand(StringWrapper sql) {
        // Remove create table prefix:
        sql.skip(CREATE_TABLE_PREFIX);
        // Read type name:
        String typeName = sql.readUntilWhiteSpace();

        logger.warn("Skipping alter table command for " + typeName);
    }

    protected Class<?> parseSqlType(String sqlType) {
        // remove (...) if exists - e.g. CHARACTER(4)
        int pos = sqlType.indexOf('(');
        if (pos != -1)
            sqlType = sqlType.substring(0, pos);
        switch (sqlType) {
            case "VARCHAR2":
            case "NVARCHAR2":
            case "NCHAR VARYING":
            case "VARCHAR":
            case "CHAR":
            case "NCHAR":
            case "NVARCHAR":
            case "SYSNAME":
            case "CLOB":
            case "RAW":
            case "MONEY":
            case "SMALLMONEY":
            case "TEXT":
            case "NTEXT":
            case "GRAPHIC":
            case "VARGRAPHIC":
            case "VARG":
            case "VARBINARY":
            case "VARBIN":
            case "CHARACTER":
            case "UNIQUEIDENTIFIER":
            case "DECFLOAT":
            case "LONG":
                return java.lang.String.class;
            case "TINYINT":
            case "SMALLINT":
                return java.lang.Short.class;
            case "INT":
            case "INTEGER":
                return java.lang.Integer.class;
            case "BIGINT":
                return java.math.BigInteger.class;
            case "NUMBER":
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return java.lang.Double.class;
            case "REAL":
            case "FLOAT":
                return java.lang.Float.class;
            case "NUMERIC":
            case "DECIMAL":
                return java.math.BigDecimal.class;
            case "DATE":
            case "DATETIME":
                return java.sql.Date.class;
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
            case "TIMESTMP":
            case "TIMESTZ":
                return java.sql.Timestamp.class;
            case "TIME":
            case "TIME WITH TIME ZONE":
                return java.sql.Time.class;
            case "BOOLEAN":
                return java.lang.Boolean.class;
            default :
                return java.lang.String.class;
        }
    }

    private static class StringWrapper {
        private String s;

        public StringWrapper(String s) {
            this.s = s;
        }

        public void trim() {
            s = s.trim();
        }

        public boolean startsWith(String prefix) {
            return s.startsWith(prefix);
        }

        public void skip(int length) {
            s = s.substring(length);
        }

        public void skip(String prefix) {
            s = s.substring(prefix.length());
        }

        private String readUntil(int pos) {
            if (pos == -1)
                return null;
            String result = s.substring(0, pos);
            skip(result);
            return result;
        }

        private String readUntilWhiteSpace() {
            return readUntil(indexOfWhiteSpace());
        }

        public int indexOfWhiteSpace() {
            int length = s.length();
            for (int i = 0; i < length; i++)
                if (Character.isWhitespace(s.charAt(i)))
                    return i;
            return -1;
        }

        public int indexOfEnclosingPair(char left, char right) {
            int length = s.length();
            int depth = 0;
            for (int i = 0; i < length; i++) {
                char c = s.charAt(i);
                if (c == left)
                    depth++;
                else if (c == right) {
                    depth--;
                    if (depth == 0)
                        return i;
                }
            }
            return -1;
        }

        public int indexOfExcludingEnclosingPair(char charToFind, char left, char right) {
            int length = s.length();
            int depth = 0;
            for (int i = 0; i < length; i++) {
                char c = s.charAt(i);
                if (c == left) {
                    depth++;
                } else if (c == right) {
                    depth--;
                } else if (c == charToFind && depth == 0)
                    return i;
            }
            return -1;
        }

        public Collection<String> splitExcludingEnclosingPair(char charToFind, char left, char right) {
            Collection<String> result = new ArrayList<>();
            int pos;
            do {
                pos = indexOfExcludingEnclosingPair(charToFind, left, right);
                if (pos != -1) {
                    result.add(readUntil(pos).trim());
                    skip(1);
                } else {
                    result.add(s.trim());
                }
            } while (pos != -1);
            return result;
        }

        public void trimOutsideEnclosingPair(char left, char right) {
            int rightPos = indexOfEnclosingPair(left, right);
            s = s.substring(0, rightPos);
            s = s.substring(s.indexOf(left) + 1);
            s = s.trim();
        }

        private String getAbbreviation(int maxLength) {
            return s.length() < maxLength ? s : s.substring(0, maxLength) + "...";
        }
    }
}
