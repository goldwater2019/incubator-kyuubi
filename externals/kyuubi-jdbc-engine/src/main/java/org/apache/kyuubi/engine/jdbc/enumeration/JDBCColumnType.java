package org.apache.kyuubi.engine.jdbc.enumeration;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: zhangxinsen
 * @Date: 2022/4/19 5:50 PM
 * @Desc:
 * @Version: v1.0
 */

public enum JDBCColumnType {
    BIT(-7),
    TINYINT(-6),
    SMALLINT(5),
    INTEGER(4),
    BIGINT(-5),
    FLOAT(6),
    REAL(7),
    DOUBLE(8),
    NUMERIC(2),
    DECIMAL(3),
    CHAR(1),
    VARCHAR(12),
    LONGVARCHAR(-1),
    DATE(91),
    TIME(92),
    TIMESTAMP(93),
    BINARY(-2),
    VARBINARY(-3),
    LONGVARBINARY(-4),
    NULL(0),
    STRUCT(2002),
    ARRAY(2003),
    BOOLEAN(16),
    LONGNVARCHAR(-16),
    OTHER(1111);

    private static final Map<Integer, JDBCColumnType> value2jdbcColumnType = new HashMap<>();

    static {
        value2jdbcColumnType.put(-7, BIT);
        value2jdbcColumnType.put(-6, TINYINT);
        value2jdbcColumnType.put(5, SMALLINT);
        value2jdbcColumnType.put(4, INTEGER);
        value2jdbcColumnType.put(-5, BIGINT);
        value2jdbcColumnType.put(6, FLOAT);
        value2jdbcColumnType.put(7, REAL);
        value2jdbcColumnType.put(8, DOUBLE);
        value2jdbcColumnType.put(2, NUMERIC);
        value2jdbcColumnType.put(3, DECIMAL);
        value2jdbcColumnType.put(1, CHAR);
        value2jdbcColumnType.put(12, VARCHAR);
        value2jdbcColumnType.put(-1, LONGVARCHAR);
        value2jdbcColumnType.put(91, DATE);
        value2jdbcColumnType.put(92, TIME);
        value2jdbcColumnType.put(93, TIMESTAMP);
        value2jdbcColumnType.put(-2, BINARY);
        value2jdbcColumnType.put(-3, VARBINARY);
        value2jdbcColumnType.put(-4, LONGVARBINARY);
        value2jdbcColumnType.put(0, NULL);
        value2jdbcColumnType.put(2002, STRUCT);
        value2jdbcColumnType.put(2003, ARRAY);
        value2jdbcColumnType.put(16, BOOLEAN);
        value2jdbcColumnType.put(-16, LONGNVARCHAR);
        value2jdbcColumnType.put(1111, OTHER);
    }

    private final int value;

    JDBCColumnType(int value) {
        this.value = value;
    }

    public static JDBCColumnType findByValue(int value) {
        return value2jdbcColumnType.getOrDefault(value, JDBCColumnType.OTHER);
    }
}
