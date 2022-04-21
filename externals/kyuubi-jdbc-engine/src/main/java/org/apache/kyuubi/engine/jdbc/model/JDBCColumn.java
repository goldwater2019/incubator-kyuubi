package org.apache.kyuubi.engine.jdbc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCColumnType;

/**
 * @Author: zhangxinsen
 * @Date: 2022/4/20 3:31 PM
 * @Desc:
 * @Version: v1.0
 */


@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCColumn {
    public JDBCColumnType getJdbcColumnType() {
        return jdbcColumnType;
    }

    public void setJdbcColumnType(JDBCColumnType jdbcColumnType) {
        this.jdbcColumnType = jdbcColumnType;
    }

    public String getJdbcColumnName() {
        return jdbcColumnName;
    }

    public void setJdbcColumnName(String jdbcColumnName) {
        this.jdbcColumnName = jdbcColumnName;
    }

    private JDBCColumnType jdbcColumnType;
    private String jdbcColumnName;
}
