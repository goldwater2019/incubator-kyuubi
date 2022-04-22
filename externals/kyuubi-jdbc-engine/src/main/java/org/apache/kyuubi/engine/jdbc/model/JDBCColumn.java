package org.apache.kyuubi.engine.jdbc.model;

import lombok.NoArgsConstructor;
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCColumnType;

/** @Author: zhangxinsen @Date: 2022/4/20 3:31 PM @Desc: @Version: v1.0 */
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

  public JDBCColumn(String columnName, JDBCColumnType columnType) {
    setJdbcColumnName(columnName);
    setJdbcColumnType(columnType);
  }

  public JDBCColumn() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String jdbcColumnName;
    private JDBCColumnType jdbcColumnType;

    public Builder jdbcColumnName(String jdbcColumnName) {
      this.jdbcColumnName = jdbcColumnName;
      return this;
    }

    public Builder jdbcColumnType(JDBCColumnType jdbcColumnType) {
      this.jdbcColumnType = jdbcColumnType;
      return this;
    }

    public JDBCColumn build() {
      return new JDBCColumn(jdbcColumnName, jdbcColumnType);
    }

  }

}
