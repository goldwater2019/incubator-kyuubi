package org.apache.kyuubi.engine.jdbc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCColumnType;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCResultColumn {
  private String columnName;
  private JDBCColumnType columnType;
  private String columnClassName;
  private String columnValue;
}
