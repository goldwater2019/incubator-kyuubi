package org.apache.kyuubi.engine.jdbc.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** JDBC查询结果封装对象 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCResultSet {
  private List<JDBCResultRow> resultRowList;
}
