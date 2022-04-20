package org.apache.kyuubi.engine.jdbc.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;

/**
 * JDBC查询结果封装对象
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCResultSet {
    private List<JDBCResultRow> resultRowList;
}
