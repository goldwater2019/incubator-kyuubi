package org.apache.kyuubi.engine.jdbc.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCResultRef {
    private JDBCOperationRef jdbcOperationRef;
    private JDBCResultSet jdbcResultSet;
    private List<JDBCColumn> jdbcColumnList;
    private List<JDBCRowSet> jdbcRowSetList;
}
