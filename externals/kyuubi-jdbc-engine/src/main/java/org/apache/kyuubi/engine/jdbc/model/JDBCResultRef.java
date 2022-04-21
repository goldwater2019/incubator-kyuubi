package org.apache.kyuubi.engine.jdbc.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCResultRef {
    private JDBCOperationRef jdbcOperationRef;
    private JDBCResultSet jdbcResultSet;
    private List<JDBCColumn> jdbcColumnList;
    private List<JDBCRowSet> jdbcRowSetList;

    public JDBCOperationRef getJdbcOperationRef() {
        return jdbcOperationRef;
    }

    public void setJdbcOperationRef(JDBCOperationRef jdbcOperationRef) {
        this.jdbcOperationRef = jdbcOperationRef;
    }

    public JDBCResultSet getJdbcResultSet() {
        return jdbcResultSet;
    }

    public void setJdbcResultSet(JDBCResultSet jdbcResultSet) {
        this.jdbcResultSet = jdbcResultSet;
    }

    public List<JDBCColumn> getJdbcColumnList() {
        return jdbcColumnList;
    }

    public void setJdbcColumnList(List<JDBCColumn> jdbcColumnList) {
        this.jdbcColumnList = jdbcColumnList;
    }

    public List<JDBCRowSet> getJdbcRowSetList() {
        return jdbcRowSetList;
    }

    public void setJdbcRowSetList(List<JDBCRowSet> jdbcRowSetList) {
        this.jdbcRowSetList = jdbcRowSetList;
    }
}
