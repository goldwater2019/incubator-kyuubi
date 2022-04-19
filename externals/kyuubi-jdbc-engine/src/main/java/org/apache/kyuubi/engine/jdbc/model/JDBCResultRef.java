package org.apache.kyuubi.engine.jdbc.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCResultRef {
    private JDBCOperationRef jdbcOperationRef;
    private JDBCResultSet jdbcResultSet;
}
