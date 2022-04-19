package org.apache.kyuubi.engine.jdbc.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCQueryStatus;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCOperationRef {
    long startTime;
    long endTime;
    String catalogName;
    UUID operationRefId;
    String sqlStatement;
    JDBCQueryStatus queryStatus;
    String message;
}
