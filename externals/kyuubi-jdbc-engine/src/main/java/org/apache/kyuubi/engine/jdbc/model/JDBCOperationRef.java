package org.apache.kyuubi.engine.jdbc.model;

import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCQueryStatus;

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

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public void setCatalogName(String catalogName) {
    this.catalogName = catalogName;
  }

  public UUID getOperationRefId() {
    return operationRefId;
  }

  public void setOperationRefId(UUID operationRefId) {
    this.operationRefId = operationRefId;
  }

  public String getSqlStatement() {
    return sqlStatement;
  }

  public void setSqlStatement(String sqlStatement) {
    this.sqlStatement = sqlStatement;
  }

  public JDBCQueryStatus getQueryStatus() {
    return queryStatus;
  }

  public void setQueryStatus(JDBCQueryStatus queryStatus) {
    this.queryStatus = queryStatus;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
