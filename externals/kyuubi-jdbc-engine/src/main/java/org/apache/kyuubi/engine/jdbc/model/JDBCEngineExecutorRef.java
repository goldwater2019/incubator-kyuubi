package org.apache.kyuubi.engine.jdbc.model;

import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCEngineExecutorRef {
  private String host;
  private Integer port;
  private String prefixPath;
  private UUID executorRefId;
  private long latestHeartbeatTime;
  private long lastAccessTime; // 为了负载均衡使用
}
