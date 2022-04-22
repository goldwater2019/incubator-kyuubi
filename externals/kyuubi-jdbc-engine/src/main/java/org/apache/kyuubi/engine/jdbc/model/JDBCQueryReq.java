package org.apache.kyuubi.engine.jdbc.model;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
public class JDBCQueryReq {
  private String querySql;
}
