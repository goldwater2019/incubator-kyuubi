package org.apache.kyuubi.engine.jdbc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCCatalog {
    private String name;
    private String driverClass;
    private String uri;
    private String username;
    private String password;
}
