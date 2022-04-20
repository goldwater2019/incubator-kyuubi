package org.apache.kyuubi.engine.jdbc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kyuubi.engine.jdbc.enumeration.JDBCColumnType;

/**
 * @Author: zhangxinsen
 * @Date: 2022/4/20 3:31 PM
 * @Desc:
 * @Version: v1.0
 */


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCColumn {
    private JDBCColumnType jdbcColumnType;
    private String jdbcColumnName;
}
