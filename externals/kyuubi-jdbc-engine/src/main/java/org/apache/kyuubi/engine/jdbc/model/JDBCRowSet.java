package org.apache.kyuubi.engine.jdbc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @Author: zhangxinsen
 * @Date: 2022/4/20 3:32 PM
 * @Desc:
 * @Version: v1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCRowSet {
    private List<String> valueList;
}
