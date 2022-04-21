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

@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCRowSet {
    private List<String> valueList;

    public List<String> getValueList() {
        return valueList;
    }

    public void setValueList(List<String> valueList) {
        this.valueList = valueList;
    }
}
