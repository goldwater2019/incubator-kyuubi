package org.apache.kyuubi.engine.jdbc.model;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

/** @Author: zhangxinsen @Date: 2022/4/20 3:32 PM @Desc: @Version: v1.0 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JDBCRowSet {
  private List<Object> valueList;

  public List<Object> getValueList() {
    return valueList;
  }

  public void setValueList(List<Object> valueList) {
    this.valueList = valueList;
  }
}