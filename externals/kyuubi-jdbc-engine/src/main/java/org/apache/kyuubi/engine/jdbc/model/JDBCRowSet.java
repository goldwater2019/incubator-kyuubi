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
  private List<String> valueList;

  public List<String> getValueList() {
    return valueList;
  }

  public List<Object> getValueListAsObject() {
    List<Object> result = new ArrayList<Object>();
    for (String s : getValueList()) {
      result.add((Object) s);
    }
    return result;
  }

  public void setValueList(List<String> valueList) {
    this.valueList = valueList;
  }
}
