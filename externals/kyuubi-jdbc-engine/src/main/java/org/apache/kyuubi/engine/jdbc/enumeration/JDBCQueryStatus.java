package org.apache.kyuubi.engine.jdbc.enumeration;

/** @Author: zhangxinsen @Date: 2022/4/19 5:48 PM @Desc: @Version: v1.0 */
public enum JDBCQueryStatus {
  OK(0),
  FAILED(1);

  private final int value;

  JDBCQueryStatus(int value) {
    this.value = value;
  }
}
