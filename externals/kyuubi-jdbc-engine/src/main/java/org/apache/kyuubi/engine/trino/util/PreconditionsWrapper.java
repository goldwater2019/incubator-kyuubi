package org.apache.kyuubi.engine.trino.util;

/** @Author: zhangxinsen @Date: 2022/4/20 9:55 AM @Desc: @Version: v1.0 */
import com.google.common.base.Preconditions;

public class PreconditionsWrapper {
  /**
   * To avoid ambiguous reference to overloaded definition in scala. {@link
   * Preconditions#checkArgument(boolean, Object)} {@link Preconditions#checkArgument(boolean,
   * String, Object...)}
   */
  public static void checkArgument(boolean expression, Object errorMessage) {
    Preconditions.checkArgument(expression, errorMessage);
  }
}
