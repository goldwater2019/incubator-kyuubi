package org.apache.kyuubi.engine.jdbc.client;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.kyuubi.engine.jdbc.model.JDBCQueryReq;
import org.apache.kyuubi.engine.jdbc.model.JDBCResultRef;
import org.apache.kyuubi.engine.jdbc.model.JsonResult;

@Data
@NoArgsConstructor
@EqualsAndHashCode
@Builder
@Slf4j
public class JDBCEngineGatewayClientManager {

  private static volatile JDBCEngineGatewayClientManager singleton;

  public static JDBCEngineGatewayClientManager getInstance() {
    if (singleton == null) {
      synchronized (JDBCEngineGatewayClientManager.class) {
        if (singleton == null) {
          singleton = new JDBCEngineGatewayClientManager();
        }
      }
    }
    return singleton;
  }

  public JsonResult<JDBCResultRef> querySql(String querySql, String baseUrl) throws IOException {
    String requestUrl = baseUrl + "/driver/query";
    OkHttpClient okHttpClient = new OkHttpClient.Builder().build();

    JDBCQueryReq jdbcQueryReq = JDBCQueryReq.builder().querySql(querySql).build();
    String json = JSONObject.toJSONString(jdbcQueryReq);
    RequestBody requestBody =
        RequestBody.create(MediaType.parse("application/json; charset=utf-8"), json);
    Request request = new Request.Builder().url(requestUrl).post(requestBody).build();
    Response response = okHttpClient.newCall(request).execute();
    String responseJsonStr = response.body().string();
    JSONObject jsonObject = JSONObject.parseObject(responseJsonStr);
    JDBCResultRef data = JSONObject.parseObject(jsonObject.getString("data"), JDBCResultRef.class);
    return new JsonResult<JDBCResultRef>(
        data, jsonObject.getString("msg"), jsonObject.getInteger("code"));
  }
}
