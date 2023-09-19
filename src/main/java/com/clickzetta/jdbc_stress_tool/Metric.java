package com.clickzetta.jdbc_stress_tool;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

public class Metric {

  @Getter
  @Setter
  private String threadName;
  @Getter
  @Setter
  private String sqlId;
  @Getter
  @Setter
  private String jobId = "anonymous";
  @Getter
  @Setter
  private long clientStartMs;
  @Getter
  @Setter
  private long clientEndMs;
  @Getter
  @Setter
  private long clientRequestMs;
  @Getter
  @Setter
  private long clientResponseMs;
  @Getter
  @Setter
  private long gatewayStartMs;
  @Getter
  @Setter
  private long gatewayEndMs;
  @Getter
  @Setter
  private long serverSubmitMs;
  @Getter
  @Setter
  private long serverStartMs;
  @Getter
  @Setter
  private long serverPlanMs;
  @Getter
  @Setter
  private long serverDagMs;
  @Getter
  @Setter
  private long serverResourceMs;
  @Getter
  @Setter
  private long serverEndMs;
  @Getter
  @Setter
  private boolean isSuccess = false;
  @Getter
  @Setter
  private Long resultSize = -1L;

  @Getter
  private static final String header = StringUtils.join(new String[]{
          "thread_name", "sql_id", "is_success", "result_size",
          "job_id", "client_duration_ms", "server_duration_ms",
          "client_start_ms", "client_end_ms", "client_request_ms",
          "client_response_ms","gateway_start_ms","gateway_end_ms",
          "server_submit_ms","server_start_ms","server_plan_ms",
          "server_dag_ms","server_resource_ms", "server_end_ms"
  }, ',');

  public Metric() {
  }

  public Long getClientDuration() {
    return clientEndMs - clientStartMs;
  }

  @Override
  public String toString() {
    long clientDuration = clientEndMs - clientStartMs;
    long serverDuration = serverEndMs - serverStartMs;
    return String.format("%s,%s,%s,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",
            threadName, sqlId, isSuccess, resultSize,
            jobId, clientDuration, serverDuration,
            clientStartMs, clientEndMs, clientRequestMs, clientResponseMs, gatewayStartMs,
            gatewayEndMs, serverSubmitMs,serverStartMs, serverPlanMs, serverDagMs,
            serverResourceMs, serverEndMs);
  }
}
