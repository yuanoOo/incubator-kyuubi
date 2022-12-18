<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Monitoring Kyuubi - Server Metrics

Kyuubi has a configurable metrics system based on the [Dropwizard Metrics Library](https://metrics.dropwizard.io/).
This allows users to report Kyuubi metrics to a variety of `kyuubi.metrics.reporters`. 
The metrics provide instrumentation for specific activities and Kyuubi server.

## Configurations

The metrics system is configured via `$KYUUBI_HOME/conf/kyuubi-defaults.conf`.

Key | Default | Meaning | Type | Since
--- | --- | --- | --- | ---
`kyuubi.metrics.enabled`|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>true</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Set to true to enable kyuubi metrics system</div>|<div style='width: 30pt'>boolean</div>|<div style='width: 20pt'>1.2.0</div>
`kyuubi.metrics.reporters`|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>JSON</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>A comma separated list for all metrics reporters<ul> <li>CONSOLE - ConsoleReporter which outputs measurements to CONSOLE periodically.</li> <li>JMX - JmxReporter which listens for new metrics and exposes them as MBeans.</li>  <li>JSON - JsonReporter which outputs measurements to json file periodically.</li> <li>PROMETHEUS - PrometheusReporter which exposes metrics in prometheus format.</li> <li>SLF4J - Slf4jReporter which outputs measurements to system log periodically.</li></ul></div>|<div style='width: 30pt'>seq</div>|<div style='width: 20pt'>1.2.0</div>
`kyuubi.metrics.console.interval`|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often should report metrics to console</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.2.0</div>
`kyuubi.metrics.json.interval`|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often should report metrics to json file</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.2.0</div>
`kyuubi.metrics.json.location`|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>metrics</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Where the json metrics file located</div>|<div style='width: 30pt'>string</div>|<div style='width: 20pt'>1.2.0</div>
`kyuubi.metrics.prometheus.path`|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>/metrics</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>URI context path of prometheus metrics HTTP server</div>|<div style='width: 30pt'>string</div>|<div style='width: 20pt'>1.2.0</div>
`kyuubi.metrics.prometheus.port`|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>10019</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>Prometheus metrics HTTP server port</div>|<div style='width: 30pt'>int</div>|<div style='width: 20pt'>1.2.0</div>
`kyuubi.metrics.slf4j.interval`|<div style='width: 65pt;word-wrap: break-word;white-space: normal'>PT5S</div>|<div style='width: 170pt;word-wrap: break-word;white-space: normal'>How often should report metrics to SLF4J logger</div>|<div style='width: 30pt'>duration</div>|<div style='width: 20pt'>1.2.0</div>

## Metrics

These metrics include:

Metrics Prefix | Metrics Suffix | Type | Since | Description
---|---|---|---|---
`kyuubi.exec.pool.threads.alive`  | | gauge | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> threads keepAlive in the backend executive thread pool</div>
`kyuubi.exec.pool.threads.active` | | gauge | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> threads active in the backend executive thread pool</div>
`kyuubi.connection.total`   | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative connection count</div>
`kyuubi.connection.opened`  | | gauge | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> current active connection count</div>
`kyuubi.connection.opened`  | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> current active connections count requested by a `${user}`</div>
`kyuubi.connection.failed`  | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed connection count</div>
`kyuubi.connection.failed`  | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> cumulative failed connections for a `${user}`</div>
`kyuubi.operation.total`    | | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative opened operation count</div>
`kyuubi.operation.total`    | `${operationType}` | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative opened count for the operation `${operationType}`</div>
`kyuubi.operation.opened`   | | gauge | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  current opened operation count</div>
`kyuubi.operation.opened`   | `${operationType}` | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  current opened count for the operation `${operationType}`</div>
`kyuubi.operation.failed`   | `${operationType}`<br/>`.${errorType}` | counter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative failed count for the operation `${operationType}` with a particular `${errorType}`, e.g. `execute_statement.AnalysisException`</div>
`kyuubi.operation.state`    | `${operationState}` | meter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  kyuubi operation state rate </div>
`kyuubi.engine.total`       | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative created engines</div>
`kyuubi.engine.timeout`     | | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative timeout engines</div>
`kyuubi.engine.failed`      | `${user}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  cumulative explicitly failed engine count for a `${user}`</div>
`kyuubi.engine.failed`      | `${errorType}` | counter | 1.2.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> cumulative explicitly failed engine count for a particular `${errorType}`, e.g. `ClassNotFoundException`</div>
`kyuubi.backend_service.open_session`            | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `openSession` method execution time and rate </div>
`kyuubi.backend_service.close_session`           | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `closeSession` method execution time and rate </div>
`kyuubi.backend_service.get_info`                | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getInfo` method execution time and rate </div>
`kyuubi.backend_service.execute_statement`       | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `executeStatement` method execution time and rate </div>
`kyuubi.backend_service.get_type_info`           | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getTypeInfo` method execution time and rate </div>
`kyuubi.backend_service.get_catalogs`            | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getCatalogs` method execution time and rate </div>
`kyuubi.backend_service.get_schemas`             | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getSchemas` method execution time and rate </div>
`kyuubi.backend_service.get_tables`              | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getTables` method execution time and rate </div>
`kyuubi.backend_service.get_table_types`         | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getTableTypes` method execution time and rate </div>
`kyuubi.backend_service.get_columns`             | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getColumns` method execution time and rate </div>
`kyuubi.backend_service.get_functions`           | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getFunctions` method execution time and rate </div>
`kyuubi.backend_service.get_operation_status`    | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getOperationStatus` method execution time and rate </div>
`kyuubi.backend_service.cancel_operation`        | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `cancelOperation` method execution time and rate </div>
`kyuubi.backend_service.close_operation`         | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `closeOperation` method execution time and rate </div>
`kyuubi.backend_service.get_result_set_metadata` | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `getResultSetMetadata` method execution time and rate </div>
`kyuubi.backend_service.fetch_results`           | | timer | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `fetchResults` method execution time and rate </div>
`kyuubi.backend_service.fetch_log_rows_rate`     | | meter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `fetchResults` method that fetch log rows rate </div>
`kyuubi.backend_service.fetch_result_rows_rate`  | | meter | 1.5.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `fetchResults` method that fetch result rows rate </div>
`kyuubi.backend_service.get_primary_keys`        | | meter | 1.6.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `get_primary_keys` method execution time and rate </div>
`kyuubi.backend_service.get_cross_reference`     | | meter | 1.6.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> kyuubi backend service `get_cross_reference` method execution time and rate </div>
`kyuubi.operation.state` | `${operationType}`<br/>`.${state}` | meter | 1.6.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'>  The `${operationType}` with a particular `${state}` rate, e.g. `BatchJobSubmission.pending`, `BatchJobSubmission.finished`. Note that, the terminal states are cumulative, but the intermediate ones are not. </div>
`kyuubi.metadata.request.opened`                 | | counter | 1.6.1 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> current opened count for the metadata requests </div>
`kyuubi.metadata.request.total`                  | | meter | 1.6.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> metadata requests time and rate </div>
`kyuubi.metadata.request.failed`                 | | meter | 1.6.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> metadata requests failure time and rate </div>
`kyuubi.metadata.request.retrying`               | | meter | 1.6.0 |<div style='width: 150pt;word-wrap: break-word;white-space: normal'> retrying metadata requests time and rate, it is not cumulative </div>

Before v1.5.0, if you use these metrics:
- `kyuubi.statement.total`
- `kyuubi.statement.opened`
- `kyuubi.statement.failed.${errorType}`

Since v1.5.0, you can use the following metrics to replace:
- `kyuubi.operation.total.ExecuteStatement`
- `kyuubi.operation.opened.ExecuteStatement`
- `kyuubi.operation.failed.ExecuteStatement.${errorType}`
