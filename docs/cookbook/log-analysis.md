# Log Analysis with ja

This cookbook shows how to analyze server logs using `ja`. We'll work with common log formats and build progressively more complex analyses.

## Sample Data

Let's start with web server access logs in JSONL format:

**`access.jsonl`**:

```json
{"timestamp": "2024-01-15T10:30:45Z", "method": "GET", "path": "/api/users", "status": 200, "response_time": 45, "ip": "192.168.1.100", "user_agent": "Mozilla/5.0"}
{"timestamp": "2024-01-15T10:31:12Z", "method": "POST", "path": "/api/login", "status": 401, "response_time": 120, "ip": "192.168.1.105", "user_agent": "curl/7.68.0"}
{"timestamp": "2024-01-15T10:31:45Z", "method": "GET", "path": "/api/users/123", "status": 200, "response_time": 67, "ip": "192.168.1.100", "user_agent": "Mozilla/5.0"}
{"timestamp": "2024-01-15T10:32:01Z", "method": "DELETE", "path": "/api/users/456", "status": 403, "response_time": 23, "ip": "192.168.1.107", "user_agent": "PostmanRuntime/7.28.4"}
{"timestamp": "2024-01-15T10:32:15Z", "method": "GET", "path": "/health", "status": 200, "response_time": 5, "ip": "10.0.0.1", "user_agent": "health-check"}
```

## Basic Analysis

### 1. Response Status Distribution

```bash
ja groupby status access.jsonl | ja agg count
```

Output:

```json
{"status": 200, "count": 3}
{"status": 401, "count": 1}
{"status": 403, "count": 1}
```

### 2. Average Response Time by Status

```bash
ja groupby status access.jsonl | ja agg avg_response_time=avg(response_time),count
```

Output:

```json
{"status": 200, "avg_response_time": 39.0, "count": 3}
{"status": 401, "avg_response_time": 120.0, "count": 1}
{"status": 403, "avg_response_time": 23.0, "count": 1}
```

### 3. Error Rate

```bash
ja agg \
  total_requests=count, \
  error_requests=count_if(status>=400), \
  error_rate=count_if(status>=400)/count \
  access.jsonl
```

Output:

```json
{"total_requests": 5, "error_requests": 2, "error_rate": 0.4}
```

## Time-Based Analysis

### 4. Extract Time Components

```bash
ja project \
  timestamp, \
  method, \
  path, \
  status, \
  response_time, \
  hour=timestamp[11:13], \
  minute=timestamp[14:16] \
  access.jsonl
```

### 5. Requests per Hour

```bash
ja project timestamp,hour=timestamp[11:13],method,path,status access.jsonl \
  | ja groupby hour \
  | ja agg requests=count,avg_response_time=avg(response_time)
```

### 6. Peak Traffic Analysis

```bash
ja project timestamp,minute=timestamp[11:16],status,response_time access.jsonl \
  | ja groupby minute \
  | ja agg requests=count,errors=count_if(status>=400) \
  | ja sort requests --desc \
  | head -10
```

## Endpoint Analysis

### 7. Most Popular Endpoints

```bash
ja groupby path access.jsonl \
  | ja agg requests=count \
  | ja sort requests --desc
```

Output:

```json
{"path": "/api/users", "requests": 1}
{"path": "/api/users/123", "requests": 1}
{"path": "/api/login", "requests": 1}
{"path": "/api/users/456", "requests": 1}
{"path": "/health", "requests": 1}
```

### 8. Endpoint Performance

```bash
ja groupby path access.jsonl \
  | ja agg \
    requests=count, \
    avg_response_time=avg(response_time), \
    max_response_time=max(response_time), \
    error_rate=count_if(status>=400)/count \
  | ja sort avg_response_time --desc
```

### 9. API vs Health Checks

```bash
ja project path,status,response_time,is_api=path.startswith("/api") access.jsonl \
  | ja groupby is_api \
  | ja agg \
    requests=count, \
    avg_response_time=avg(response_time), \
    error_rate=count_if(status>=400)/count
```

## Multi-Dimensional Analysis

### 10. Method and Status Cross-Tabulation

```bash
ja groupby method access.jsonl \
  | ja groupby status \
  | ja agg count \
  | ja sort method,status
```

Output:

```json
{"method": "DELETE", "status": 403, "count": 1}
{"method": "GET", "status": 200, "count": 3}
{"method": "POST", "status": 401, "count": 1}
```

### 11. Hourly Error Analysis

```bash
ja project \
  timestamp, \
  hour=timestamp[11:13], \
  status, \
  path, \
  response_time \
  access.jsonl \
  | ja groupby hour \
  | ja groupby 'status>=400' \
  | ja agg count,paths=list(path) \
  | ja select 'status>=400 == true'
```

## Advanced Patterns

### 12. Slow Requests Analysis

```bash
# Define slow requests as > 50ms
ja select 'response_time > 50' access.jsonl \
  | ja groupby path \
  | ja agg \
    slow_requests=count, \
    avg_slow_time=avg(response_time), \
    max_time=max(response_time)
```

### 13. User Agent Analysis

```bash
ja project user_agent,status,path access.jsonl \
  | ja groupby user_agent \
  | ja agg \
    requests=count, \
    unique_paths=count_distinct(path), \
    error_rate=count_if(status>=400)/count \
  | ja sort requests --desc
```

### 14. IP Address Security Analysis

```bash
# Find IPs with high error rates
ja groupby ip access.jsonl \
  | ja agg \
    requests=count, \
    errors=count_if(status>=400), \
    error_rate=count_if(status>=400)/count \
  | ja select 'error_rate > 0.5 and requests > 1' \
  | ja sort error_rate --desc
```

## Real-World Scenarios

### 15. Performance Monitoring Dashboard

```bash
# Generate performance summary
ja project \
  timestamp, \
  hour=timestamp[11:13], \
  status, \
  response_time, \
  is_error=status>=400, \
  is_slow=response_time>100 \
  access.jsonl \
  | ja agg \
    total_requests=count, \
    avg_response_time=avg(response_time), \
    p95_response_time=percentile(response_time,0.95), \
    error_rate=sum(is_error)/count, \
    slow_rate=sum(is_slow)/count
```

### 16. Security Alert Detection

```bash
# Find suspicious patterns
ja select 'status == 401 or status == 403' access.jsonl \
  | ja groupby ip \
  | ja agg \
    failed_attempts=count, \
    unique_paths=count_distinct(path), \
    time_span=max(timestamp)-min(timestamp) \
  | ja select 'failed_attempts >= 3' \
  | ja sort failed_attempts --desc
```

### 17. API Rate Limiting Analysis

```bash
# Analyze request patterns per IP
ja project \
  ip, \
  timestamp, \
  minute=timestamp[0:16], \
  path \
  access.jsonl \
  | ja groupby ip \
  | ja groupby minute \
  | ja agg requests_per_minute=count \
  | ja select 'requests_per_minute > 10' \
  | ja groupby ip \
  | ja agg \
    peak_minutes=count, \
    max_rpm=max(requests_per_minute)
```

## Combining Multiple Log Sources

### 18. Join with Application Logs

**`app.jsonl`**:

```json
{"timestamp": "2024-01-15T10:30:45Z", "level": "INFO", "message": "User authenticated", "user_id": 123}
{"timestamp": "2024-01-15T10:31:12Z", "level": "WARN", "message": "Invalid credentials", "user_id": null}
{"timestamp": "2024-01-15T10:31:45Z", "level": "INFO", "message": "User data retrieved", "user_id": 123}
```

```bash
# Correlate access logs with application logs
ja join app.jsonl access.jsonl --on timestamp=timestamp \
  | ja project timestamp,method,path,status,level,message,user_id \
  | ja groupby level \
  | ja agg count,avg_response_time=avg(response_time)
```

### 19. Error Correlation Analysis

```bash
# Find patterns between HTTP errors and application errors
ja join app.jsonl access.jsonl --on timestamp=timestamp \
  | ja select 'status >= 400 or level == "ERROR"' \
  | ja groupby path \
  | ja agg \
    http_errors=count_if(status>=400), \
    app_errors=count_if(level=="ERROR"), \
    total_issues=count
```

## Time Series Analysis

### 20. Request Volume Trends

```bash
# Analyze request patterns over time
ja project \
  timestamp, \
  minute_bucket=timestamp[0:16], \
  status, \
  response_time \
  access.jsonl \
  | ja groupby minute_bucket \
  | ja agg \
    requests=count, \
    errors=count_if(status>=400), \
    avg_response_time=avg(response_time) \
  | ja sort minute_bucket
```

### 21. Anomaly Detection

```bash
# Find time periods with unusual patterns
ja project timestamp,minute=timestamp[0:16],status,response_time access.jsonl \
  | ja groupby minute \
  | ja agg \
    requests=count, \
    avg_response_time=avg(response_time), \
    error_rate=count_if(status>=400)/count \
  | ja project \
    minute, \
    requests, \
    avg_response_time, \
    error_rate, \
    is_anomaly='requests > 100 or avg_response_time > 200 or error_rate > 0.1' \
  | ja select 'is_anomaly == true'
```

## Export for Visualization

### 22. Prepare Data for Grafana/Charts

```bash
# Export time series data
ja project \
  timestamp, \
  hour=timestamp[11:13], \
  status, \
  response_time \
  access.jsonl \
  | ja groupby hour \
  | ja agg \
    requests=count, \
    avg_response_time=avg(response_time), \
    error_count=count_if(status>=400) \
  | ja export csv > hourly_metrics.csv
```

### 23. Create Status Code Distribution

```bash
# Format for pie chart
ja groupby status access.jsonl \
  | ja agg count \
  | ja project label=status,value=count \
  | ja export json
```

## Tips for Production Use

### Performance Optimization

1. **Filter Early**: Apply time range filters first
2. **Sample Large Datasets**: Use `head` for exploratory analysis
3. **Index Common Fields**: Consider pre-processing for frequently queried fields

```bash
# Efficient large log analysis
cat large_access.log.jsonl \
  | ja select 'timestamp > "2024-01-15T00:00:00Z"' \
  | ja select 'status >= 400' \
  | ja groupby path \
  | ja agg error_count=count
```

### Automation Scripts

Create reusable analysis scripts:

```bash
#!/bin/bash
# error_summary.sh
ja select 'status >= 400' $1 \
  | ja groupby status \
  | ja groupby path \
  | ja agg count \
  | ja sort count --desc
```

### Integration with Monitoring

```bash
# Real-time monitoring pipeline
tail -f /var/log/access.log \
  | ja select 'status >= 500' \
  | ja project timestamp,path,status,ip \
  | while read line; do
      echo "CRITICAL ERROR: $line" | send_alert
    done
```

## Next Steps

- [Performance Optimization](../advanced/performance.md) - Handle large log files efficiently
- [Format Conversion](../advanced/format-conversion.md) - Work with different log formats
- [Real-time Processing](../cookbook/etl-pipelines.md) - Build live monitoring systems
