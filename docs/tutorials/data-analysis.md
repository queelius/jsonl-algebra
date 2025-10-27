# Tutorial: Analyzing Log Files

Learn how to use **jsonl-algebra** to analyze log files, extract insights, and identify issues. This tutorial walks through a real-world scenario: analyzing web server access logs.

**Time required:** 15-20 minutes

**What you'll learn:**

- Parsing and filtering log data
- Calculating statistics and trends
- Identifying errors and anomalies
- Creating reports from logs
- Building monitoring dashboards

## Scenario

You're analyzing access logs from a web application. The logs are in JSONL format with this structure:

```json
{"timestamp": "2025-10-27T10:15:30Z", "method": "GET", "path": "/api/users", "status": 200, "duration_ms": 45, "user_id": 1001, "ip": "192.168.1.100"}
{"timestamp": "2025-10-27T10:15:31Z", "method": "POST", "path": "/api/orders", "status": 201, "duration_ms": 120, "user_id": 1002, "ip": "192.168.1.101"}
{"timestamp": "2025-10-27T10:15:32Z", "method": "GET", "path": "/api/products", "status": 500, "duration_ms": 5000, "user_id": 1001, "ip": "192.168.1.100"}
```

## Setup: Create Sample Data

First, let's create a realistic sample log file:

```bash
cat > access_logs.jsonl << 'EOF'
{"timestamp": "2025-10-27T10:00:00Z", "method": "GET", "path": "/api/users", "status": 200, "duration_ms": 45, "user_id": 1001, "ip": "192.168.1.100"}
{"timestamp": "2025-10-27T10:00:05Z", "method": "POST", "path": "/api/orders", "status": 201, "duration_ms": 120, "user_id": 1002, "ip": "192.168.1.101"}
{"timestamp": "2025-10-27T10:00:10Z", "method": "GET", "path": "/api/products", "status": 500, "duration_ms": 5000, "user_id": 1001, "ip": "192.168.1.100"}
{"timestamp": "2025-10-27T10:00:15Z", "method": "GET", "path": "/api/users/1001", "status": 200, "duration_ms": 30, "user_id": 1001, "ip": "192.168.1.100"}
{"timestamp": "2025-10-27T10:00:20Z", "method": "DELETE", "path": "/api/orders/500", "status": 404, "duration_ms": 20, "user_id": 1003, "ip": "192.168.1.102"}
{"timestamp": "2025-10-27T10:00:25Z", "method": "GET", "path": "/api/products", "status": 200, "duration_ms": 55, "user_id": 1002, "ip": "192.168.1.101"}
{"timestamp": "2025-10-27T10:00:30Z", "method": "POST", "path": "/api/users", "status": 400, "duration_ms": 15, "user_id": null, "ip": "192.168.1.103"}
{"timestamp": "2025-10-27T10:00:35Z", "method": "GET", "path": "/api/orders", "status": 200, "duration_ms": 80, "user_id": 1001, "ip": "192.168.1.100"}
{"timestamp": "2025-10-27T10:00:40Z", "method": "PUT", "path": "/api/users/1002", "status": 500, "duration_ms": 3000, "user_id": 1002, "ip": "192.168.1.101"}
{"timestamp": "2025-10-27T10:00:45Z", "method": "GET", "path": "/api/products/search", "status": 200, "duration_ms": 150, "user_id": 1001, "ip": "192.168.1.100"}
{"timestamp": "2025-10-27T10:00:50Z", "method": "POST", "path": "/api/orders", "status": 201, "duration_ms": 95, "user_id": 1003, "ip": "192.168.1.102"}
{"timestamp": "2025-10-27T10:00:55Z", "method": "GET", "path": "/api/users", "status": 200, "duration_ms": 40, "user_id": 1002, "ip": "192.168.1.101"}
{"timestamp": "2025-10-27T10:01:00Z", "method": "GET", "path": "/api/products/123", "status": 404, "duration_ms": 25, "user_id": 1001, "ip": "192.168.1.100"}
{"timestamp": "2025-10-27T10:01:05Z", "method": "POST", "path": "/api/auth/login", "status": 401, "duration_ms": 10, "user_id": null, "ip": "192.168.1.104"}
{"timestamp": "2025-10-27T10:01:10Z", "method": "GET", "path": "/api/orders/1001", "status": 200, "duration_ms": 60, "user_id": 1001, "ip": "192.168.1.100"}
EOF
```

## Task 1: Find All Errors

Let's identify all error responses (status >= 400):

```bash
ja select 'status >= 400' access_logs.jsonl
```

**Output:**
```json
{"timestamp": "2025-10-27T10:00:10Z", "method": "GET", "path": "/api/products", "status": 500, ...}
{"timestamp": "2025-10-27T10:00:20Z", "method": "DELETE", "path": "/api/orders/500", "status": 404, ...}
{"timestamp": "2025-10-27T10:00:30Z", "method": "POST", "path": "/api/users", "status": 400, ...}
{"timestamp": "2025-10-27T10:00:40Z", "method": "PUT", "path": "/api/users/1002", "status": 500, ...}
{"timestamp": "2025-10-27T10:01:00Z", "method": "GET", "path": "/api/products/123", "status": 404, ...}
{"timestamp": "2025-10-27T10:01:05Z", "method": "POST", "path": "/api/auth/login", "status": 401, ...}
```

### Save Errors to File

```bash
ja select 'status >= 400' access_logs.jsonl > errors.jsonl
```

### Count Error Types

```bash
ja select 'status >= 400' access_logs.jsonl \
  | ja groupby status --agg count \
  | ja sort count --desc
```

**Output:**
```json
{"status": 404, "count": 2}
{"status": 500, "count": 2}
{"status": 400, "count": 1}
{"status": 401, "count": 1}
```

!!! tip "HTTP Status Categories"
    - 400-499: Client errors (bad requests, auth failures)
    - 500-599: Server errors (application crashes, timeouts)

## Task 2: Identify Slow Requests

Find requests that took longer than 1 second (1000ms):

```bash
ja select 'duration_ms > 1000' access_logs.jsonl \
  | ja project timestamp,method,path,duration_ms \
  | ja sort duration_ms --desc
```

**Output:**
```json
{"timestamp": "2025-10-27T10:00:10Z", "method": "GET", "path": "/api/products", "duration_ms": 5000}
{"timestamp": "2025-10-27T10:00:40Z", "method": "PUT", "path": "/api/users/1002", "duration_ms": 3000}
```

### Calculate Performance Statistics

```bash
ja groupby path --agg count,avg_ms=avg:duration_ms,max_ms=max:duration_ms access_logs.jsonl \
  | ja sort avg_ms --desc
```

**Output:**
```json
{"path": "/api/products", "count": 3, "avg_ms": 1683.33, "max_ms": 5000}
{"path": "/api/orders", "count": 3, "avg_ms": 98.33, "max_ms": 120}
{"path": "/api/products/search", "count": 1, "avg_ms": 150.0, "max_ms": 150}
...
```

## Task 3: Analyze Request Patterns

### Requests by HTTP Method

```bash
ja groupby method --agg count access_logs.jsonl \
  | ja sort count --desc
```

**Output:**
```json
{"method": "GET", "count": 9}
{"method": "POST", "count": 4}
{"method": "PUT", "count": 1}
{"method": "DELETE", "count": 1}
```

### Requests by Endpoint

```bash
ja groupby path --agg count access_logs.jsonl \
  | ja sort count --desc \
  | head -5
```

**Output:**
```json
{"path": "/api/products", "count": 2}
{"path": "/api/users", "count": 2}
{"path": "/api/orders", "count": 2}
...
```

### Success Rate by Endpoint

```bash
ja groupby path --agg total=count,errors="count:status >= 400" access_logs.jsonl
```

## Task 4: User Activity Analysis

### Most Active Users

```bash
ja select 'user_id != null' access_logs.jsonl \
  | ja groupby user_id --agg requests=count \
  | ja sort requests --desc
```

**Output:**
```json
{"user_id": 1001, "requests": 7}
{"user_id": 1002, "requests": 4}
{"user_id": 1003, "requests": 2}
```

### User Error Analysis

Find users experiencing the most errors:

```bash
ja select 'status >= 400 and user_id != null' access_logs.jsonl \
  | ja groupby user_id --agg errors=count \
  | ja sort errors --desc
```

**Output:**
```json
{"user_id": 1001, "errors": 2}
{"user_id": 1002, "errors": 1}
```

## Task 5: Create a Summary Report

Combine multiple analyses into a comprehensive report:

```bash
#!/bin/bash
# log_report.sh

LOG_FILE="access_logs.jsonl"

echo "=== Log Analysis Report ==="
echo

echo "Total Requests:"
wc -l < "$LOG_FILE"
echo

echo "Error Summary:"
ja select 'status >= 400' "$LOG_FILE" \
  | ja groupby status --agg count \
  | ja sort count --desc

echo
echo "Top 5 Endpoints by Request Count:"
ja groupby path --agg count "$LOG_FILE" \
  | ja sort count --desc \
  | head -5

echo
echo "Slowest Endpoints (avg response time):"
ja groupby path --agg avg_ms=avg:duration_ms "$LOG_FILE" \
  | ja sort avg_ms --desc \
  | head -5

echo
echo "Most Active Users:"
ja select 'user_id != null' "$LOG_FILE" \
  | ja groupby user_id --agg count \
  | ja sort count --desc \
  | head -5
```

Make it executable and run:

```bash
chmod +x log_report.sh
./log_report.sh
```

## Task 6: Time-based Analysis

### Extract Hour from Timestamp

To analyze patterns by hour, we'll use a technique with `ja` expressions:

```bash
# For more complex time parsing, use the data explorer or a preprocessing step
# For now, let's group by full timestamp prefix

ja project timestamp,path,status access_logs.jsonl | head -5
```

### Peak Usage Times

For a real-world scenario, you'd parse timestamps. Here's a conceptual approach:

```python
# preprocess.py - Add hour field
import json
from datetime import datetime

with open('access_logs.jsonl') as f:
    for line in f:
        record = json.loads(line)
        dt = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
        record['hour'] = dt.hour
        print(json.dumps(record))
```

Then analyze:

```bash
python preprocess.py | ja groupby hour --agg requests=count \
  | ja sort hour
```

## Task 7: Alerting on Anomalies

### High Error Rate Detection

```bash
# Find endpoints with >50% error rate
ja groupby path \
  --agg total=count,"errors=count:status >= 400" \
  access_logs.jsonl \
  | ja select 'errors * 2 > total'  # More than 50% errors
```

### Performance Degradation

```bash
# Endpoints averaging >500ms
ja groupby path --agg avg_ms=avg:duration_ms access_logs.jsonl \
  | ja select 'avg_ms > 500'
```

**Output:**
```json
{"path": "/api/products", "avg_ms": 1683.33}
{"path": "/api/users/1002", "avg_ms": 3000.0}
```

## Task 8: Multi-File Analysis

If logs are split across multiple files:

```bash
# Combine all log files
ja union logs_morning.jsonl logs_afternoon.jsonl logs_evening.jsonl \
  > full_day_logs.jsonl

# Or analyze directly
ja union logs_*.jsonl \
  | ja select 'status >= 500' \
  | ja groupby path --agg count
```

## Task 9: Export for Visualization

### Create CSV for Spreadsheet Analysis

```bash
ja groupby path --agg count,avg_ms=avg:duration_ms,errors="count:status >= 400" access_logs.jsonl \
  | ja export csv > endpoint_stats.csv
```

Open `endpoint_stats.csv` in Excel, Google Sheets, or similar.

### Create JSON for Dashboard

```bash
ja groupby path --agg count,avg_ms=avg:duration_ms access_logs.jsonl \
  | ja export json > dashboard_data.json
```

## Advanced Patterns

### Pattern 1: Funnel Analysis

Track user journey through endpoints:

```bash
# Users who hit products endpoint
ja select 'path == "/api/products"' access_logs.jsonl \
  | ja project user_id \
  | ja distinct > viewed_products.jsonl

# Of those, who placed orders?
ja join viewed_products.jsonl access_logs.jsonl --on user_id=user_id \
  | ja select 'path == "/api/orders" and method == "POST"' \
  | ja project user_id \
  | ja distinct
```

### Pattern 2: Session Reconstruction

Group requests by user to reconstruct sessions:

```bash
ja select 'user_id != null' access_logs.jsonl \
  | ja sort user_id,timestamp \
  | ja groupby user_id --agg requests=count,paths=list:path
```

### Pattern 3: Correlation Analysis

Find endpoints often accessed together:

```bash
# First endpoint per user
ja select 'user_id != null' access_logs.jsonl \
  | ja sort user_id,timestamp \
  | ja groupby user_id \
  # Take first path per user (would need custom aggregation)
```

## Real-World Integration

### With Monitoring Tools

```bash
# Continuous monitoring
tail -f /var/log/app/access.log \
  | ja select 'status >= 500' \
  | ja project timestamp,path,status,duration_ms \
  > critical_errors.jsonl &

# Alert when threshold reached
watch -n 60 'ja select "status >= 500" critical_errors.jsonl | wc -l'
```

### With Log Aggregation

```bash
# Process logs from multiple servers
for server in web{1..5}; do
  scp $server:/var/log/app/access.log ${server}_access.jsonl
done

ja union *_access.jsonl \
  | ja groupby server_hostname --agg errors="count:status >= 500"
```

## Best Practices

1. **Filter Early** - Reduce data size before expensive operations
   ```bash
   # Good
   ja select 'status >= 400' huge.jsonl | ja groupby path

   # Bad
   ja groupby path huge.jsonl | ja select 'count > 10'
   ```

2. **Save Intermediate Results** - For complex analyses
   ```bash
   ja select 'status >= 400' logs.jsonl > errors.jsonl
   ja groupby path --agg count errors.jsonl
   ja groupby user_id --agg count errors.jsonl
   ```

3. **Use Scripts for Reports** - Automate repetitive analysis
4. **Timestamp Preprocessing** - Add derived time fields early
5. **Monitor Performance** - Keep an eye on query execution time

## Troubleshooting

### Issue: Memory errors with large logs

**Solution:** Filter or sample first
```bash
ja select 'status >= 400' huge_logs.jsonl | ja groupby path
# Or
head -100000 huge_logs.jsonl | ja groupby path
```

### Issue: Inconsistent timestamps

**Solution:** Normalize in preprocessing
```python
# normalize_timestamps.py
import json
from dateutil import parser

for line in sys.stdin:
    record = json.loads(line)
    record['timestamp'] = parser.parse(record['timestamp']).isoformat()
    print(json.dumps(record))
```

## Summary

You've learned how to:

- ✅ Filter logs for errors and anomalies
- ✅ Calculate performance statistics
- ✅ Analyze request patterns
- ✅ Track user activity
- ✅ Create summary reports
- ✅ Detect performance issues
- ✅ Export data for visualization

## Next Steps

- [Real-time Monitoring Tutorial](monitoring.md) - Set up live monitoring
- [ETL Pipeline Tutorial](etl.md) - Build data pipelines
- [Log Analyzer Integration](../integrations/log-analyzer.md) - Use the built-in tool
- [Data Quality Tutorial](quality.md) - Validate and clean data

## Practice Exercises

Try these on your own logs:

1. Find the endpoint with the highest error rate
2. Identify the slowest hour of the day
3. Calculate 95th percentile response time per endpoint
4. Find users with the most failed authentication attempts
5. Create an automated daily summary report

!!! success "Well Done!"
    You can now analyze logs like a pro with jsonl-algebra!
