#!/bin/bash
# fix_geo_datafeed.sh
# Deletes and recreates datafeed-mortgage-geo-login with the correct index (traces-apm-default)

HOST="https://mortgage-ml-workshop-139e6f.es.us-east4.gcp.elastic-cloud.com"
AUTH="peter:flt72100"
OPTS="--insecure -sk"

echo "=== Step 1: Force stop datafeed ==="
curl $OPTS -X POST -u $AUTH \
  "$HOST/_ml/datafeeds/datafeed-mortgage-geo-login/_stop?force=true" \
  | python3 -m json.tool
echo
 
echo "=== Step 2: Close job ==="
curl $OPTS -X POST -u $AUTH \
  "$HOST/_ml/anomaly_detectors/mortgage-geo-login-anomaly/_close?force=true" \
  | python3 -m json.tool
echo
 
echo "=== Step 3: Delete datafeed ==="
curl $OPTS -X DELETE -u $AUTH \
  "$HOST/_ml/datafeeds/datafeed-mortgage-geo-login?force=true" \
  | python3 -m json.tool
echo
 
echo "=== Step 4: Verify datafeed is gone ==="
STATUS=$(curl $OPTS -o /dev/null -w "%{http_code}" -u $AUTH \
  "$HOST/_ml/datafeeds/datafeed-mortgage-geo-login")
echo "HTTP status: $STATUS"
if [ "$STATUS" != "404" ]; then
  echo "ERROR: Datafeed still exists (HTTP $STATUS). Aborting."
  exit 1
fi
echo "Datafeed confirmed deleted."
echo
 
echo "=== Step 5: Recreate datafeed ==="
curl $OPTS -X PUT -u $AUTH \
  "$HOST/_ml/datafeeds/datafeed-mortgage-geo-login" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "mortgage-geo-login-anomaly",
    "indices": ["traces-apm-default"],
    "query": {
      "bool": {
        "must": [
          {"exists": {"field": "client.geo.location"}},
          {"exists": {"field": "user.id"}}
        ]
      }
    }
  }' | python3 -m json.tool
echo
 
echo "=== Step 6: Open job ==="
curl $OPTS -X POST -u $AUTH \
  "$HOST/_ml/anomaly_detectors/mortgage-geo-login-anomaly/_open" \
  -H "Content-Type: application/json" \
  -d '{"timeout": "30s"}' \
  | python3 -m json.tool
echo
 
echo "=== Step 7: Start datafeed ==="
curl $OPTS -X POST -u $AUTH \
  "$HOST/_ml/datafeeds/datafeed-mortgage-geo-login/_start" \
  | python3 -m json.tool
echo
 
echo "=== Step 8: Verify running ==="
curl $OPTS -u $AUTH \
  "$HOST/_ml/datafeeds/datafeed-mortgage-geo-login/_stats" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
df = d.get('datafeeds', [{}])[0]
print(f'  state:   {df.get(\"state\",\"?\")}')
print(f'  indices: {df.get(\"indices\", [\"?\"])}')
"
 
