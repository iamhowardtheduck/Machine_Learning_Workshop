python /home/elastic/Machine_Learning_Workshop/run_workshop.py \
  --host https://es.elastic.lab:443 \
  --user sdg --password changeme \
  --no-verify-ssl \
  --sdg-config /workspace/workshop/Machine_Learning_Workshop/mortgage-workshop.yml \
  --anomaly-chance 0.07 \
  --status-interval 3
