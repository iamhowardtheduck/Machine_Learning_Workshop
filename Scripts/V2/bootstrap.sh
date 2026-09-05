pip install faker elasticsearch --break-system-packages 
python /home/elastic/Machine_Learning_Workshop/bootstrap.py \
  --host http://es.elastic.lab:443 \
  --user sdg \
  --password changeme \
  --kibana-host http://kb.elastic.lab:443 \
  --no-verify-ssl \
  --skip-ml
