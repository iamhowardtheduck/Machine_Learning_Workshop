python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install elasticsearch faker pyyaml
#pip install faker elasticsearch --break-system-packages 
python3 /home/elastic/Machine_Learning_Workshop/bootstrap.py \
  --host https://es.elastic.lab:443 \
  --user sdg \
  --password changeme \
  --kibana-host https://kb.elastic.lab:443 \
  --no-verify-ssl \
  --skip-ml
