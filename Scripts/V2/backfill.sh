python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install elasticsearch faker pyyaml
python3  /home/elastic/Machine_Learning_Workshop/backfill_all-MLv2-WORKSHOP.py \
        --host https://es.elastic.lab:443 \
        --user sdg --password changeme \
        --days 30 \
        --no-verify-ssl \
        --sdg-bulk 7500  \
        --apm-bulk 2500 \
        --no-then-run --timezone "UTC" \
        --spike-count 3 --spike-volume-mult 2 --spike-duration-hrs 1 \
        --spike-error-rate 0.3 --spike-latency-mult 2 --spike-cap-override \
        --skip-ml
