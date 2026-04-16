pip install faker elasticsearch --break-system-packages 
python /workspace/workshop/Machine-Learning-Workshop/bootstrap.py \
  --host http://kubernetes-vm:30920 \
  --user sdg \
  --password changeme \
  --kibana-host http://kubernetes-vm:30002 \
  --no-verify-ssl \
  --skip-ml
