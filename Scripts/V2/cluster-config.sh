# Set up environment variables
echo 'ELASTICSEARCH_USERNAME=elastic' >> /home/elastic/Machine_Learning_Workshop/.env
#echo -n 'ELASTICSEARCH_PASSWORD=' >> /root/.env
kubectl get secret elasticsearch-es-elastic-user -n default -o go-template='ELASTICSEARCH_PASSWORD={{.data.elastic | base64decode}}' >> /home/elastic/Machine_Learning_Workshop/.env
echo '' >> /home/elastic/Machine_Learning_Workshop/.env
echo 'ELASTICSEARCH_URL="https://es.elastic.lab:443"' >> /home/elastic/Machine_Learning_Workshop/.env
echo 'KIBANA_URL="https://kb.elastic.lab:443"' >> /home/elastic/Machine_Learning_Workshop/.env
echo 'BUILD_NUMBER="10"' >> /home/elastic/Machine_Learning_Workshop/.env
echo 'ELASTIC_VERSION="9.1.0"' >> /home/elastic/Machine_Learning_Workshop/.env


# Set up environment
export $(cat /home/elastic/Machine_Learning_Workshop/.env | xargs)

BASE64=$(echo -n "elastic:${ELASTICSEARCH_PASSWORD}" | base64)
KIBANA_URL_WITHOUT_PROTOCOL=$(echo $KIBANA_URL | sed -e 's#http[s]\?://##g')

# Add sdg user with superuser role
curl -X POST "https://es.elastic.lab:443/_security/user/sdg" -H "Content-Type: application/json" -u "elastic:${ELASTICSEARCH_PASSWORD}" -d '{
  "password" : "changeme",
  "roles" : [ "superuser" ],
  "full_name" : "SDG User",
  "email" : "sdg@elastic-pahlsoft.com"
}'


echo
echo "You are now ready to move onto the next set of instructions."
echo
