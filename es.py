import elasticsearch
from elasticsearch import Elasticsearch
import json
import pandas as pd

# Create the client instance
es = Elasticsearch(
    ["https://satu-production.es.ap-southeast-1.aws.found.io:9243"],
    basic_auth=("satusystem", "sat@sia2021"),
)

# Successful response!
# print(es.ping())
# print(es.info())
# {'name': 'instance-0000000000', 'cluster_name': ...},.l                                                                                                                                                      k
# res = es.indices.get_alias(index="*")
# print(list(dict(res).keys()))


index_name = "sda_alerts"
try:
    response = es.search(index=index_name)
    # print(dict(response).keys())
    # data = json.dumps(response['hits']['hits'],indent=4)
    data = response['hits']['hits'] # list

except Exception as e:
    print(str(e))



# print(pd.DataFrame(data).columns)



data_source = []

for i in data:
    # print(json.dumps(i['_source'],indent=4))
    data_source.append(i['_source'])

print(pd.DataFrame(data_source).columns)
# print(pd.DataFrame(data_source)[['tenant_id','external_id']].head())

# result = es.search(
#  index='lord-of-the-rings',
#   query={
#     'match': {'quote': 'late'}
#   }
#  )