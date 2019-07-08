import requests
from elasticsearch_dsl.connections import connections
from rest_framework.response import Response
from rest_framework.views import APIView

# Define a default Elasticsearch client
connections.create_connection(hosts=['localhost'])


class BulkInsertion(APIView):
    def post(self, request):
        client = Elasticsearch()

        # data_dict = {}
        #
        # df_ = pd.read_csv("/home/programmer/Desktop/sample_data.csv")
        # columns = list(df_.columns.values)
        # for index, row in df_.iterrows():
        #     for column in columns:
        #         if isinstance(row[column], str):
        #             data_dict[column] = row[column].strip()
        #         else:
        #             data_dict[column] = row[column]
        #     with open("sample_data.json", 'a') as json_file:
        #         json.dump({"index": {"_index": "sample_data"}}, json_file)
        #         json_file.write("\n")
        #         json.dump(data_dict, json_file)
        #         json_file.write("\n")

        #### Index creation and indexing
        from elasticsearch import Elasticsearch
        es = Elasticsearch()

        NUMBER_OF_SHARDS = 3
        NUMBER_OF_REPLICAS = 2

        mapping_file = "mapping.json"
        data_file = "sample_data.json"

        with open(mapping_file, "rb") as f:
            mapping = json.load(f)
        request_body = {
            "settings": {
                "number_of_shards": NUMBER_OF_SHARDS,
                "number_of_replicas": NUMBER_OF_REPLICAS
            },
            "mappings": mapping
        }

        data_file = open(data_file, "rb").read()
        print("creating...")
        es.indices.create(index="sample_data", body=request_body)
        headers = {"Content-Type": "application/x-ndjson"}
        res = requests.post(url="http://localhost:9200/_bulk", data=data_file, headers=headers)
        print(res.status_code)
        print(res.content)

        return Response('Bulk Insertion completed');
