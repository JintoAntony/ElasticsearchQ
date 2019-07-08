from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl.connections import connections
from rest_framework.response import Response
from rest_framework.views import APIView

# Define a default Elasticsearch client
connections.create_connection(hosts=['localhost'])


class BulkInsertion(APIView):
    def get(self, request):
        print(request.data)
        data_json = request.data
        # # data_json = {
        # #     "country": "india",
        # #     "state": "tamilnadu",
        # #     "year": "2019-2020",
        # # }
        #
        es = Elasticsearch()
        actions = [
            {
                "_index": "index_name",
                "_type": "_doc",
                "_source": item
            }
            for item in data_json
        ]
        helpers.bulk(es, actions)

        return Response('Bulk Insertion completed');
