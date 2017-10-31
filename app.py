from flask import Flask, request
from flask_restplus import Resource, Api, reqparse
import requests

app = Flask(__name__)
api = Api(app)

interactions_ns = api.namespace('interactions', "Queries for interactions")

base_url = 'http://biggim.ncats.io/api'
todos = {}

#a couple of simple helper functions
def postBG(endpoint, data={}, base_url=base_url):
    req = requests.post('%s/%s' % (base_url,endpoint), data=data)
    req.raise_for_status()
    return req.json()

def getBG(endpoint, data={}, base_url=base_url):
    req = requests.get('%s/%s' % (base_url,endpoint), data=data)
    print(req.url)
    req.raise_for_status()
    return req.json()


@api.route('/metadata')
class MetaDataStudy(Resource):
    def get(Request):
        try:
            studies = getBG(endpoint='metadata/study', data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return studies

@api.route('/tissue')
class Tissues(Resource):
    def get(Request):
        try:
            studies = getBG(endpoint='metadata/tissue', data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return studies


@api.route('/tissue/<string:tissue_name>')
class SingleTissue(Resource):
    def get(self, tissue_name):
        try:
            endpoint = 'metadata/tissue/%s' % (tissue_name)
            single_tissue = getBG(endpoint=endpoint, data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return single_tissue

##########
# GET /interactions
##########

@interactions_ns.route('/query')
@interactions_ns.param('table', 'The table to select from.', default='BigGIM_70_v1', required=True)
@interactions_ns.param('columns', 'A comma delimited list of column names to return',
                       default='all columns', required=True)
@interactions_ns.param('ids1', 'A comma delimited list of Entrez gene ids to select',
                       default='all genes', required=True)
@interactions_ns.param('ids2', 'Entrez gene ids to select: If not given, the query selects any '
                               'gene related to a gene in ids 1. If given, the query only selects '
                               'relations that contain a gene in ids1 and a gene in ids2.',
                       default='all genes', required=True)
@interactions_ns.param('restriction_bool', 'A list of pairs of values column name,value with which to '
                                           'restrict the results of the query to rows where the value of'
                                           ' the column is True or False',
                       default='No restrictions', required=True)
@interactions_ns.param('restriction_lt', 'A list of pairs of values column name,value with which to '
                                         'restrict the results of the query to rows where the value '
                                         'of the column is less than the given value.',
                       default='No restrictions', required=True)
@interactions_ns.param('restriction_gt', 'A list of pairs of values column name,value with which to '
                                         'restrict the results of the query to rows where the value '
                                         'of the column is greater than the given value.',
                       default='No restrictions', required=True)
@interactions_ns.param('restriction_join', 'The type of join made on restrictions. Either intersect or union',
                       default='intersect', required=True)
@interactions_ns.param('limit', 'The maximum number of rows to return',
                       default='10000', required=True)

# example query url http://127.0.0.1:5000/interactions/query?columns=TCGA_GBM_Correlation,TCGA_GBM_Pvalue,GTEx_Brain_Correlation,GTEx_Brain_Pvalue&ids1=5111,6996,57697,6815,889,7112,2176,1019,5888,5706,5722,1111,112,3333&ids2=5111,6996,57697,6815,889,7112,2176,1019,5888,5706,3333,1111,112,3333&limit=10000&restriction_gt=TCGA_GBM_Correlation,.2,%20GTEx_Brain_Correlation,.2&restriction_join=union&restriction_lt=TCGA_GBM_Pvalue,.05,%20GTEx_Brain_Pvalue,.01&table=BigGIM_70_v1
# Currently returns the uri for an interactions table.csv
class GetInteractionsQuery(Resource):
    def get(self):
        try:
            query_submit = getBG('interactions/query', base_url=base_url, data=request.args)
        except Exception as e:
            print(e)
        import time
        try:
            while True:
                query_status = getBG(endpoint='interactions/query/status/%s' % (query_submit['request_id'],),
                                     base_url=base_url, data={})
                if query_status['status'] != 'running':
                    # query has finished
                    break
                else:
                    time.sleep(1)
        except requests.HTTPError as e:
            print(e)
        # try:
        #     final_table = getBG(endpoint=query_status['request_uri'],)
        # except Exception as e:
        #     print(e)

        return query_status['request_uri'],


if __name__ == '__main__':
    app.run(debug=True)

