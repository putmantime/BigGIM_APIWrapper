from flask import Flask, request
from flask_restplus import Resource, Api, reqparse
import requests
import time

app = Flask(__name__)
api = Api(app)

# instantiate namespaces
interactions_ns = api.namespace('interactions', "Queries for interactions")
tissue_ns = api.namespace('tissue', "Queries for tissue")
metadata_ns = api.namespace('metadata', 'Queries for study metadata')
base_url = 'http://biggim.ncats.io/api'
todos = {}

#a couple of simple helper functions
def postBG(endpoint, data={}, base_url=base_url):
    req = requests.post('%s/%s' % (base_url,endpoint), json=data)
    req.raise_for_status()
    return req.json()

def getBG(endpoint, data={}, base_url=base_url):
    req = requests.get('%s/%s' % (base_url,endpoint), data=data)
    print(req.url)
    req.raise_for_status()
    return req.json()

##########
#  /metadata
##########
@metadata_ns.route('/openapiv3')
class MetaDataStudy(Resource):
    def get(Request):
        try:
            studies = getBG(endpoint='metadata/openapiv3', data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return studies

@metadata_ns.route('/study')
class MetaDataStudy(Resource):
    def get(Request):
        try:
            studies = getBG(endpoint='metadata/study', data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return studies

@metadata_ns.route('/study/<string:study_name>')
class SingleStudy(Resource):
    def get(self, study_name):
        try:
            endpoint = 'metadata/study/%s' % (study_name)
            study_meta = getBG(endpoint=endpoint, data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return study_meta
##########
#  /tissue
##########

@tissue_ns.route('/')
class Tissues(Resource):
    def get(Request):
        try:
            studies = getBG(endpoint='metadata/tissue', data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return studies


@tissue_ns.route('/<string:tissue_name>')
class SingleTissue(Resource):
    def get(self, tissue_name):
        try:
            endpoint = 'metadata/tissue/%s' % (tissue_name)
            single_tissue = getBG(endpoint=endpoint, data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return single_tissue

##########
#  /interactions
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
    def post(self):
        # query_submit = None
        try:
            query_submit = postBG(endpoint='interactions/query', base_url=base_url, data=dict(request.args))
        except Exception as e:
            print(e)
        query_status = self.get_query_status(query_key=query_submit['request_id'])
        return query_status['request_uri']

    def get(self):
        # query_submit = None
        try:
            query_submit = getBG('interactions/query', base_url=base_url, data=request.args)
        except Exception as e:
            print(e)
        query_status = self.get_query_status(query_key=query_submit['request_id'])
        return query_status['request_uri']

    def get_query_status(self, query_key):
        """
        use the query key from initial interactions/query request to return uri for interactions csv
        :param query_key:
        :return:
        """
        try:
            while True:
                query_status = getBG(endpoint='interactions/query/status/%s' % (query_key),
                                     base_url=base_url, data={})
                if query_status['status'] != 'running':
                    # query has finished
                    return query_status
                else:
                    time.sleep(1)
        except requests.HTTPError as e:
            print(e)


if __name__ == '__main__':
    app.run(debug=True)

