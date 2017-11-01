from flask import Flask, request
from flask_restplus import Resource, Api
import requests
import time

app = Flask(__name__)
api = Api(app)

# instantiate namespaces
interactions_ns = api.namespace('interactions', "Queries for interactions")
metadata_ns = api.namespace('metadata', 'Queries for study metadata')

base_url = 'http://biggim.ncats.io/api'

# http request methods
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


@metadata_ns.route('/swagger')
class MetaDataSwagger(Resource):
    def get(Request):
        try:
            swagger = getBG(endpoint='metadata/swagger', data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return swagger


@metadata_ns.route('/table')
class MetaDataTable(Resource):
    def get(Request):
        try:
            table_result = getBG(endpoint='metadata/table', data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return table_result


@metadata_ns.route('/table/<string:table_name>')
class SingleTable(Resource):
    def get(self, table_name):
        try:
            endpoint = 'metadata/table/%s' % (table_name)
            table_meta = getBG(endpoint=endpoint, data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return table_meta


@metadata_ns.route('/')
class Tissues(Resource):
    def get(Request):
        try:
            studies = getBG(endpoint='metadata/tissue', data={}, base_url=base_url)
        except requests.HTTPError as e:
            print(e)
        return studies


@metadata_ns.route('/<string:tissue_name>')
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

# TODO Finalize return; Currently returns the uri for an interactions table.csv
class GetInteractionsQuery(Resource):
    def post(self):
        try:
            query_submit = postBG(endpoint='interactions/query', base_url=base_url, data=request.args)
        except Exception as e:
            print(e)
        query_status = self.get_query_status(query_key=query_submit['request_id'])
        return query_status['request_uri']

    def get(self):
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
                    break
                else:
                    time.sleep(1)
        except requests.HTTPError as e:
            print(e)
        return query_status

if __name__ == '__main__':
    app.run(debug=True)

