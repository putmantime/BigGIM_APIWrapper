from flask import Flask, request
from flask_restplus import Resource, Api
import requests
import time
import json
import pandas as pd
from pprint import pprint

app = Flask(__name__)


description = """ An NCATS Translator SMARTAPI compliant API wrapper for the 
BigGIM (Gene Interaction Miner) API http://biggim.ncats.io/api
"""
api = Api(app, description=description)


# instantiate namespaces
interactions_ns = api.namespace('interactions', "Mine the interaction profiles of various entities")
metadata_ns = api.namespace('metadata', 'Access the metadata for available datasets')

base_url = 'http://biggim.ncats.io/api'

# map of bto -> uberon -> bg terms
uberon_bto_map = json.loads(open ('bto_uberon_bg.json').read())

# map of columns to metadata objects
meta_columns = json.loads(open ('bg_column_map.json').read())

def id2term(var, key, return_key, json_blob):
    """
    check json map for value
    :param var: identifier
    :param key: source of identifier
    :param return_key: key to return in matched object
    :param json_blob: json to search
    :return: bg term name if mapping to that id exists, input var if no mapping exists
    """
    result = var
    for obj in json_blob:
        if obj[key] == var:
            result = obj[return_key]
    return result


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
    """
    Return the OpenAPI v3 spec for this API
    """
    def get(Request):
        try:
            studies = getBG(endpoint='metadata/openapiv3', data={}, base_url=base_url)
            return studies
        except requests.HTTPError as e:
            return {
                'error': str(e)
            }


@metadata_ns.route('/study')
class MetaDataStudy(Resource):
    """
    Return all available studies
    """
    def get(Request):
        try:
            studies = getBG(endpoint='metadata/study', data={}, base_url=base_url)
            return studies
        except requests.HTTPError as e:
            return {
                'error': str(e)
            }


@metadata_ns.route('/study/<string:study_name>')
class SingleStudy(Resource):
    """
    Return a single study and associated substudies
    """
    def get(self, study_name):
        try:
            endpoint = 'metadata/study/%s' % (study_name)
            study_meta = getBG(endpoint=endpoint, data={}, base_url=base_url)
            return study_meta
        except requests.HTTPError as e:
            return {
                'error': str(e)
            }


@metadata_ns.route('/swagger')
class MetaDataSwagger(Resource):
    """
    Return the swagger v2 spec for this API
    """
    def get(Request):
        try:
            swagger = getBG(endpoint='metadata/swagger', data={}, base_url=base_url)
            return swagger
        except requests.HTTPError as e:
            return {
                'error': str(e)
            }


@metadata_ns.route('/table')
class MetaDataTable(Resource):
    """
    Retreive list of avaiable tables
    """
    def get(Request):
        try:
            table_result = getBG(endpoint='metadata/table', data={}, base_url=base_url)
            return table_result
        except requests.HTTPError as e:
            return {
                'error': str(e)
            }



@metadata_ns.route('/table/<string:table_name>')
class SingleTable(Resource):
    """
    Retrieve metadata about a table
    """
    def get(self, table_name):
        try:
            endpoint = 'metadata/table/%s' % (table_name)
            table_meta = getBG(endpoint=endpoint, data={}, base_url=base_url)
            return table_meta
        except requests.HTTPError as e:
            return {
                'error': str(e)
            }


@metadata_ns.route('/table/<string:table_name>/column/<string:column_name>')
class SingleColumn(Resource):
    """
    Retrieve metadata about a column in a table
    """
    def get(self, table_name, column_name):
        try:
            endpoint = 'metadata/table/%s/column/%s' % (table_name, column_name)
            table_meta = getBG(endpoint=endpoint, data={}, base_url=base_url)
            return table_meta
        except requests.HTTPError as e:
            return {
                'error': str(e)
            }


@metadata_ns.route('/tissue')
class Tissues(Resource):
    """
    Return a list of available tissues (bto terms with underscores)
    """
    def get(Request):
        try:
            studies = getBG(endpoint='metadata/tissue', data={}, base_url=base_url)
            return studies
        except requests.HTTPError as e:
            return {
                'error': str(e)
            }



@metadata_ns.route('/tissue/<string:tissue_name>')
class SingleTissue(Resource):
    """
    Return a list of substudies and columns associated with a tissue
    """
    def get(self, tissue_name):
        # retrieve bg term if bto or uberon as input
        if 'UBERON:' in tissue_name:
            tissue_name = id2term(var=tissue_name, key='uberon_id', return_key='bg_label', json_blob=uberon_bto_map)
        if 'BTO:' in tissue_name:
            tissue_name = id2term(var=tissue_name, key='bto_id', return_key='bg_label', json_blob=uberon_bto_map)

        try:
            endpoint = 'metadata/tissue/%s' % (tissue_name)
            single_tissue = getBG(endpoint=endpoint, data={}, base_url=base_url)
            return single_tissue
        except requests.HTTPError as e:
            return {
                'message': "'{0}' is not a valid tissue name or identifier".format(tissue_name),
                'error': str(e)
            }



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


class GetInteractionsQuery(Resource):
    def post(self):
        try:
            query_submit = postBG(endpoint='interactions/query', base_url=base_url, data=request.args)
        except Exception as e:
            return {
                'error': str(e)
            }
        query_status = self.get_query_status(query_key=query_submit['request_id'])
        return self.pandas2json(query_status['request_uri'])

    def get(self):
        try:
            query_submit = getBG('interactions/query', base_url=base_url, data=request.args)
        except Exception as e:
            return {
                'error': str(e)
            }
        query_status = self.get_query_status(query_key=query_submit['request_id'])
        return self.pandas2json(query_status['request_uri'])

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
            return {
                'error': str(e)
            }
        return query_status

    def search_things(self, key, value, data):
        return [item for item in data if item[key] == value]

    def pandas2json(self, request_uri):
        # use pandas to get csv with request uri and serialize into json for return
        pd_df = pd.read_csv(request_uri[0])
        out_json = json.loads(pd_df.to_json(orient='records'))
        final_json = list()

        # begin deconstruction of multi concept csv headers into json metadata fields
        for record in out_json:
            new_record = {
                'Gene1': record['Gene1'],
                'Gene2': record['Gene2'],
                'GPID': record['GPID'],
                'GIANT': [],
                'GTEx': [],
                'BioGRID': [],
                'TCGA': []
            }

            # temporary container for source objects before merging
            sources = {
                'GIANT': [],
                'GTEx': [],
                'BioGRID': [],
                'TCGA': []
            }
            for k, v in record.items():
                # check for key name in meta_columns(deconstructed metadata fields from
                # csv headers with uberon, bto terms mapped.  Code in Columns2JsonFields.ipynb
                if k in meta_columns.keys():
                    col = meta_columns[k]
                    sources[col['source']].append({
                        col['type']: v,
                        'cancer_type': col['cancer_type'],
                        'tissue': col['tissue']
                    })
            # combined results on source and tissue
            for sor in list(sources.keys()):
                for l1, l2 in zip(sources[sor], sources[sor][1:]):
                    if l1['tissue'] is not None and l2['tissue'] is not None:
                        if l1['tissue']['bg_label'] == l2['tissue']['bg_label']:
                            l1.update(l2)
                            new_record[sor].append(l1)
                    if l1['cancer_type'] is not None and l2['cancer_type'] is not None:
                        if l1['cancer_type'] == l2['cancer_type']:
                            l1.update(l2)
                            new_record[sor].append(l1)
            final_json.append(new_record)

        return final_json


if __name__ == '__main__':
    app.run(debug=True)

