# BigGIM_APIWrapper
Wraps the 'http://biggim.ncats.io/api' API to make it Tranlsator SMART API Complient

UNDER CONSTRUCTION
Currently implements the following endpoints:

'metadata/tissue'

'metadata/tissue/%s'

'metadata/study'

'interactions/query'
- interactions/query is a two stage call in BigGIM returning a request id that is used to get the URI to the desired interactions table.csv file.  This wrapper combines that effort into one call
- takes all the arguments specified [here](http://biggim.ncats.io/api)
TODO:
Rest of the BigGIM enpoints

Map the BTO tissue terms to Uberon terms

Add to SMART API registry


