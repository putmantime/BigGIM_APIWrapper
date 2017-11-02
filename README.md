# BigGIM_APIWrapper
Wraps the 'http://biggim.ncats.io/api' API to make it Tranlsator SMART API Complient

UNDER CONSTRUCTION

- interactions/query is a two stage call in BigGIM returning a request id that is used to get the URI to the desired interactions table.csv file.  This wrapper combines that effort into one call
- takes all the arguments specified [here](http://biggim.ncats.io/api)

TODO:

- Map the BTO tissue terms to Uberon terms (partially implemented but some uberon terms are missing
- Add to SMART API registry
- process final output of interactions/query call; currently returns uri for csv file


