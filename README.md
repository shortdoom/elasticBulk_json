# elasticBulk_json
Load data from your JSON file using bulk API in ElasticSearch.

## What's the purpose?
ElasticSearch indexing is slow if you are loading large json files with many keys. Documentation suggests using bulk API for such tasks.

[ElasticSearch Bulk API Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)

As a rule of thumb, you should use bulk API when you have more than 10.000 items in your dictionary. 

## What's in the code?

This is very simple wrapper around ``elasticsearch`` Python client. It allows loading file, creating dynamic mapping (can be skipped) and loading data into created index. Additionally, it saves JSON file to separate variable defined as pandas dataframe for inspection or change of index orientation (This is dependant on your needs).

It uses tqdm library for showing progress on your loading.

You can change what keys should be indexed by bulk API by editing ```generate_actions``` function from using whole dictionary to only specific keys.

## TODO
- Add tests
- Allow for loading files different than json (csv, xlxs, etc.)
- Wrap it into CLI
