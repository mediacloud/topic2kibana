Media Cloud Topic-to-Kibana Importer
====================================

This is a simple utility for automating the import of content from a Media Cloud topic version into Kibana for analysis.

Install
-------

1. Set up Python 3.x
2. run `pip install -r requirements.txt`
3. set up the requisite environment variables in your system, or in a `.env` file, as described below

Configuration
-------------

Set up a few environment variables to make this work:

* MC_API_KEY: your Media Cloud API key (with admin privileges)
* ELASTIC_SEARCH_HOST: the URL of your Elastic Search host, including the port number

Running
-------

You need to know the `topics_id` and `snapshots_id`. Then run it like:

```
python topic2kibana.py [topics_id] [snapshots_id]
```
