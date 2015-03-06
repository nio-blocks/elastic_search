def elasticsearch_running():
    try:
        from elasticsearch import Elasticsearch
    except:
        return False

    es = Elasticsearch()
    # try just once
    es.transport.max_retries = 1
    if not es.ping():
        print("elasticsearch connection error, Test skipped")
        return False

    return True


def delete_elasticsearch_document(document):
    try:
        import requests
    except:
        return

    url = "http://127.0.0.1:9200/{0}/".format(document)
    requests.delete(url)
