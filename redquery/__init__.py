from urlparse import urljoin
import json
import time

import requests


class QueryResult(object):
    def __init__(self, rows, query):
        self.query = query
        self.rows = rows

    @classmethod
    def create(cls, res):
        rows = res['query_result']['data']['rows']
        query = res['query_result']['query']
        return QueryResult(rows, query)


class Client(object):
    def __init__(self, host, api_key, data_source_id):
        self.api_base = urljoin(host, 'api')
        self.api_key = api_key
        self.data_source_id = data_source_id

    def _api_get(self, resource):
        return requests.get(self.api_base + '/' + resource,
                headers={'Authorization': 'Key %s' % self.api_key})

    def _api_post(self, resource, data):
        return requests.post(self.api_base + '/' + resource,
                            headers={'Authorization': 'Key %s' % self.api_key},
                            data=json.dumps(data))

    def data_sources(self):
        return self._api_get('data_sources')

    def query(self, query, retry_num=30, interval_sec=1):
        res_j = self._post_query(query).json()
        retried = 0
        while not self._query_completed(res_j):
            time.sleep(interval_sec)
            res_j = self._post_query(query).json()
            retried += 1
            if retried > retry_num:
                raise Exception('Max retry num reached.')
        return QueryResult.create(res_j)

    def _post_query(self, query):
        data = {
            'query': query,
            'data_source_id': self.data_source_id
        }
        return self._api_post('query_results', data)

    def _has_result(self, res_json):
        return ('query_result' in res_json) and ('retrieved_at' in res_json['query_result'])

    def _query_completed(self, res_json):
        if self._has_result(res_json):
            return True
        uncompleted_job = self.job(res_json['job']['id'])
        if self._job_has_error(uncompleted_job):
            raise Exception(uncompleted_job['job']['error'])
        return False

    def _job_has_error(self, res_json):
        if res_json['job']['error']:
           return True
        return False

    def job(self, jid):
        return self._api_get('jobs/%s' % jid).json()
