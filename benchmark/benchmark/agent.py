import time
import requests


class FakeAgent:
    def __init__(self, duration, node_parameters, prometheus_addresses):
        self.duration = duration
        self.node_parameters = node_parameters
        self.addresses = prometheus_addresses

        for address in self.addresses:
            print(address)

    def run(self):
        time.sleep(self.duration/4)

        new_batch_size = int(self.node_parameters.json['batch_size']) / 2
        new_parameters = {
            'header_size': self.node_parameters.json['header_size'],
            'max_header_delay': self.node_parameters.json['max_header_delay'],
            'sync_retry_delay': self.node_parameters.json['sync_retry_delay'],
            'sync_retry_nodes': self.node_parameters.json['sync_retry_nodes'],
            'batch_size': int(new_batch_size),
            'max_batch_delay': self.node_parameters.json['max_batch_delay']
        }

        print(f'Setting new parameters to: {new_parameters}')

        for address in self.addresses:
            url = f'http://{address}/parameters'
            x = requests.post(url, json=new_parameters)
            print(x.text)

        time.sleep(3*self.duration/4)
