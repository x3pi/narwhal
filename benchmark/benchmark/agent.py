import time
import requests


class FakeAgent:
    def __init__(self, duration, node_parameters, prometheus_addresses):
        self.duration = duration
        self.node_parameters = node_parameters
        self.addresses = prometheus_addresses

        # Print out the addresses where the nodes expose the metrics and
        # accept new parameters.
        print()
        for i, address in enumerate(self.addresses):
            print(f'Metrics address of node {i}: {address}')

    def run(self):

        # Run for 25% of the benchmark duration without changing the parameters.
        time.sleep(self.duration/4)

        # Query the state of the node and print the state of the first node.
        for i, address in enumerate(self.addresses):
            if i == 0:
                url = f'http://{address}/metrics'
                x = requests.get(url, ())
                print(f'\nState of node {i}:')
                print(x.text)

        # Update the parameters of all nodes to divide the batch size by 2.
        new_batch_size = int(self.node_parameters.json['batch_size']) / 2
        new_parameters = {
            'header_size': self.node_parameters.json['header_size'],
            'max_header_delay': self.node_parameters.json['max_header_delay'],
            'sync_retry_delay': self.node_parameters.json['sync_retry_delay'],
            'sync_retry_nodes': self.node_parameters.json['sync_retry_nodes'],
            'batch_size': int(new_batch_size),
            'max_batch_delay': self.node_parameters.json['max_batch_delay']
        }

        print(f'\nSetting new parameters to: {new_parameters}')

        for i, address in enumerate(self.addresses):
            url = f'http://{address}/parameters'
            x = requests.post(url, json=new_parameters)
            print(f'Node {i} update status: {x.text}')
        print()

        # Run the rest of the benchmark. It is not important to precisely
        # respect the benchmark duration.
        time.sleep(3*self.duration/4)
