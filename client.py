import json
from sseclient import SSEClient
from urllib3 import PoolManager, Timeout
from threading import Thread


class Client:
    def __init__(self, address: str):
        self.address = address
        self.connection = PoolManager()
        self.subscribe_functions = []
        self.subscribe_client = None
        self.thread = None

    def push(self, key: str, value: bytes):
        headers = {'Content-type': 'application/json'}
        data = {'key': key, 'value': value.decode()}
        response = self.connection.request("POST", self.address + "/push", body=json.dumps(data), headers=headers)
        if response.status != 200:
            raise Exception("error in calling push function", response.msg)

    def pull(self) -> (str, bytes):
        response = self.connection.request("GET", self.address + "/pull")
        if response.status != 200:
            raise Exception("error in calling pull function", response.msg)
        data = json.loads(response.data.decode())
        return data['key'], bytes(data['value'], encoding="UTF-8") if data['value'] is not None else None

    def _request_generator(self):
        return self.connection.request("GET",
                                       self.address + "/subscribe",
                                       preload_content=False,
                                       headers={'Accept': 'text/event-stream'})

    def _init_subscribe(self):
        if self.subscribe_client is None:
            self.subscribe_client = SSEClient(self._request_generator())

    def _call_subscribe_functions(self, key: str, value: bytes):
        for f in self.subscribe_functions:
            f(key, value)

    def _wait_for_events(self):
        try:
            for event in self.subscribe_client.events():
                json_data = json.loads(event.data)
                key = json_data['key']
                value = json_data['value']
                self._call_subscribe_functions(key, value)
        except:
            return

    def subscribe(self, f):
        self._init_subscribe()
        self.subscribe_functions.append(f)
        if self.thread is None:
            self.thread = Thread(target=self._wait_for_events)
            self.thread.start()

    def close(self):
        self.subscribe_client.close()
        self.connection.clear()
