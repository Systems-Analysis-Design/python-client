import json
from urllib3 import PoolManager


class Client:
    def __init__(self, address: str):
        self.address = address
        self.connection = PoolManager()
        self.subscribers = []

    def push(self, key: str, value: bytes):
        headers = {'Content-type': 'application/json'}
        data = {'key': key, 'value': value.decode()}
        response = self.connection.request("POST", self.address + "/push", body=json.dumps(data), headers=headers)
        if response.status != 200:
            raise Exception("error in calling push function")
        self._notify_subscribers()

    def pull(self) -> (str, bytes):
        response = self.connection.request("GET", self.address + "/pull")
        if response.status != 200:
            raise Exception("error in calling pull function")
        data = json.loads(response.data.decode())
        return data['key'], bytes(data['value'], encoding="UTF-8") if data['value'] is not None else None

    def _notify_subscribers(self):
        if len(self.subscribers) != 0:
            f = self.subscribers.pop(0)
            key, value = self.pull()
            if value is not None:
                f(key, value)
            self.subscribers.append(f)

    def subscribe(self, f):
        self.subscribers.append(f)

    def close(self):
        self.connection.clear()
        self.subscribers.clear()


SERVER_ADDRESS = "http://localhost:8080"
client = Client(SERVER_ADDRESS)


def push(key: str, value: bytes):
    client.push(key, value)


def pull() -> (str, bytes):
    return client.pull()


def subscribe(action):
    client.subscribe(action)


def close():
    client.close()
