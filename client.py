import http.client
import json


class Client:
    def __init__(self, host: str, port: int, timeout: int = 600):
        self.connection = http.client.HTTPConnection(host, port, timeout)

    def push(self, key: str, value: bytes):
        headers = {'Content-type': 'application/json'}
        data = {'key': key, 'value': value}
        json_data = json.dumps(data)
        self.connection.request("POST", "/push", json_data, headers)
        response = self.connection.getresponse()
        if response.status != 200:
            raise Exception("error in calling push function", response.msg)

    def pull(self) -> (str, bytes):
        self.connection.request("GET", "/pull")
        response = self.connection.getresponse()
        if response.status != 200:
            raise Exception("error in calling pull function", response.msg)
        return response.read()

    def subscribe(self, f):
        # TODO implement
        pass

    def close(self):
        # TODO unsubscribe
        self.connection.close()
