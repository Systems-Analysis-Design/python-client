# python-client
Python client to connect to the queueing system

To connect to the server, just do the following:

```
from client import Client

client = Client("SERVER ADDRESS")
client.push(key, value) # push message
key, value = client.pull() # pull message
client.subscribe(function) # subscribe a function 
```
