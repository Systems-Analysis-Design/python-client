# python-client
Python client to connect to the queueing system

To connect to the server, just do the following:

```
from client import pull, push, subscribe, close

push(key, value) # push message
key, value = pull() # pull message
subscribe(function) # subscribe a function
close() # close the client and unsubscribe
```
