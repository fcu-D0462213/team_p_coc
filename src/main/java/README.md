**Setup Environment**

1. Configure all nodes in cockroachDB via the command below
wget -qO- https://binaries.cockroachdb.com/cockroach-v19.2.9.linux-amd64.tgz | tar  xvz
cockroach start --insecure --store=node1 --listen-addr=192.168.48.184:26257 --http-addr=192.168.48.184:8080 --background
cockroach start --insecure --store=node2 --listen-addr=192.168.48.185:26257 --http-addr=192.168.48.185:8080 --join=192.168.48.184:26257 --background
cockroach start --insecure --store=node3 --listen-addr=192.168.48.186:26257 --http-addr=192.168.48.186:8080 --join=192.168.48.184:26257 --background
cockroach start --insecure --store=node4 --listen-addr=192.168.48.187:26257 --http-addr=192.168.48.187:8080 --join=192.168.48.184:26257 --background
cockroach start --insecure --store=node5 --listen-addr=192.168.48.188:26257 --http-addr=192.168.48.188:8080 --join=192.168.48.184:26257 --background

2.Create DataBase in 