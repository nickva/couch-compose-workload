Docker compose workload for CouchDB
--

Starts 3 nodes then write and read documents

The workload is in python with requests module as the only external dependency.

Some helpful podman commands:
----

Reset network:`podman network prune`

Force rebuild + start: `podman-compose up --build --force-recreate`

Build + start: `podman-compose up --build`

Stop: `podman-compose down`

Exec: `podman-compose exec couchdb1 bash`

Log: `podman-compose logs workload`
