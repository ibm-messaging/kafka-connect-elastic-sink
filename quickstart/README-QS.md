# Build Verification and Quickstart
Files in this directory show how you can build the Kafka Connect sink connector for
Elasticsearch, run a simple test to see it working, and use it directly for
basic investigations.

A complete standalone environment is created in Docker containers so that it does
not affect your local installation. There is also no persistence
in these containers; data is built from scratch on each execution to ensure
repeatability.

## The Quickstart Environment

Please note that the quickstart environment uses the same default port numbers as
Apache Kafka and Elasticsearch. As a result, the quickstart environment cannot be
run at the same time as those unless the configuration is changed so they do not clash.

### Requirements for the quickstart environment
To run the environment, you need these commands on your local system:
* [docker-compose](https://docs.docker.com/compose/)
* curl
* [jq](https://stedolan.github.io/jq/)

### Running the environment
The `RUNME.sh` script creates and runs containers with a Kafka broker, an
Elasticsearch server and Kibana. It compiles the connector and then runs it in
a container. The script then publishes some messages to Kafka which are
copied into Elasticsearch by the connector.

Note that the script has some delays coded to give time for the environment
to stabilise and services to be fully available. If you have a slow or very
busy machine, these delays may not be sufficient.

The script leaves the containers running, to allow you to inspect it further.
For example, you could run `docker-compose logs connector` to look at the
log files for one of the containers.

If you want to see the documents that have been put into Elasticsearch, you can
use the Kibana instance by pointing a browser at http://localhost:5601.

When you want to stop the containers, run `docker-compose --project-name qs down`.

## Running your own investigations
You can use the `docker-compose.yml` file to create your own environment outside
of the `RUNME.sh` script. Take a look at the script to see how commands
are issued to the Kafka broker such as creating topics and publishing events.
