<h1>Cluster Partition Rebalancer For Kafka</h1>
Cluster Partition Rebalancer For Kafka is a tool that runs in the background on Kafka brokers and lets them move partitions across brokers to maintain a good balance of partitions per broker.

[This is at a Proof of Concept stage. You might have to change some utility code. Example: How to fetch all broker and zookeeper machines on your cluster]

Read more at: https://onedrive.live.com/redir?resid=98916D951A1A5BFB!2128&authkey=!ALpNOGFF80csKdo&ithint=file%2cpdf

## Usage
1. Clone the repository locally
Git clone

2. Change directory to your repo

3. Build and package the project
```
mvn package
```

4. Run the jar like this (preferably as a service so that it is restarted upon exit):
```
rm -f partitions-to-move.json*

On Windows:
java -cp "partitionRebalancerForKafka-1.0-SNAPSHOT.jar;*" BrokerLoadWatcher

On Linux:
java -cp "partitionRebalancerForKafka-1.0-SNAPSHOT.jar:*" BrokerLoadWatcher
```

## License

[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=plastic)](https://github.com/Microsoft/SparkCLR/blob/master/LICENSE.txt)

Cluster Partition Rebalancer For Kafka is licensed under the MIT license. See [LICENSE](LICENSE) file for full license information.