# BigDataKickStarters
A set of KickStarter Projects to bootstrap various Machine Learning/Big Data Technologies

## Kafka KickStarters
A set of Producers and Consumers using Kafka as the underlying Bus

* Basic Producer and Consumer
Consumer is based on the Kafka Stream Implementation (Typically called High Level Consumer)

* Producer with a Custom Partitioner
Utilizes Kafkas Partitioning Ability to send a set of ClickStream Access Log to a Specific partition based on [Link] [1]
[1]: https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example

* Multi Threaded Consumer 
Uses Kafkas innate multi threaded stream along with Thread Pool Executor to consume data from Multiple Partitions simultaneously.
