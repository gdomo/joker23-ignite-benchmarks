Apache Ignite benchmarks for "How to write to Apache Ignite fast" presented on Joker 2023.

## Prerequirements
### To build
JDK 17, Maven

### To run
1. Available Apache Ignite v. 2.15 cluster of up to 6 server nodes depending on benchmark.
See `config/aws-c6i.xlarge.xml` for a server node configuration template.
2. Java 17 installed

## Build
> mvn clean package

## Install
Place `target/joker23-ignite-benchmarks-1.0.0-SNAPSHOT.jar` to a desired VM.

## Run

> java -cp joker23-ignite-benchmarks-1.0.0-SNAPSHOT.jar --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED 
> --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED 
> --add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED 
> --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED 
> --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED 
> --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED 
> --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED 
> --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED 
> --add-opens=java.sql/java.sql=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED -Xmx6500M  
> com.jokerconf.gdomo.ignite.$BENCHMARK $SERVER_LIST

Where:  
* `$SERVER_LIST` - ip:port_range of server nodes divided by space as required for [static IP finder](https://ignite.apache.org/docs/latest/clustering/tcp-ip-discovery#static-ip-finder)
* `$BENCHMARK` - Benchmark simple class name, see overview below.

## Benchmarks
* `BenchmarkRunner` - basic put / DataStreamer / putAll comparison
* `PutAllBenchmarkRunner` - putAll vs DS comparison, incl key collision rate correlation
* `SqlBenchmarkRunner` - write speed comparison with SQL turned on/off
* `PojoSizeBenchmarkRunner` - write speed comparison with increasing data amount
* `BatchSizeBenchmarkRunner` - write speed comparison with increasing batch size. 
Batch is `flush` frequency for DataStreamer and `Map` size for putAll
* `BackupsBenchmarkRunner` - write speed comparison with increasing replication factor
* `BinaryBenchmarkRunner` - pojo vs BinaryObject comparison