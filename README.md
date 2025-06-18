### Cassandra on Docker

start cassandra

docker run --name cassandra-dev -d -p 9042:9042 cassandra:4.1

logs 
docker logs -f cassandra-dev

enter shell in container
docker exec -it cassandra-dev cqlsh

