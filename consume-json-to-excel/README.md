A quick tool cli to see what messages are on a particular topic in Kafka using a groupid.

The script is small enough that it may be easily modified for custom features/needs.

Sample usage:

python app.py --brokerlist localhost:9092 --topic some-test-topic --groupid testing1