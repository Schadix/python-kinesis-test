# Most simple.
import boto
import time
import kinesis_config
import json
import datetime

kinesis = boto.connect_kinesis()
stream = kinesis_config.STREAM_NAME
shard_id = kinesis_config.SHARD_ID

shard_iterator_json = kinesis.get_shard_iterator(stream, shard_id, 'LATEST')
shard_iterator = shard_iterator_json['ShardIterator']
counter = 0
latency_sum=0
# outfile=open('result.log', 'w')

while True:
	response = kinesis.get_records(shard_iterator, limit=10000)
	shard_iterator = response['NextShardIterator']
	#print "{0} records. {1}".format(len(response['Records']), "response")
	if len(response['Records']):
		for i,v in enumerate(response['Records']):
			counter+=1
			received_data=json.loads(v['Data'])
			time_now = datetime.datetime.now()
			latency=(time_now-datetime.datetime.strptime(received_data['insert-date'], '%Y-%m-%d %H:%M:%S.%f'))
			# latency_sum+=latency
			# latency_avg=latency_sum / counter / 1000000
			print (received_data, time_now, latency)
			# outfile.write("{0},{1}\n".format(received_data['counter'],v['SequenceNumber']))
			# outfile.flush()
	time.sleep(1)
# outfile.close()
