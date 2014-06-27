# Most simple. Stream needs to be there already
import boto
import time
import datetime
import kinesis_config
import json

kinesis = boto.connect_kinesis()
stream = kinesis_config.STREAM_NAME
counter = 0

insert_data = {"counter":counter, "insert-date": str(datetime.datetime.now())}
response = kinesis.put_record(stream, json.dumps(insert_data), str(counter))
sequence_number = response['SequenceNumber']

# write data to the stream
while True:
	counter += 1
	try:
		time.sleep(.0000001)
		insert_data = {"counter":counter, "insert-date": str(datetime.datetime.now())}
		response = kinesis.put_record(stream, json.dumps(insert_data), str(counter), sequence_number_for_ordering=sequence_number)
		sequence_number = response['SequenceNumber']
		print "{0}".format(insert_data)
	except Exception,e:
		print "failed because of: {0}".format(e)
