from complex_worker import Worker


consumer = Worker.get_consumer()
async_consumer = Worker.get_async_consumer()
consumer.add_log("Going to calculate!")
async = async_consumer.calculate(1,2,3)
result = async.wait()
# If we go async, we can tell the processing time and the total roundtrip time
consumer.add_log("Got: %d, Processed in %f seconds, total seconds: %f" % (result, async.process_time_seconds, async.total_time_seconds, ))
