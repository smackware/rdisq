from complex_worker import Worker


worker = Worker()
worker.add_log("Going to calculate!")
async = worker.async.calculate(1,2,3)
result = async.wait()
# If we go async, we can tell the processing time and the total roundtrip time
worker.add_log("Got: %d, Processed in %f seconds, total seconds: %f" % (result, async.process_time, async.total_time, ))
