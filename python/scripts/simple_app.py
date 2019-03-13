import config

# Read log data into file
log_file = "%s/README.md" % config.HOME
log_data = config.spark.read.text(log_file).cache()

# Count lines with 'a', 'b'
num_a = log_data.filter(log_data.value.contains('a')).count()
num_b = log_data.filter(log_data.value.contains('b')).count()
print("Lines with a: %i, lines with b: %i" % (num_a, num_b))

# Stop Spark session
config.spark.stop()
