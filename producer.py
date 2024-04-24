from kafka import KafkaProducer
from time import sleep

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Topic name to which data will be sent
topic = 'topic'

# Path to your file containing row-wise data
file_path = 'Sampled_Amazon_Meta.json'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Function to read file and send data row by row
def stream_data():
	with open(file_path, 'r') as file:
		for line in file:
			sleep(3)
			producer.send(topic, line.encode())
			print(f"Message written with value = {line}")
			producer.flush()
			
	    # Close the producer after sending all data
		producer.close()

# Start streaming data
stream_data()

