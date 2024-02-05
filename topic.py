from kafka.admin import KafkaAdminClient, NewTopic


quickstart_topic = NewTopic(name="quickstart-events", num_partitions=1, replication_factor=1)
training_topic = NewTopic(name="training", num_partitions=4, replication_factor=1)

kafka_admin = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id="admin")
kafka_admin.create_topics([training_topic, quickstart_topic])