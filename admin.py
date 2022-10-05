from confluent_kafka.admin import AdminClient, NewTopic


admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})


topic_list = [
    NewTopic("topic_1_partition", 1, 1),
    NewTopic("topic_2_partition", 2, 1),
    NewTopic("topic_6_partition", 6, 1)
]
admin_client.create_topics(topic_list)

print(admin_client.list_topics().topics)
