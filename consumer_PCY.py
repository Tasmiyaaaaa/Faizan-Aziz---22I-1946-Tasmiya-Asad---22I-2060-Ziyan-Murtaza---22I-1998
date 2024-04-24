from kafka import KafkaConsumer
import json
import itertools
import hashlib

# Configuration parameters
min_support = 3
min_confidence = 0.25
hash_table_size = 1000

def create_consumer(topic):

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def hash_function(itemset):
    return int(hashlib.sha256(str(itemset).encode('utf-8')).hexdigest(), 16) % hash_table_size

def generate_item_pairs(transaction):
    return itertools.combinations(sorted(transaction), 2)

def process_messages(consumer):
    transactions = []
    hash_table = [0] * hash_table_size

    for message in consumer:
        data = message.value
        if 'asin' in data and 'also_buy' in data and data['also_buy']:
        
            # Append the current product asin with also_buy items as a single transaction
            transaction = [data['asin']] + data['also_buy']
            transactions.append(transaction)

        print(len(transactions))

        if len(transactions) >= 20:

            item_counts = {}
            for transaction in transactions:
                for item in transaction:
                    item_counts[item] = item_counts.get(item, 0) + 1
                pairs = generate_item_pairs(transaction)
                for pair in pairs:
                    hash_value = hash_function(pair)
                    hash_table[hash_value] += 1

            frequent_items = set(item for item, count in item_counts.items() if count >= min_support)
            frequent_pairs = set()
            for pair in itertools.combinations(frequent_items, 2):
                hash_value = hash_function(pair)
                if hash_table[hash_value] >= min_support:
                    frequent_pairs.add(pair)

            # Generate association rules from frequent pairs
            for pair in frequent_pairs:
                item1, item2 = pair
                count_item1 = item_counts[item1]
                confidence = hash_table[hash_function((item1, item2))] / count_item1
                if confidence >= min_confidence:
                    print(f"Rule: {item1} -> {item2}")
                    print(f"Confidence: {confidence:.3f}")
                    print("-" * 10)

            transactions = []  # Reset transactions

def main():
    topic = 'topic'
    consumer = create_consumer(topic)
    process_messages(consumer)

if __name__ == '__main__':
    main()

