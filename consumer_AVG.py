from kafka import KafkaConsumer
import json

def create_consumer(topic):
    """Create a Kafka consumer for a given topic."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def process_messages(consumer):

    sum_price = 0.0
    
    count = 0

    for message in consumer:
        
        try:
        
            data = message.value
        
            price = data['price']
            
            if '-' in price:
            
                p = price.split('-')
                
                for element in p:
                
                    element = element.strip('$')
                    
                    element = float(element)
                    
                    sum_price += element
                    
                    count += 1
                    
                    avg  = sum_price/count
                    
                    print(f"Average price at this moment is: {avg}")
                
            else:
            
                p = price.strip('$')
                
                p = float(p)
                
                sum_price += p
                
                count += 1
                
                avg  = sum_price/count
                
                print(f"Average price at this moment is: {avg}")
                
        except:
        
            continue
        
def main():
    topic = 'topic'
    consumer = create_consumer(topic)
    process_messages(consumer)
    
if __name__ == '__main__':
    main()
