from quixstreams import Application
import logging
import json

def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        auto_offset_reset="earliest",
        consumer_group="BankStreamingFake"
    )
    

    
    with app.get_consumer() as consumer:
        consumer.subscribe(["BankStreaming"])
        while True:
            msg = consumer.poll(2)
            
            if msg is None:
                print("waiting for transaction...")
            else:
                key = msg.key().decode("utf-8")
                value = json.loads(msg.value())
                
                print(f"Received transaction: {key}{value}")
                
if __name__ == "__main__":
    main()
            