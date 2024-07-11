from quixstreams import Application
import logging
import json
from datetime import timedelta


#Define functions for grouping transactions on a 5 minute interval
def initialize_fn(msg):

    amount = msg['transAmount']
    
    return{
        "open": amount,
        "high": amount,
        "low": amount,
        "close": amount,
    }
    
def reducer_fn(summary, msg):

    amount = msg['transAmount']

    return{
        "open": summary["open"],
        "high": max(summary["high"], amount),
        "low": min(summary["low"], amount),
        "close": amount,
    }



def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        auto_offset_reset="latest",
        consumer_group="BankStreamingFake"
    )
    
    input_topic = app.topic("BankStreaming")
    
    sdf = app.dataframe(topic=input_topic)
    sdf = (
    sdf.tumbling_window(timedelta(minutes=5))
    .reduce(reducer=reducer_fn, initializer=initialize_fn)
    
    .final()
    )
    app.run(sdf)
    
    # with app.get_consumer() as consumer:
    #     consumer.subscribe(["BankStreaming"])
    #     while True:
    #         msg = consumer.poll(2)
            
    #         if msg is None:
    #             print("waiting for transaction...")
    #         else:
    #             key = msg.key().decode("utf-8")
    #             value = json.loads(msg.value())
                
    #             print(f"Received transaction: {type(value)}")
                
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
            