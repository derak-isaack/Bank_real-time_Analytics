from quixstreams import Application
import logging
import json, mysql.connector
from datetime import timedelta


#Define functions for grouping transactions on a 5 minute interval
def initialize_fn(msg):
    amounts = [float(transaction['transAmount']) for transaction in msg]
    
    if not amounts:
        return {"open": None, "high": None, "low": None, "close": None}

    return {
        "open": amounts[0],
        "high": max(amounts),
        "low": min(amounts),
        "close": amounts[-1],
    }
    
def reducer_fn(summary, msg):
    if isinstance(msg, list):
        amounts = float(msg[0]['transAmount'])  
    else:
        amounts = float(msg['transAmount'])
    

    return{
        "open": summary["open"],
        "high": max(summary["high"], amounts),
        "low": min(summary["low"], amounts),
        "close": amounts,
    }

def send_to_mysql(summary, connection):
        cursor = connection.cursor()
        print("Sending to MySQL...")
        # Code to send data to MySQL database goes here
        query = """INSERT INTO transaction_summary (start, end, open_transaction, high_transaction, low_transaction, close_transaction) VALUES (%s, %s, %s, %s, %s, %s)"""
        values = (summary['start'], summary['end'], summary['value']['open'], summary['value']['high'], summary['value']['low'], summary['value']['close'])
        cursor.execute(query, values)
        connection.commit()
        cursor.close()
        print("Data sent to MySQL successfully!")

def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        auto_offset_reset="latest",
        consumer_group="BankStreamingFake"
    )
    # app.clear_state()
    
    input_topic = app.topic("BankStreaming")
    output_topic = app.topic("BankStreamingSummary")
    
    sdf = app.dataframe(topic=input_topic)
    # sdf = sdf.update(lambda row: print(row))
    sdf = (
    sdf.tumbling_window(timedelta(seconds=10))
    .reduce(reducer=reducer_fn, initializer=initialize_fn)
    ).final()
    
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="@admin#2024*10",
        database="minute_batches"
    )
    sdf = sdf.update(lambda msg: logging.debug(print(msg)))
    sdf = sdf.update(lambda msg: send_to_mysql(msg, connection))
    # sdf = sdf.to_topic(output_topic)
    #sdf = sdf.apply(send_to_mysql)
    app.run(sdf)
    
                
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
            