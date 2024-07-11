from faker import Faker
from quixstreams import Application
import numpy as np  
import time, json  
from datetime import date, timedelta, datetime


def datasimulator():
    fake = Faker()
    start_date = datetime.now()
    increement = timedelta(milliseconds=100)
    
    while True:
        transaction_type = fake.random_element(elements=('deposit', 'withdraw'))
        card_type = fake.random_element(elements=('Classic visa', 'Gold mastercard', 'Platinum visa', 
                                                  'Gold visa', 'Visa signature','World elite','Infinite credit'))
        transaction = {
            'tranDate': fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            'tranCode': fake.swift8(),
            'custName': fake.name(),
            'cardNum': fake.credit_card_number(),
            'zipcode': fake.zipcode(),
            'transAmount': int(np.random.randint(100, 1000000)),
            'cardType': card_type,
            'type_transaction': transaction_type
        }
        
        yield json.dumps(transaction)
        start_date += increement 
        time.sleep(1)
        

def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        auto_offset_reset="earliest"
    )
        
    with app.get_producer() as producer:
        for transactions_json in datasimulator():
            data = json.loads(transactions_json)
            producer.produce(
                topic=("BankStreaming"),
                key="BankTransactions",
                value=json.dumps(data)
            )
            time.sleep(1)
        
if __name__ == "__main__":
    main()
