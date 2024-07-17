from faker import Faker
from quixstreams import Application
import numpy as np  
import time, json, pytz  
from datetime import date, timedelta, datetime
from mimesis import Field, Schema
from mimesis.enums import Gender
from mimesis.locales import Locale


#Define a function to generate data in json formart for easier analysis. 
def generate_bank_transactions(start_date=None):
    field = Field(Locale.EN, seed=0xff)
    fake = Faker()
    
    east_african_time = pytz.timezone('Africa/Nairobi')

    if start_date is None:
        start_date = datetime.utcnow()

    current_date = pytz.utc.localize(start_date).astimezone(east_african_time)

    schema_definition = lambda: {
        "pk": field("increment"),
        "tranDate": current_date.isoformat(),
        "tranCode": fake.swift8(),
        "custName": field("full_name"),
        "cardNum": field("credit_card_number"),
        "zipcode": field("zip_code"),
        "transAmount": field("float_number", start=1.0, end=1000000.0, precision=2),
        "cardType": field("choice", items=['Classic visa', 'Gold mastercard', 'Platinum visa', 
                                                  'Gold visa', 'Visa signature','World elite','Infinite credit']),
        "type_transaction": field("choice", items=["purchase", "refund", "withdrawal"]),
    }

    while True:
        schema = Schema(schema=schema_definition)
        transaction = schema.create() 
        json_transaction = json.dumps(transaction, indent=4)  
        yield json_transaction
        current_date += timedelta(seconds=1) 
        time.sleep(2)

#Define main application to stream data
def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        auto_offset_reset="latest",
        consumer_group="BankStreamingFake"
    )
        
    with app.get_producer() as producer:
        for transactions_json in generate_bank_transactions():
            data = json.loads(transactions_json)
            producer.produce(
                topic=("BankStreaming"),
                key="BankTransactions",
                value=json.dumps(data).encode("utf-8")    
            )
            time.sleep(1)
        
if __name__ == "__main__":
    main()
