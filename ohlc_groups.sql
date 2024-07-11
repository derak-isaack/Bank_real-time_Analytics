CREATE DATABASE Minute_batches;
use DATABASE Minute_batches;
CREATE TABLE ohlc(
    id INT PRIMARY KEY AUTO_INCREMENT,
    open_transaction FLOAT,
    high_transaction FLOAT,
    low_transaction FLOAT,
    close_transaction FLOAT,
)