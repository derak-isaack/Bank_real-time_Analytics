CREATE TABLE IF NOT EXISTS transaction_summary (
                date_time TIMESTAMP NOT NULL,
                start BIGINT NOT NULL,
                end BIGINT NOT NULL,
                open_transaction FLOAT NOT NULL,
                high_transaction FLOAT NOT NULL,
                low_transaction FLOAT NOT NULL,
                close_transaction FLOAT NOT NULL,
                PRIMARY KEY (start, end)
            );
