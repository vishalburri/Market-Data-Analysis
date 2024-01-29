# Market Data Analysis with Apache Flink

## Overview

This project utilizes Apache Flink to process real-time market data, compute moving averages, and generate buy/sell signals based on predefined criteria.

## Architecture

Data Source: Alpaca MarketData API / CSV file containing trade data (symbol, price, timestamp).

Data Processing:  
    * Reads data from the CSV file using a custom AlpacaSourceFunction / CSVSourceFunction.
    * Gets market data stream based on the subscribed symbols
    * Assigns timestamps and watermarks to handle out-of-order events.  
    * Computes moving averages within 60-second sliding windows with 30-second slide intervals.  
    * Joins processed streams for target and non-target symbols.  
    * Generates buy/sell signals based on calculated averages. 

Output: Prints buy/sell signals to the console.

## Prerequisites

* Java: 11 or higher
* Apache Maven: 3.5 or higher
* Apache Flink: 1.18.1 (download and install from https://flink.apache.org/downloads.html)

## Running the Application
    1. git clone https://github.com/vishalburri/Market-Data-Analysis.git
    2. cd market-data-analysis
    3. mvn clean package

## Source Config
To use Alpaca source:  
    
    1. export ALPACA_API_KEY = <YOUR_API_KEY>
    2. export ALPACA_API_SECRET = <YOUR_SECRET_KEY>

To use CSV source: (Modify below value in StreamUtil.java)

    TRADES_FILE_PATH = "<YOUR_PATH_TO_CSV>";

## Start the Flink job
    ./bin/start-cluster.sh
    ./bin/flink run target/Market-Data-Analysis-1.0-SNAPSHOT.jar

## Checking Signals

The buy/sell signals will be printed to the console as the job processes data.  
You can also view the signals in the Flink web UI (usually at http://localhost:8081) and
go to flink-task-executor.out file.  
Since the sliding time window is 60 sec and slide interval is 30 sec, each signal will be received at 30 sec frequency. 

### Sample Output:

```dtd
SELL Signal triggered with currentTargetAvgPrice: 191.8071875 lastTargetAvgPrice: 191.97793103448274 currentOtherStockAvgPrice: 247.86621290545202 lastOtherStockAvgPrice: 247.762923564135
SELL Signal triggered with currentTargetAvgPrice: 191.7021428571429 lastTargetAvgPrice: 191.8071875 currentOtherStockAvgPrice: 248.02881868131865 lastOtherStockAvgPrice: 247.86621290545202
BUY Signal triggered with currentTargetAvgPrice: 191.70828124999994 lastTargetAvgPrice: 191.7021428571429 currentOtherStockAvgPrice: 248.03098350451296 lastOtherStockAvgPrice: 248.02881868131865
SELL Signal triggered with currentTargetAvgPrice: 191.7559375 lastTargetAvgPrice: 191.70828124999994 currentOtherStockAvgPrice: 247.92202735098616 lastOtherStockAvgPrice: 248.03098350451296
SELL Signal triggered with currentTargetAvgPrice: 191.8509090909091 lastTargetAvgPrice: 191.7559375 currentOtherStockAvgPrice: 247.83713143152 lastOtherStockAvgPrice: 247.92202735098616
SELL Signal triggered with currentTargetAvgPrice: 191.80090909090907 lastTargetAvgPrice: 191.8509090909091 currentOtherStockAvgPrice: 247.76276576982252 lastOtherStockAvgPrice: 247.83713143152
SELL Signal triggered with currentTargetAvgPrice: 191.77904761904765 lastTargetAvgPrice: 191.80090909090907 currentOtherStockAvgPrice: 247.80552020202018 lastOtherStockAvgPrice: 247.76276576982252
BUY Signal triggered with currentTargetAvgPrice: 191.79166666666669 lastTargetAvgPrice: 191.77904761904765 currentOtherStockAvgPrice: 247.9367171717172 lastOtherStockAvgPrice: 247.80552020202018
BUY Signal triggered with currentTargetAvgPrice: 191.89166666666668 lastTargetAvgPrice: 191.79166666666669 currentOtherStockAvgPrice: 248.1155698005698 lastOtherStockAvgPrice: 247.9367171717172
SELL Signal triggered with currentTargetAvgPrice: 191.8234090909091 lastTargetAvgPrice: 191.89166666666668 currentOtherStockAvgPrice: 248.34696471704623 lastOtherStockAvgPrice: 248.1155698005698
SELL Signal triggered with currentTargetAvgPrice: 191.76454545454547 lastTargetAvgPrice: 191.8234090909091 currentOtherStockAvgPrice: 248.4481112415232 lastOtherStockAvgPrice: 248.34696471704623
SELL Signal triggered with currentTargetAvgPrice: 191.7238235294118 lastTargetAvgPrice: 191.76454545454547 currentOtherStockAvgPrice: 248.60464724164729 lastOtherStockAvgPrice: 248.4481112415232
BUY Signal triggered with currentTargetAvgPrice: 191.76425 lastTargetAvgPrice: 191.7238235294118 currentOtherStockAvgPrice: 248.74756831014318 lastOtherStockAvgPrice: 248.60464724164729
SELL Signal triggered with currentTargetAvgPrice: 191.744375 lastTargetAvgPrice: 191.76425 currentOtherStockAvgPrice: 248.73921687370603 lastOtherStockAvgPrice: 248.74756831014318
SELL Signal triggered with currentTargetAvgPrice: 191.66 lastTargetAvgPrice: 191.744375 currentOtherStockAvgPrice: 248.49456673052362 lastOtherStockAvgPrice: 248.73921687370603
BUY Signal triggered with currentTargetAvgPrice: 191.69420000000002 lastTargetAvgPrice: 191.66 currentOtherStockAvgPrice: 248.51621362979253 lastOtherStockAvgPrice: 248.49456673052362
```

## Running Tests
    mvn test

## Additional Information

* Checkpointing: The job is configured for checkpointing with a 30-second interval. Refer to the code for configuration details.  
* Failure Handling: The job is designed to recover from failures using checkpoints.   
* Custom Functions: The project includes custom source and aggregate functions.

## Contributing

Feel free to contribute to this project! Please follow the standard fork-and-pull request workflow on GitHub.
