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

## Start the Flink job:
    ./bin/start-cluster.sh
    ./bin/flink run target/market-data-analysis-1.0-SNAPSHOT.jar

## Checking Signals

The buy/sell signals will be printed to the console as the job processes data. You can also view the signals in the Flink web UI (usually at http://localhost:8081).

## Running Tests
    mvn test

## Additional Information

* Checkpointing and Savepoints: The job is configured for checkpointing with a 30-second interval. Refer to the code for configuration details.  
* Failure Handling: The job is designed to recover from failures using checkpoints.   
* Custom Functions: The project includes custom source and aggregate functions.

## Contributing

Feel free to contribute to this project! Please follow the standard fork-and-pull request workflow on GitHub.
