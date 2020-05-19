#!/bin/bash

# Purpose: Read csv Files and write into respective Kafka topics
# Assumption: topics 'lineitem', 'orders' and 'customers' already exist
# ------------------------------------------------------------------------------------

csvToKafka () {
    echo "---------------- Pushing data from $1 to $2 db ----------------"
    # checking if the csv file exists
    [[ ! -f $1 ]] && {
      echo "$1 file not found"
      exit 99
    }

    # helps in stopping the script with manual intervention (ctrl + c)
    trap "exit" INT

    # loop thru the file line-by-line
    while IFS= read -r line
    do
        echo ${line} | kafka-console-producer --broker-list localhost:9092 --topic $2
    done < $1
}


csvToKafka "static/customer.csv" "customer" &
csvToKafka "static/lineitem.csv" "lineitem" &
csvToKafka "static/orders.csv" "orders"