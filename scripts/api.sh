#!/bin/bash

# URL of your endpoint
URL="http://localhost:5000/health"

# Call the endpoint and store response
response=$(curl -s "$URL")

# Extract the status field using jq
status=$(echo "$response" | jq -r '.status')

# Print header
echo "------------------------------------"
echo " Health Check Result "
echo "------------------------------------"

# Check the status value
if [[ "$status" == "success" || "$status" == "OK" ]]; then
    echo "Status: OK"
else
    echo "Status: DEGRADED"
    echo
    echo "Recent Errors:"
    echo "------------------------------------"

    # Loop through each error in the response and print it neatly
    index=1
    echo "$response" | jq -c '.errors[]' | while read -r error; do
        error_msg=$(echo "$error" | jq -r '.error')
        time=$(echo "$error" | jq -r '.time')
        echo "Error $index:"
        echo "  Time : $time"
        echo "  Detail: $error_msg"
        echo "------------------------------------"
        ((index++))
    done
fi
