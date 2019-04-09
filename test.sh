#! /usr/bin/env bash

# Here we test the basic functionalities of PicoKV, namely set and get on a running instance
# and reload from disk after a restart.

# Run picokv server in background
node server.js &

# But remember its PID so we can kill it later
kv_pid=$!

# Wait for the server to boot...
sleep 2

# We are going to memorize the last values for each test key in this hash table
declare -A last_test_values

# Put a value that we will never update. It should stay here after multiple compactions.
oldcat_value=$RANDOM

# Put some random value for key 'oldcat'
curl -s -X PUT -d "$oldcat_value" -H 'content-type: text/plain' http://localhost:9001/pkv/oldcat > /dev/null

# And memorize it inside our hash table
last_test_values[oldcat]=$oldcat_value

# For each test key we're going to generate some random value and put it into picokv
test_keys=("mew" "purr" "yawn" "scratch")

# We'll do it for a lot of times in order to trigger segment splitting and compaction
for _ in {1..500}; do
    # write all the things!
    for test_key in "${test_keys[@]}"; do
        value=$RANDOM
        last_test_values[$test_key]=$value
        curl -s -X PUT -d "$value" -H 'content-type: text/plain' http://localhost:9001/pkv/"$test_key" > /dev/null
    done
done

# We can add 'oldcat' to the test keys now, we couldn't do it earlier or its value would have been overwritten
test_keys+=("oldcat")

# expect all the keys to have the expected values
statuscode=0
for test_key in "${test_keys[@]}"; do
    result=$(curl -s http://localhost:9001/pkv/"$test_key")
    expected=${last_test_values[$test_key]}
    if [ "$result" != "$expected" ]; then
        printf 'TEST FAILED! For key %s expected %s but got %s\n' "$test_key" "$expected" "$result"
        statuscode=1
    fi
done

# If something gone wrong, cleanup and exit
if [ $statuscode == 1 ]; then
    kill -15 "$kv_pid"
    exit 1
fi

# Now we're going to test a server restart

# So we stop the picokv process...
kill -15 "$kv_pid"

# ...and start it again so we can test rebuilding indexes from disk
node server.js &
kv_pid=$!
sleep 2

# Put a new key...
meeoww_value=$RANDOM
curl -s -X PUT -d "$meeoww_value" -H 'content-type: text/plain' http://localhost:9001/pkv/meeoww > /dev/null

# ...and add it to the tests
read_tests+=('meeoww')
last_test_values[meeoww]=$meeoww_value

# Run test expectations again
for test_key in "${read_tests[@]}"; do
    result=$(curl -s http://localhost:9001/pkv/"$test_key")
    expected=${last_test_values[$test_key]}
    if [ "$result" != "$expected" ]; then
        printf 'TEST FAILED! For key %s expected %s but got %s\n' "$test_key" "$expected" "$result"
        statuscode=1
    fi
done

if [ $statuscode == 1 ]; then
    kill -15 "$kv_pid"
    exit 1
fi

# Cleanup
kill -15 "$kv_pid"

# ...aaaand we're happy! :-)
printf 'TEST OK\n'
