
#!/usr/bin/env bash

HOST="127.0.0.1"
PORT="${1:-7000}"

# Open a TCP connection to the selected MeshDB node.
exec 3<>"/dev/tcp/$HOST/$PORT" || {
    echo "Error: Could not connect to MeshDB at $HOST:$PORT"
    echo "Make sure the server is running on that port."
    exit 1
}

echo "Connected to MeshDB at $HOST:$PORT"
echo "Commands: PING, SET <key> <value>, GET <key>, exit"
echo

while true; do
    read -r -p "meshdb> " command || break

    if [[ "$command" == "exit" ]]; then
        break
    fi

    printf '%s\n' "$command" >&3

    if IFS= read -r -t 5 response <&3; then
        echo "$response"
    else
        echo "Error: No response received from MeshDB."
        break
    fi
done

exec 3>&-
exec 3<&-

echo "Disconnected from MeshDB."
