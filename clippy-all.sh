#!/bin/bash

cargo_file="./lib/Cargo.toml"
features=$(sed -n '/\[features\]/,/^\[/p' "$cargo_file" | grep '=' | cut -d '=' -f 1)

combinations=()
for feature in $features; do
    if [ ${#combinations[@]} -eq 0 ]; then
        combinations+=("$feature")
    else
        new_combinations=()
        for combination in "${combinations[@]}"; do
            new_combinations+=("$combination $feature")
        done
        combinations+=("${new_combinations[@]}")
        combinations+=("$feature")
    fi
done

for combination in "${combinations[@]}"; do
    echo "Running Clippy with features: $combination"
    cargo clippy --no-default-features --features "$combination"
    if [ $? -ne 0 ]; then
        echo "Clippy failed with features: $combination"
        exit 1
    fi
done

echo "Clippy passed for all feature combinations."
exit 0
