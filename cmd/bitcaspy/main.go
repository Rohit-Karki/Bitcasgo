package main

import (
	"bitcasgo"
	"fmt"
)

func main() {
	// Initialize Bitcaspy with default config
	bc, err := bitcasgo.Init()
	if err != nil {
		fmt.Printf("Failed to initialize: %v\n", err)
		return
	}
	defer bc.Close()

	// Example usage
	key := "test_key"
	value := []byte("test_value")

	// Put a key-value pair
	if err := bc.Put(key, value); err != nil {
		fmt.Printf("Failed to put: %v\n", err)
		return
	}

	// Get the value back
	val, err := bc.Get(key)
	if err != nil {
		fmt.Printf("Failed to get: %v\n", err)
		return
	}
	fmt.Printf("Got value: %s\n", string(val))
}
