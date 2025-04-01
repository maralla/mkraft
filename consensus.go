package main

import "fmt"

// to be implemented
func appendLog(data string) {
	// todo: implement the append log function
}

// return the commitID
func commitLog(data string) int {
	fmt.Println("Committing log:", data)
	return 1
}

func resetLog() {
	// to be implemented
}

func getCurrentCommitID() int {
	return 1
}
