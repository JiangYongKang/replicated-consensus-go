package raft

import "os"

func readFileBytes(path string) ([]byte, error)  { return os.ReadFile(path) }
func writeFileBytes(path string, b []byte) error { return os.WriteFile(path, b, 0o600) }
