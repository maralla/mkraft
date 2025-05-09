package common

import (
	"fmt"
	"os"
	"testing"
)

func TestGrpcServiceConfigDialOptionFromYAML(t *testing.T) {
	dialOption, err := GrpcServiceConfigDialOptionFromYAML("../server.yaml")
	if err != nil {
		t.Fatalf("GrpcServiceConfigDialOptionFromYAML returned an error: %v", err)
	}
	fmt.Printf("Dial option string: %s\n", dialOption)
}

func TestGrpcServiceConfigDialOptionFromYAML_FileNotFound(t *testing.T) {
	_, err := GrpcServiceConfigDialOptionFromYAML("non_existent_file.yaml")
	if err == nil {
		t.Error("Expected an error for non-existent file, but got nil")
	}
}

func TestGrpcServiceConfigDialOptionFromYAML_InvalidYAML(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test_invalid_*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	invalidYAMLContent := `
  loadBalancingPolicy: "round_robin"
  methodConfig:
	name:
	service: "example.Service"
	retryPolicy:
	maxAttempts: "invalid_number"
`
	if _, err := tempFile.Write([]byte(invalidYAMLContent)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tempFile.Close()

	_, err = GrpcServiceConfigDialOptionFromYAML(tempFile.Name())
	if err == nil {
		t.Error("Expected an error for invalid YAML, but got nil")
	}
}
