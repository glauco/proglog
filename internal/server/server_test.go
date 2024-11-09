package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleProduce(t *testing.T) {
	srv := newHttpServer()

	// Create a sample record to produce
	reqBody := ProduceRequest{
		Record: Record{
			Value: []byte("test data"),
		},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("Failed to marshal request body: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	w := httptest.NewRecorder()

	// Call handleProduce and check response
	srv.handleProduce(w, req)
	res := w.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("Expected status OK; got %v", res.StatusCode)
	}

	var produceRes ProduceResponse
	if err := json.NewDecoder(res.Body).Decode(&produceRes); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if produceRes.Offset != 0 {
		t.Fatalf("Expected offset 0; got %v", produceRes.Offset)
	}
}

func TestHandleConsume(t *testing.T) {
	srv := newHttpServer()

	// First, produce a record to consume later
	reqBody := ProduceRequest{
		Record: Record{
			Value: []byte("test data"),
		},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("Failed to marshal request body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.handleProduce(w, req)

	// Now consume the record we just produced
	consumeReq := ConsumeRequest{Offset: 0}
	consumeBody, err := json.Marshal(consumeReq)
	if err != nil {
		t.Fatalf("Failed to marshal consume request body: %v", err)
	}
	req = httptest.NewRequest(http.MethodGet, "/", bytes.NewReader(consumeBody))
	w = httptest.NewRecorder()

	// Call handleConsume and check response
	srv.handleConsume(w, req)
	res := w.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("Expected status OK; got %v", res.StatusCode)
	}

	var consumeRes ConsumeResponse
	if err := json.NewDecoder(res.Body).Decode(&consumeRes); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if string(consumeRes.Record.Value) != "test data" {
		t.Fatalf("Expected record value 'test data'; got %v", string(consumeRes.Record.Value))
	}

	if consumeRes.Record.Offset != 0 {
		t.Fatalf("Expected record offset 0; got %v", consumeRes.Record.Offset)
	}
}

func TestHandleConsumeNotFound(t *testing.T) {
	srv := newHttpServer()

	// Try to consume a record that doesn't exist
	consumeReq := ConsumeRequest{Offset: 999}
	consumeBody, err := json.Marshal(consumeReq)
	if err != nil {
		t.Fatalf("Failed to marshal consume request body: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/", bytes.NewReader(consumeBody))
	w := httptest.NewRecorder()

	// Call handleConsume and expect an error
	srv.handleConsume(w, req)
	res := w.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("Expected status Internal Server Error; got %v", res.StatusCode)
	}
}
