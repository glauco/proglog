package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
)

func TestHandleProduce(t *testing.T) {
	srv := newHttpServer()

	// Create a sample record to produce
	reqBody := ProduceRequest{
		Record: Record{
			Value: write,
		},
	}
	body, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	w := httptest.NewRecorder()

	// Call handleProduce and check response
	srv.handleProduce(w, req)
	res := w.Result()
	defer res.Body.Close()

	require.Equal(t, http.StatusOK, res.StatusCode)
	var produceRes ProduceResponse
	require.NoError(t, json.NewDecoder(res.Body).Decode(&produceRes))
	require.Equal(t, uint64(0), produceRes.Offset)
}

func TestHandleConsume(t *testing.T) {
	srv := newHttpServer()

	// First, produce a record to consume later
	reqBody := ProduceRequest{
		Record: Record{
			Value: write,
		},
	}
	body, err := json.Marshal(reqBody)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.handleProduce(w, req)

	// Now consume the record we just produced
	consumeReq := ConsumeRequest{Offset: 0}
	consumeBody, err := json.Marshal(consumeReq)
	require.NoError(t, err)
	req = httptest.NewRequest(http.MethodGet, "/", bytes.NewReader(consumeBody))
	w = httptest.NewRecorder()

	// Call handleConsume and check response
	srv.handleConsume(w, req)
	res := w.Result()
	defer res.Body.Close()

	require.Equal(t, http.StatusOK, res.StatusCode)
	var consumeRes ConsumeResponse
	require.NoError(t, json.NewDecoder(res.Body).Decode(&consumeRes))
	require.Equal(t, string(write), string(consumeRes.Record.Value))
	require.Equal(t, uint64(0), consumeRes.Record.Offset)
}

func TestHandleConsumeNotFound(t *testing.T) {
	srv := newHttpServer()

	// Try to consume a record that doesn't exist
	consumeReq := ConsumeRequest{Offset: 999}
	consumeBody, err := json.Marshal(consumeReq)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/", bytes.NewReader(consumeBody))
	w := httptest.NewRecorder()

	// Call handleConsume and expect an error
	srv.handleConsume(w, req)
	res := w.Result()
	defer res.Body.Close()

	require.Equal(t, http.StatusInternalServerError, res.StatusCode)
}
