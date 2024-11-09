package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// NewHttpServer initializes a new HTTP server with endpoints for producing and consuming log records.
// It binds to the provided address and returns a configured *http.Server instance.
func NewHttpServer(addr string) *http.Server {
	httpsrv := newHttpServer()
	r := mux.NewRouter()

	// POST endpoint for producing records
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	// GET endpoint for consuming records
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

// httpServer is a wrapper around the Log type, providing HTTP-based access to its methods.
type httpServer struct {
	Log *Log // Log instance to store and retrieve records
}

// newHttpServer creates and returns a new httpServer instance with an initialized Log.
func newHttpServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

// ProduceRequest defines the structure for incoming requests to produce a new record in the log.
type ProduceRequest struct {
	Record Record `json:"record"` // Record to be added to the log
}

// ProduceResponse defines the structure for responses to produce requests, containing the record offset.
type ProduceResponse struct {
	Offset uint64 `json:"offset"` // Offset of the newly added record in the log
}

// ConsumeRequest defines the structure for incoming requests to consume (read) a record from the log.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"` // Offset of the record to be read
}

// ConsumeResponse defines the structure for responses to consume requests, containing the requested record.
type ConsumeResponse struct {
	Record Record `json:"record"` // Record retrieved from the log
}

// handleProduce processes HTTP POST requests to add a new record to the log.
// It decodes the request, appends the record to the log, and responds with the record's offset.
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	// Decode the JSON body into a ProduceRequest struct
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		// Respond with a 400 Bad Request if decoding fails
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Append the record to the log and get its offset
	off, err := s.Log.Append(req.Record)
	if err != nil {
		// Respond with a 500 Internal Server Error if appending fails
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond with a JSON containing the offset of the new record
	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		// Respond with a 500 Internal Server Error if encoding fails
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleConsume processes HTTP GET requests to retrieve a record from the log by its offset.
// It decodes the request, retrieves the record, and responds with the record's content.
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	// Decode the JSON body into a ConsumeRequest struct
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		// Respond with a 400 Bad Request if decoding fails
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Read the record from the log using the provided offset
	rec, err := s.Log.Read(req.Offset)
	if err != nil {
		// Respond with a 500 Internal Server Error if reading fails
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond with a JSON containing the requested record
	res := ConsumeResponse{Record: rec}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		// Respond with a 500 Internal Server Error if encoding the response fails
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
