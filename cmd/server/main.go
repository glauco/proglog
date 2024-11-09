package main

import (
	"log"

	"github.com/glauco/proglog/internal/server"
)

func main() {
	// Initialize a new HTTP server instance listening on port 9090
	srv := server.NewHttpServer(":9090")
	// Start the server and log any fatal errors if the server fails to start or crashes
	log.Fatal(srv.ListenAndServe())
}
