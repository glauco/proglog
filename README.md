# ProgLog HTTP Server

A simple HTTP server component that provides endpoints to manage a log of records, allowing clients to produce and consume records over HTTP. This server is designed to handle concurrent requests safely.

## Features

- **Produce Records**: Add new records to the log via an HTTP POST request.
- **Consume Records**: Retrieve records from the log by their offset via an HTTP GET request.
- **Concurrency Safe**: Uses a mutex to ensure thread-safe access to the log.

## Getting Started

### Prerequisites

- Go (1.16 or later)

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/glauco/proglog.git
   cd proglog
   ```
2. Install dependencies:
   ```bash
   go mod download
   ```

### Running the Server

To start the server, navigate to the root directory and run the following command:

```bash
go run cmd/server/main.go
```

This will start the server on port `9090`.

### Usage

The server exposes two main endpoints to interact with the log:

1. Produce (Add a Record)
  - URL: `/`
  - Method: `POST`
  - Body (encoded in `base64`):
    ```json
    {
      "record": {
        "value": "SGVsbG8sIFdvcmxkCg=="
      }
    }
    ```
  - Response:
    - `200 OK`: `{ "offset": <record_offset> }` if the record is successfully added.
    - `400 Bad Request`: If the request format is invalid.
    - `500 Internal Server Error`: If there is an issue with appending the record.

2. Consume (Retrieve a Record)
  - URL: `/`
  - Method: `GET`
  - Body:
    ```json
    {
      "offset": 0
    }
    ```
  - Response:
    - `200 OK`: `{ "record": { "value": ""SGVsbG8sIFdvcmxkCg=="", "offset": 0 } }`
    - `400 Bad Request`: If the request format is invalid.
    - `500 Internal Server Error`: If the requested offset is not found or there is a server issue.
