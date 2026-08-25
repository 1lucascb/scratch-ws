package main

import (
	"log"
	"net/http"
	"time"
	"uuid"

	"scratch-ws/ws"
)

const MAX_CONNECTIONS_PER_SESSION uint8 = 2

func main() {
	hub := ws.NewHub()

	upgrader := ws.Upgrader{
		MaxMessageSize: 1024 * 1024, // 1 MB limit per frame
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins (adjust for production as needed)
		},
	}

	// WebSocket handler route
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		sessionId := r.URL.Query().Get("sessionId")
		if sessionId == "" {
			http.Error(w, "Missing sessionId query parameter", http.StatusBadRequest)
			return
		}

		// only MAX_CONNECTIONS_PER_SESSION connections per session
		if hub.ConnectedClients(sessionId) == MAX_CONNECTIONS_PER_SESSION {
			http.Error(w, "Enough clients connected to this session", http.StatusBadRequest)
			return
		}

		conn, err := upgrader.Upgrade(w, r)
		if err != nil {
			log.Printf("[HTTP] WebSocket upgrade failed: %v", err)
			return
		}
		handleClient(hub, uuid.NewV4().String(), sessionId, conn)
	})

	server := &http.Server{
		Addr:         ":8000",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Println("==================================================")
	log.Println("🚀 High-Performance WebSocket Server Running")
	log.Println("📌 URL: http://<ip>:8000")
	log.Println("📌 WebSocket Endpoint: ws://<ip>:8000/ws")
	log.Println("📦 Dependencies: 0 external packages (stdlib only)")
	log.Println("==================================================")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleClient(hub *ws.Hub, clientID string, sessionId string, conn *ws.Conn) {
	client := ws.NewWebSocketConnection(clientID, sessionId, conn)
	hub.Register(client)

	defer func() {
		_ = client.Conn.Close()
		hub.Unregister(client)
	}()

	for {
		messageType, message, err := client.Conn.ReadMessage()
		if err != nil {
			log.Printf("[Client %s] Read error: %v", clientID, err)
			break
		}

		if messageType == ws.OpText || messageType == ws.OpBinary {
			hub.BroadcastSameSession(&ws.Message{
				ClientId:  clientID,
				SessionId: sessionId,
				Content:   message,
			})
		}
	}
}
