package ws

import (
	"log"
	"sync"
)

type Hub struct {
	clients sync.Map
}

func (h *Hub) ClientIdAlreadyConnected(clientId, sessionId string) bool {
	exists := false
	h.clients.Range(func(key, value any) bool {
		client := key.(*WebSocketConnection)
		if client.Id == clientId && client.SessionId == sessionId{
			exists = true
			return false // finish loop
		}
		return true
	})
	return exists
}

func NewHub() *Hub {
	return &Hub{
		clients: sync.Map{},
	}
}

func (h *Hub) Register(client *WebSocketConnection) {
	h.clients.Store(client, true)
}

func (h *Hub) Unregister(client *WebSocketConnection) {
	h.clients.Delete(client)
}

func (h *Hub) ConnectedClientsCount(sessionId string) uint8 {
	var connectedClientsCount uint8

	h.clients.Range(func(key, value any) bool {
		client := key.(*WebSocketConnection)
		if client.SessionId == sessionId {
			connectedClientsCount++
		}
		return true
	})

	return connectedClientsCount
}

func (h *Hub) BroadcastSameSession(message *Message) {
	h.clients.Range(func(key, value interface{}) bool {
		client := key.(*WebSocketConnection)
		if client.SessionId == message.SessionId && client.Id != message.ClientId {
			if err := client.Conn.WriteMessage(OpText, message.Content); err != nil {
				log.Printf("[Hub] Failed to send message to client %s: %v\n", client.Id, err)
			}
		}
		return true
	})
}
