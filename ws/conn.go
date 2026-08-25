package ws

type WebSocketConnection struct {
	Conn      *Conn
	Id        string
	SessionId string
}

type Message struct {
	ClientId  string
	SessionId string
	Content   []byte
}

func NewWebSocketConnection(id string, sessionID string, conn *Conn) *WebSocketConnection {
	return &WebSocketConnection{
		Id:        id,
		SessionId: sessionID,
		Conn:      conn,
	}
}
