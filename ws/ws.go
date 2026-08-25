package ws

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// WebSocket Frame Opcodes (RFC 6455)
const (
	OpContinuation = 0x0
	OpText         = 0x1
	OpBinary       = 0x2
	OpClose        = 0x8
	OpPing         = 0x9
	OpPong         = 0xA
)

const (
	websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	defaultMaxMsg = 1024 * 1024 // 1 MB max frame size
)

// Upgrader handles upgrading HTTP connection to WebSocket connection.
type Upgrader struct {
	CheckOrigin    func(r *http.Request) bool
	MaxMessageSize uint64
}

// Conn represents a low-overhead WebSocket connection.
type Conn struct {
	rawConn        net.Conn
	r              *bufio.Reader
	w              *bufio.Writer
	writeMu        sync.Mutex
	closeOnce      sync.Once
	closed         bool
	maxMessageSize uint64
}

// Upgrade upgrades the HTTP connection to a WebSocket connection.
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return nil, errors.New("websocket: request method must be GET")
	}

	if !headerContains(r.Header, "Upgrade", "websocket") {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return nil, errors.New("websocket: missing or invalid Upgrade header")
	}

	if !headerContains(r.Header, "Connection", "upgrade") {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return nil, errors.New("websocket: missing or invalid Connection header")
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return nil, errors.New("websocket: missing Sec-WebSocket-Key header")
	}

	if u.CheckOrigin != nil && !u.CheckOrigin(r) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return nil, errors.New("websocket: origin not allowed")
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return nil, errors.New("websocket: response writer does not support hijacking")
	}

	netConn, bufrw, err := hj.Hijack()
	if err != nil {
		return nil, fmt.Errorf("websocket hijack error: %w", err)
	}

	acceptKey := computeAcceptKey(key)

	// Send Handshake response
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"

	if _, err := bufrw.WriteString(resp); err != nil {
		netConn.Close()
		return nil, err
	}
	if err := bufrw.Flush(); err != nil {
		netConn.Close()
		return nil, err
	}

	maxMsg := u.MaxMessageSize
	if maxMsg == 0 {
		maxMsg = defaultMaxMsg
	}

	return &Conn{
		rawConn:        netConn,
		r:              bufrw.Reader,
		w:              bufrw.Writer,
		maxMessageSize: maxMsg,
	}, nil
}

// ReadMessage reads a complete text or binary message, handling ping/pong/close automatically.
func (c *Conn) ReadMessage() (messageType byte, payload []byte, err error) {
	var accumulated []byte
	var firstOpcode byte

	for {
		fin, opcode, msgPayload, err := c.readFrame()
		if err != nil {
			return 0, nil, err
		}

		switch opcode {
		case OpPing:
			// Auto-respond with Pong
			_ = c.WriteMessage(OpPong, msgPayload)
			continue
		case OpPong:
			// Ignore Pong control frames
			continue
		case OpClose:
			// Send close confirmation and return EOF
			_ = c.WriteClose(1000, "normal closure")
			_ = c.Close()
			return OpClose, nil, io.EOF
		case OpText, OpBinary:
			if firstOpcode == 0 {
				firstOpcode = opcode
			}
			accumulated = append(accumulated, msgPayload...)
		case OpContinuation:
			if firstOpcode == 0 {
				return 0, nil, errors.New("websocket: unexpected continuation frame")
			}
			accumulated = append(accumulated, msgPayload...)
		default:
			return 0, nil, fmt.Errorf("websocket: unknown opcode 0x%x", opcode)
		}

		if fin {
			return firstOpcode, accumulated, nil
		}
	}
}

// readFrame reads a single RFC 6455 frame from the connection.
func (c *Conn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	var header [2]byte
	if _, err := io.ReadFull(c.r, header[:]); err != nil {
		return false, 0, nil, err
	}

	fin = (header[0] & 0x80) != 0
	opcode = header[0] & 0x0F
	masked := (header[1] & 0x80) != 0
	payloadLen := uint64(header[1] & 0x7F)

	switch payloadLen {
	case 126:
		var lenBuf [2]byte
		if _, err := io.ReadFull(c.r, lenBuf[:]); err != nil {
			return false, 0, nil, err
		}
		payloadLen = uint64(binary.BigEndian.Uint16(lenBuf[:]))
	case 127:
		var lenBuf [8]byte
		if _, err := io.ReadFull(c.r, lenBuf[:]); err != nil {
			return false, 0, nil, err
		}
		payloadLen = binary.BigEndian.Uint64(lenBuf[:])
	}

	if c.maxMessageSize > 0 && payloadLen > c.maxMessageSize {
		return false, 0, nil, fmt.Errorf("websocket: payload length %d exceeds max limit %d", payloadLen, c.maxMessageSize)
	}

	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(c.r, maskKey[:]); err != nil {
			return false, 0, nil, err
		}
	}

	payload = make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return false, 0, nil, err
		}
	}

	if masked {
		for i := uint64(0); i < payloadLen; i++ {
			payload[i] ^= maskKey[i%4]
		}
	}

	return fin, opcode, payload, nil
}

// WriteTextMessage sends a text frame to the client.
func (c *Conn) WriteTextMessage(text string) error {
	return c.WriteMessage(OpText, []byte(text))
}

// WriteBinaryMessage sends a binary frame to the client.
func (c *Conn) WriteBinaryMessage(data []byte) error {
	return c.WriteMessage(OpBinary, data)
}

// WriteMessage writes an unmasked frame (Server -> Client).
func (c *Conn) WriteMessage(messageType byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closed {
		return errors.New("websocket: connection closed")
	}

	length := len(payload)
	var header []byte

	b0 := byte(0x80) | (messageType & 0x0F) // FIN bit + opcode

	if length <= 125 {
		header = []byte{b0, byte(length)}
	} else if length <= 65535 {
		header = make([]byte, 4)
		header[0] = b0
		header[1] = 126
		binary.BigEndian.PutUint16(header[2:], uint16(length))
	} else {
		header = make([]byte, 10)
		header[0] = b0
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:], uint64(length))
	}

	if _, err := c.w.Write(header); err != nil {
		return err
	}
	if length > 0 {
		if _, err := c.w.Write(payload); err != nil {
			return err
		}
	}
	return c.w.Flush()
}

// WriteClose sends a close frame to the client.
func (c *Conn) WriteClose(statusCode uint16, reason string) error {
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload[0:2], statusCode)
	copy(payload[2:], reason)
	return c.WriteMessage(OpClose, payload)
}

// Close closes the underlying TCP connection.
func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed = true
		err = c.rawConn.Close()
	})
	return err
}

// SetReadDeadline sets the read deadline on the underlying connection.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.rawConn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline on the underlying connection.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.rawConn.SetWriteDeadline(t)
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.rawConn.RemoteAddr()
}

func computeAcceptKey(key string) string {
	h := sha1.New()
	h.Write([]byte(key + websocketGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func headerContains(h http.Header, name, value string) bool {
	for _, v := range h[name] {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), value) {
				return true
			}
		}
	}
	return false
}
