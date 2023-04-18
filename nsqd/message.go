package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID // 唯一id
	Body      []byte
	Timestamp int64  // 时间戳 8byte
	Attempts  uint16 // 重发次数 2byte

	// for in-flight handling
	deliveryTS time.Time // 入inFightQueue时长
	clientID   int64
	pri        int64 // 优先级
	index      int   // 优先级下标
	deferred   time.Duration
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// WriteTo 写msg的ID、Body、Timestamp、Attempts
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64
	//binary.Write(w,binary.BigEndian,m.Timestamp) -> 第三个参数的data大小需要是固定大小
	// 这样写是为了减少Write内部判断 减少耗时
	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// decodeMessage deserializes data (as []byte) and creates a new Message
//
//	[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
//	|       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
//	|       8-byte         ||    ||                 16-byte                      || N-byte
//	------------------------------------------------------------------------------------------...
//	  nanosecond timestamp    ^^                   message ID                       message body
//	                       (uint16)
//	                        2-byte
//	                       attempts
func decodeMessage(b []byte) (*Message, error) { // 解码消息
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])
	msg.Body = b[10+MsgIDLength:]

	return &msg, nil
}

func writeMessageToBackend(msg *Message, bq BackendQueue) error {
	buf := bufferPoolGet()
	defer bufferPoolPut(buf)
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())
}
