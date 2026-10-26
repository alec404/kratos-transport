package websocket

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

// NetMessageType 表示业务消息类型编号。
type NetMessageType uint32

// MessagePayload 表示业务消息体。
type MessagePayload any

// BinaryNetPacket 是二进制帧下的网络包结构：前 4 字节为消息类型，后续为业务负载。
type BinaryNetPacket struct {
	Type    NetMessageType
	Payload []byte
}

// Marshal 将二进制网络包编码为字节切片。
func (m *BinaryNetPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, uint32(m.Type)); err != nil {
		return nil, err
	}
	buf.Write(m.Payload)
	return buf.Bytes(), nil
}

// Unmarshal 从字节切片中解析二进制网络包。
func (m *BinaryNetPacket) Unmarshal(buf []byte) error {
	network := new(bytes.Buffer)
	network.Write(buf)

	if err := binary.Read(network, binary.LittleEndian, &m.Type); err != nil {
		return err
	}

	m.Payload = append(m.Payload[:0], network.Bytes()...)
	return nil
}

// TextNetPacket 是文本帧下的网络包结构。
type TextNetPacket struct {
	Type    NetMessageType `json:"type" xml:"type"`
	Payload string         `json:"payload" xml:"payload"`
}

// Marshal 将文本网络包编码为 JSON。
func (m *TextNetPacket) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal 从 JSON 中解析文本网络包。
func (m *TextNetPacket) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, m)
}
