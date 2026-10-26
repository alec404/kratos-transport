package websocket

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinaryNetPacketMarshal(t *testing.T) {
	msg := BinaryNetPacket{Type: 100, Payload: []byte("hello")}
	buf, err := msg.Marshal()
	require.NoError(t, err)

	var out BinaryNetPacket
	require.NoError(t, out.Unmarshal(buf))
	require.Equal(t, msg.Type, out.Type)
	require.Equal(t, msg.Payload, out.Payload)
}

func TestTextNetPacketMarshal(t *testing.T) {
	msg := TextNetPacket{Type: 200, Payload: "hello"}
	buf, err := msg.Marshal()
	require.NoError(t, err)

	var out TextNetPacket
	require.NoError(t, out.Unmarshal(buf))
	require.Equal(t, msg.Type, out.Type)
	require.Equal(t, msg.Payload, out.Payload)
}
