package broker

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"
	_ "github.com/go-kratos/kratos/v2/encoding/proto"
)

// Marshal 使用指定 codec 将消息编码为字节切片。
// 当 codec 为空时，会优先兼容 []byte / string，并回退到 gob 编码。
func Marshal(codec encoding.Codec, msg any) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}

	if codec != nil {
		return codec.Marshal(msg)
	}

	switch t := msg.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	default:
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(msg); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
}

// Unmarshal 使用指定 codec 将字节切片解码到目标对象。
// 当 codec 为空时，会优先兼容 *[]byte / *string，并回退到 gob 解码。
func Unmarshal(codec encoding.Codec, inputData []byte, outValue any) error {
	if inputData == nil {
		return errors.New("inputData is nil")
	}
	if outValue == nil {
		return errors.New("outValue is nil")
	}

	if codec != nil {
		return codec.Unmarshal(inputData, outValue)
	}

	switch v := outValue.(type) {
	case *[]byte:
		*v = make([]byte, len(inputData))
		copy(*v, inputData)
		return nil
	case *string:
		*v = string(inputData)
		return nil
	default:
		buf := bytes.NewBuffer(inputData)
		dec := gob.NewDecoder(buf)
		return dec.Decode(outValue)
	}
}
