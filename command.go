package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

var commandTypes map[string]Command

func init() {
	commandTypes = map[string]Command{}
}

// 表示要在复制状态机上执行的操作。
type Command interface {
	CommandName() string
}

// CommandApply 表示将命令应用到 server 的接口
type CommandApply interface {
	Apply(Context) (interface{}, error)
}

// old interface
type deprecatedCommandApply interface {
	Apply(Server) (interface{}, error)
}

type CommandEncoder interface {
	Encode(w io.Writer) error
	Decode(r io.Reader) error
}

// newCommand 按名称创建命令的新实例。
func newCommand(name string, data []byte) (Command, error) {
	// Find the registered command
	command := commandTypes[name]
	if command == nil {
		return nil, fmt.Errorf("raft.Command: Unregistered command type: %s", name)
	}

	// Make a copy of the command.
	v := reflect.New(reflect.Indirect(reflect.ValueOf(command)).Type()).Interface()
	copy, ok := v.(Command)
	if !ok {
		panic(fmt.Sprintf("raft: Unable to copy command: %s (%v)", command.CommandName(), reflect.ValueOf(v).Kind().String()))
	}

	// If data for the command was passed in the decode it.
	if data != nil {
		if encoder, ok := copy.(CommandEncoder); ok {
			if err := encoder.Decode(bytes.NewReader(data)); err != nil {
				return nil, err
			}
		} else {
			// default json encoder
			if err := json.NewDecoder(bytes.NewReader(data)).Decode(copy); err != nil {
				return nil, err
			}
		}
	}

	return copy, nil
}

// RegisterCommand 注册 Command 到 commandTypes 中
func RegisterCommand(command Command) {
	if command == nil {
		panic("raft: Cannot register nil")
	} else if commandTypes[command.CommandName()] != nil {
		panic(fmt.Sprintf("raft: Duplicate registration: %s", command.CommandName()))
	}
	commandTypes[command.CommandName()] = command
}
