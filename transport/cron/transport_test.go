package cron

import "testing"

func TestTransport_Kind(t *testing.T) {
	o := &Transport{}
	if got := o.Kind().String(); got != KindCron {
		t.Errorf("expect %v, got %v", KindCron, got)
	}
}

func TestTransport_Endpoint(t *testing.T) {
	v := "hello"
	o := &Transport{endpoint: v}
	if got := o.Endpoint(); got != v {
		t.Errorf("expect %v, got %v", v, got)
	}
}

func TestTransport_Operation(t *testing.T) {
	v := "hello"
	o := &Transport{operation: v}
	if got := o.Operation(); got != v {
		t.Errorf("expect %v, got %v", v, got)
	}
}
