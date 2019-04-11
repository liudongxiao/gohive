package hive

import "testing"

var host = "192.168.10.60:10000"

func TestSession(t *testing.T) {

	session, err := NewSession(host, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	op, err := session.Submit("show tables")
	if err != nil {
		t.Fatal(err)
	}

	if err := session.CloseOperation(op); err != nil {
		t.Fatal(err)
	}

	if err := session.CloseOperation(op); err == nil {
		t.Fatal("should unable to close operation")
	}
}
