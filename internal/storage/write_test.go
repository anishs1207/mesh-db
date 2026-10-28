package storage

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

type oneByteWriter struct{ bytes.Buffer }

func (w *oneByteWriter) Write(p []byte) (int, error) {
	return w.Buffer.Write(p[:1])
}

func TestWriteFullRejectsZeroProgress(t *testing.T) {
	if err := writeFull(zeroWriter{}, []byte("record")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeFull error = %v, want io.ErrShortWrite", err)
	}
}

func TestWriteFullAcceptsPartialWrites(t *testing.T) {
	var writer oneByteWriter
	if err := writeFull(&writer, []byte("record")); err != nil {
		t.Fatal(err)
	}
	if got := writer.String(); got != "record" {
		t.Fatalf("written data = %q", got)
	}
}
