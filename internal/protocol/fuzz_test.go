package protocol

import "testing"

func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		"PING", "GET key", "SET key value with spaces",
		"REPLICA_SET key 1 node value", "\x00\xff\n", "",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		request, err := Parse(input)
		if err != nil {
			return
		}
		if request.Command == 0 {
			t.Fatal("successful parse returned zero command")
		}
		if len(request.Key) > MaxKeyBytes || len(request.Value) > MaxValueBytes {
			t.Fatalf("parser accepted oversized request: key=%d value=%d", len(request.Key), len(request.Value))
		}
	})
}
