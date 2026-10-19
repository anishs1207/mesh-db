package identity

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func TestLoadOrCreateIsStable(t *testing.T) {
	dir := t.TempDir()
	first, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || len(first) != 32 {
		t.Fatalf("identities are not stable: %q %q", first, second)
	}
	info, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("identity permissions are too broad: %v", info.Mode())
	}
}

func TestConcurrentIdentityCreationPublishesOneCompleteValue(t *testing.T) {
	dir := t.TempDir()
	const workers = 16
	identities := make(chan string, workers)
	errors := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			id, err := LoadOrCreate(dir)
			identities <- id
			errors <- err
		}()
	}
	group.Wait()
	close(identities)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	var winner string
	for id := range identities {
		if winner == "" {
			winner = id
		}
		if id != winner {
			t.Fatalf("concurrent creators returned %q and %q", winner, id)
		}
	}
	contents, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	if stored, err := validate(string(contents)); err != nil || stored != winner {
		t.Fatalf("published identity = %q, %v; want %q", stored, err, winner)
	}
}

func TestLoadRejectsCorruptIdentity(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte("not-an-id\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dir); err == nil {
		t.Fatal("expected corrupt identity to be rejected")
	}
}
