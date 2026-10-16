package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExampleConfigurations(t *testing.T) {
	for _, name := range []string{"local.json", "compose.json", "ollama.json"} {
		if _, err := Load(filepath.Join("..", "..", "configs", name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
func TestUnknownFieldsAndInvalidDurationsRejected(t *testing.T) {
	for _, body := range []string{`{"unknown":true}`, `{"queue_timeout":"bad"}`, `{"queue_timeout":3}`} {
		p := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(p, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(p); err == nil {
			t.Fatalf("invalid config accepted: %s", body)
		}
	}
}
