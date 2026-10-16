package protocol

// Request is the gateway API, intentionally smaller than the Ollama API.
type Request struct {
	Model     string `json:"model,omitempty"`
	Prompt    string `json:"prompt"`
	MaxTokens int    `json:"max_tokens,omitempty"`
	Priority  int    `json:"priority,omitempty"`
	Stream    *bool  `json:"stream,omitempty"`
}
type Chunk struct {
	Model        string `json:"model,omitempty"`
	Response     string `json:"response,omitempty"`
	Done         bool   `json:"done"`
	Error        string `json:"error,omitempty"`
	PromptTokens int    `json:"prompt_eval_count,omitempty"`
	OutputTokens int    `json:"eval_count,omitempty"`
}
