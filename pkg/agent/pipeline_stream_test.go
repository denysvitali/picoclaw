package agent

import (
	"context"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/providers"
)

type streamTestProvider struct {
	chunks []string
	resp   *providers.LLMResponse
}

func (p *streamTestProvider) Chat(
	context.Context,
	[]providers.Message,
	[]providers.ToolDefinition,
	string,
	map[string]any,
) (*providers.LLMResponse, error) {
	return p.resp, nil
}

func (p *streamTestProvider) ChatStream(
	_ context.Context,
	_ []providers.Message,
	_ []providers.ToolDefinition,
	_ string,
	_ map[string]any,
	onChunk func(string),
) (*providers.LLMResponse, error) {
	for _, chunk := range p.chunks {
		onChunk(chunk)
	}
	return p.resp, nil
}

func (p *streamTestProvider) GetDefaultModel() string { return "test-model" }

type streamRecorder struct {
	updates   []string
	final     string
	cancelled bool
}

func (s *streamRecorder) Update(_ context.Context, content string) error {
	s.updates = append(s.updates, content)
	return nil
}

func (s *streamRecorder) Finalize(_ context.Context, content string) error {
	s.final = content
	return nil
}

func (s *streamRecorder) Cancel(context.Context) {
	s.cancelled = true
}

func TestStreamChatIDForTurn_TelegramTopic(t *testing.T) {
	ts := &turnState{
		channel: "telegram",
		chatID:  "-100123",
		opts: processOptions{
			Dispatch: DispatchRequest{
				InboundContext: &bus.InboundContext{
					Channel: "telegram",
					ChatID:  "-100123",
					TopicID: "42",
				},
			},
		},
	}

	if got := streamChatIDForTurn(ts); got != "-100123/42" {
		t.Fatalf("streamChatIDForTurn() = %q, want -100123/42", got)
	}
}

func TestPipelineChatWithResponseStream_FinalizesDirectResponse(t *testing.T) {
	provider := &streamTestProvider{
		chunks: []string{"part", "partial"},
		resp:   &providers.LLMResponse{Content: "partial"},
	}
	streamer := &streamRecorder{}
	p := &Pipeline{}

	resp, err := p.chatWithResponseStream(
		context.Background(),
		&turnState{channel: "telegram", chatID: "123"},
		provider,
		streamer,
		nil,
		nil,
		"test-model",
		nil,
	)
	if err != nil {
		t.Fatalf("chatWithResponseStream() error = %v", err)
	}
	if resp.Content != "partial" {
		t.Fatalf("response content = %q, want partial", resp.Content)
	}
	if len(streamer.updates) != 2 || streamer.updates[0] != "part" || streamer.updates[1] != "partial" {
		t.Fatalf("updates = %#v", streamer.updates)
	}
	exec := &turnExecution{responseStreamer: streamer}
	p.finalizeResponseStream(context.Background(), &turnState{channel: "telegram", chatID: "123"}, exec, resp.Content)
	if streamer.final != "partial" {
		t.Fatalf("final = %q, want partial", streamer.final)
	}
	if exec.responseStreamer != nil {
		t.Fatal("streamer should be cleared after finalize")
	}
	if streamer.cancelled {
		t.Fatal("stream should not be cancelled after direct final response")
	}
}

func TestPipelineChatWithResponseStream_CancelsToolCallResponse(t *testing.T) {
	provider := &streamTestProvider{
		chunks: []string{"checking"},
		resp: &providers.LLMResponse{
			Content: "checking",
			ToolCalls: []providers.ToolCall{{
				ID:   "call-1",
				Name: "read_file",
			}},
		},
	}
	streamer := &streamRecorder{}
	p := &Pipeline{}

	_, err := p.chatWithResponseStream(
		context.Background(),
		&turnState{channel: "telegram", chatID: "123"},
		provider,
		streamer,
		nil,
		nil,
		"test-model",
		nil,
	)
	if err != nil {
		t.Fatalf("chatWithResponseStream() error = %v", err)
	}
	cancelResponseStream(context.Background(), &turnExecution{responseStreamer: streamer})
	if streamer.final != "" {
		t.Fatalf("final = %q, want empty", streamer.final)
	}
	if !streamer.cancelled {
		t.Fatal("stream should be cancelled for tool-call responses")
	}
}
