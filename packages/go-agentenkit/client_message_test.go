package agentenkit_test

import (
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

// Workstream J: the sending client names its turn, and the name comes back
// on MESSAGE_APPENDED so it swaps its optimistic copy by id. The same case
// runs in the TS package (test/client-message.test.ts).
func TestRun_TheClientsMessageIDComesBackOnItsTurn(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "ok"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "hi", ClientMessageID: "cm-1"})
	appended := h.events(ran.ThreadID, "MESSAGE_APPENDED")
	mustEqual(t, len(appended), 1, "the user's turn")
	mustEqual(t, payload(appended[0])["clientMessageId"], "cm-1", "carries the client's name for it")
}
