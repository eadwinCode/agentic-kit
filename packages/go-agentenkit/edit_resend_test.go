package agentenkit_test

import (
	"testing"

	agentenkit "github.com/eadwinCode/agentic-kit/packages/go-agentenkit"
)

func TestEditResend_ReplacesTheTurnAndAnswersAgain(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "first answer"}, step{text: "second answer"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "original"})
	h.handleNext(t)
	userMsg := h.storage.MessageRows(ran.ThreadID)[0]

	edited := h.run(t, chat, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "edited", EditMessageID: userMsg.ID})
	if edited.RunID == ran.RunID {
		t.Fatal("an edit is a fresh run with a new id")
	}
	h.handleNext(t)
	rows := h.storage.MessageRows(ran.ThreadID)
	mustStrings(t, h.roles(ran.ThreadID), []string{"user", "assistant"}, "history rewritten")
	mustEqual(t, string(rows[0].Content), `"edited"`, "edited prompt")
	if rows[0].ID == userMsg.ID {
		t.Fatal("the edited turn is a new row")
	}
	dropped := h.events(ran.ThreadID, "MESSAGES_DROPPED")
	mustEqual(t, len(dropped), 1, "other clients are told")
	mustEqual(t, payload(dropped[0])["fromMessageId"], userMsg.ID, "from")
}

func TestEditResend_RefusesAnythingButAUserTurn(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "answer"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "q"})
	h.handleNext(t)
	assistant := h.storage.MessageRows(ran.ThreadID)[1]
	res, _ := chat.Run(h.ctx, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "x", EditMessageID: assistant.ID})
	mustEqual(t, res.Error, "Only a user message can be edited", "assistant turn")
	res, _ = chat.Run(h.ctx, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "x", EditMessageID: "nope"})
	mustEqual(t, res.Error, "Message not found", "unknown id")
	mustEqual(t, len(h.storage.MessageRows(ran.ThreadID)), 2, "thread left alone")
}

func TestEditResend_IsRefusedWhileARunIsLive(t *testing.T) {
	h := makeRuntime(t, scripted(step{text: "answer"}))
	chat := h.rt.CreateStreamTextAgent(agentenkit.StreamTextAgentSpec{Name: "chat"})
	ran := h.run(t, chat, agentenkit.RunInput{Prompt: "q"})
	userMsg := h.storage.MessageRows(ran.ThreadID)[0]
	res, _ := chat.Run(h.ctx, agentenkit.RunInput{ThreadID: ran.ThreadID, Prompt: "x", EditMessageID: userMsg.ID})
	mustEqual(t, res.Error, "Thread has an active run", "refused")
}

func TestMessagesDeleteFrom_DropsTheSuffixAndReportsTheCount(t *testing.T) {
	h := makeRuntime(t, scripted())
	sc := agentenkit.StorageContext{}
	th, _ := h.storage.Threads().Create(h.ctx, agentenkit.ThreadInit{}, sc)
	var ids []string
	for _, txt := range []string{"a", "b", "c"} {
		m, _ := h.storage.Messages().Append(h.ctx, th.ID, agentenkit.NewMessage{Role: agentenkit.RoleUser, Content: agentenkit.TextContent(txt)}, sc)
		ids = append(ids, m.ID)
	}
	n, _ := h.storage.Messages().DeleteFrom(h.ctx, th.ID, ids[1], sc)
	mustEqual(t, n, 2, "deleted")
	mustEqual(t, len(h.storage.MessageRows(th.ID)), 1, "left")
	n, _ = h.storage.Messages().DeleteFrom(h.ctx, th.ID, "nope", sc)
	mustEqual(t, n, 0, "unknown id")
}
