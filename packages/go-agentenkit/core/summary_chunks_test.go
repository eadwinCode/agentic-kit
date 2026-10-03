package core

import (
	"strings"
	"testing"
)

func TestChunkLines_PacksInOrderUnderTheLimit(t *testing.T) {
	lines := []string{strings.Repeat("a", 400), strings.Repeat("b", 400), strings.Repeat("c", 400)}
	chunks := chunkLines(lines, 250) // two 100-token lines fit, the third does not
	if len(chunks) != 2 || !strings.HasPrefix(chunks[0], "a") || !strings.HasPrefix(chunks[1], "c") {
		t.Fatalf("chunks = %d", len(chunks))
	}
	if got := chunkLines(nil, 250); len(got) != 1 || got[0] != "" {
		t.Fatalf("empty input = %q", got)
	}
}

func TestChunkLines_ALineLargerThanAChunkKeepsItsEnds(t *testing.T) {
	line := "START" + strings.Repeat("m", 10_000) + "END"
	chunks := chunkLines([]string{line}, 500)
	if len(chunks) != 1 {
		t.Fatalf("chunks = %d", len(chunks))
	}
	got := chunks[0]
	if !strings.HasPrefix(got, "START") || !strings.HasSuffix(got, "END") || !strings.Contains(got, "[... cut ...]") {
		t.Fatal("the cut line keeps its start and end")
	}
	if estimateTokens([]byte(got)) > 520 {
		t.Fatalf("cut line is %d tokens", estimateTokens([]byte(got)))
	}
}
