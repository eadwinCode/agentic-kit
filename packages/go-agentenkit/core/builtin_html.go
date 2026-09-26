package core

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// Our own HTML to markdown, for the page reader. Deliberately small: a fixed
// list of rules the TS runtime follows word for word (core/builtin/html.ts),
// so the same page reads the same in both (packages/parity/tools/page-reader
// holds the cases). It is not a full HTML parser; a page it reads badly is
// what the Jina reader is for.

// htmlDrop are the elements dropped with everything inside them: code,
// chrome and forms.
var htmlDrop = []string{
	"script", "style", "noscript", "template", "svg", "head", "nav", "footer", "header", "aside", "form",
	"iframe", "button", "select", "canvas",
}

// htmlBlock are the elements that end a block of text.
var htmlBlock = []string{
	"p", "div", "section", "article", "main", "ul", "ol", "table", "tr", "blockquote", "pre", "figure",
	"figcaption", "dl", "dt", "dd", "hr",
}

var htmlEntities = map[string]string{
	"amp": "&", "lt": "<", "gt": ">", "quot": `"`, "apos": "'", "nbsp": " ", "ndash": "–", "mdash": "—",
	"hellip": "…", "lsquo": "‘", "rsquo": "’", "ldquo": "“", "rdquo": "”", "copy": "©",
}

// The spaces this reader knows, spelled out: Go's \s and JavaScript's
// differ, and the two runtimes must read a page the same.
const (
	htmlSpace = `[ \t\r\f\v\x{00a0}]`   // within a line
	htmlWS    = `[ \t\n\r\f\v\x{00a0}]` // across lines
)

var (
	reSpaceRun  = regexp.MustCompile(htmlSpace + `+`)
	reWSRun     = regexp.MustCompile(htmlWS + `+`)
	reWSEnds    = regexp.MustCompile(`^` + htmlWS + `+|` + htmlWS + `+$`)
	reTag       = regexp.MustCompile(`<[^>]*>`)
	reEntity    = regexp.MustCompile(`&(#[xX][0-9a-fA-F]+|#[0-9]+|[a-zA-Z]+);`)
	reComment   = regexp.MustCompile(`<!--[\s\S]*?-->`)
	reManyLines = regexp.MustCompile(`\n{3,}`)
	reLink      = regexp.MustCompile(`(?i)<a\s[^>]*?href\s*=\s*"([^"]*)"[^>]*>([\s\S]*?)</a\s*>`)
	reLi        = regexp.MustCompile(`(?i)<li(?:\s[^>]*)?>`)
	reBr        = regexp.MustCompile(`(?i)<br\s*/?>`)
	reCell      = regexp.MustCompile(`(?i)</?(?:td|th)(?:\s[^>]*)?>`)
	reBadScheme = regexp.MustCompile(`(?i)^(javascript|mailto|tel|data):`)
	reInner     = map[string]*regexp.Regexp{}
	reDrop      = map[string][2]*regexp.Regexp{}
	reBlock     = map[string]*regexp.Regexp{}
	reHeading   [7]*regexp.Regexp
	reHeadTag   [7]*regexp.Regexp
)

func init() {
	for _, tag := range []string{"title", "article", "main", "body"} {
		reInner[tag] = regexp.MustCompile(`(?i)<` + tag + `(?:\s[^>]*)?>([\s\S]*?)</` + tag + `\s*>`)
	}
	for _, tag := range htmlDrop {
		reDrop[tag] = [2]*regexp.Regexp{
			regexp.MustCompile(`(?i)<` + tag + `(?:\s[^>]*)?>[\s\S]*?</` + tag + `\s*>`),
			regexp.MustCompile(`(?i)<` + tag + `(?:\s[^>]*)?/?>`),
		}
	}
	for _, tag := range htmlBlock {
		reBlock[tag] = regexp.MustCompile(`(?i)</?` + tag + `(?:\s[^>]*)?/?>`)
	}
	for n := 1; n <= 6; n++ {
		h := strconv.Itoa(n)
		reHeading[n] = regexp.MustCompile(`(?i)<h` + h + `(?:\s[^>]*)?>([\s\S]*?)</h` + h + `\s*>`)
		reHeadTag[n] = regexp.MustCompile(`(?i)</?h` + h + `(?:\s[^>]*)?>`)
	}
}

func trimWS(s string) string { return reWSEnds.ReplaceAllString(s, "") }

// oneLine is text with its tags taken out, on one line.
func oneLine(s string) string {
	return trimWS(reWSRun.ReplaceAllString(reTag.ReplaceAllString(s, ""), " "))
}

// DecodeEntities turns the HTML entities the reader knows into text; any
// other is left as it is.
func DecodeEntities(s string) string {
	return reEntity.ReplaceAllStringFunc(s, func(whole string) string {
		name := whole[1 : len(whole)-1]
		if name[0] == '#' {
			var code int64
			var err error
			if name[1] == 'x' || name[1] == 'X' {
				code, err = strconv.ParseInt(name[2:], 16, 64)
			} else {
				code, err = strconv.ParseInt(name[1:], 10, 64)
			}
			if err != nil || code <= 0 || code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff) {
				return whole
			}
			return string(rune(code))
		}
		if v, ok := htmlEntities[strings.ToLower(name)]; ok {
			return v
		}
		return whole
	})
}

// tidy collapses spaces on each line, drops empty runs of lines, and trims.
func tidy(s string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = trimWS(reSpaceRun.ReplaceAllString(l, " "))
	}
	return trimWS(reManyLines.ReplaceAllString(strings.Join(lines, "\n"), "\n\n"))
}

func innerOf(html, tag string) (string, bool) {
	m := reInner[tag].FindStringSubmatch(html)
	if m == nil {
		return "", false
	}
	return m[1], true
}

func resolveHref(href, base string) (string, bool) {
	h := trimWS(DecodeEntities(href))
	if h == "" || strings.HasPrefix(h, "#") || reBadScheme.MatchString(h) {
		return "", false
	}
	b, err := url.Parse(base)
	if err != nil {
		return "", false
	}
	ref, err := url.Parse(h)
	if err != nil {
		return "", false
	}
	u := b.ResolveReference(ref)
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", false
	}
	// As a browser writes it: lower-case host, and "/" for an empty path.
	u.Host = strings.ToLower(u.Host)
	if u.Path == "" && u.Opaque == "" {
		u.Path = "/"
	}
	return u.String(), true
}

// ReadPage is a page's title and its main text.
type ReadPage struct {
	Title   string
	Content string
}

// HTMLToText is a page's title and its main text, as markdown or plain text
// (format "text").
func HTMLToText(html, baseURL, format string) ReadPage {
	title := ""
	if raw, ok := innerOf(html, "title"); ok {
		title = oneLine(DecodeEntities(raw))
	}

	s := reComment.ReplaceAllString(html, "")
	// The main part of the page: the first article, else main, else body.
	for _, tag := range []string{"article", "main", "body"} {
		if in, ok := innerOf(s, tag); ok {
			s = in
			break
		}
	}
	for _, tag := range htmlDrop {
		s = reDrop[tag][0].ReplaceAllString(s, "")
		s = reDrop[tag][1].ReplaceAllString(s, "")
	}

	if format != "text" {
		for n := 1; n <= 6; n++ {
			hashes := strings.Repeat("#", n)
			s = reHeading[n].ReplaceAllStringFunc(s, func(m string) string {
				text := reHeading[n].FindStringSubmatch(m)[1]
				return "\n\n" + hashes + " " + oneLine(text) + "\n\n"
			})
		}
		s = reLink.ReplaceAllStringFunc(s, func(m string) string {
			sub := reLink.FindStringSubmatch(m)
			label := oneLine(sub[2])
			if link, ok := resolveHref(sub[1], baseURL); ok && label != "" {
				return "[" + label + "](" + link + ")"
			}
			return label
		})
		s = reLi.ReplaceAllString(s, "\n- ")
	} else {
		for n := 1; n <= 6; n++ {
			s = reHeadTag[n].ReplaceAllString(s, "\n\n")
		}
		s = reLi.ReplaceAllString(s, "\n")
	}
	s = reBr.ReplaceAllString(s, "\n")
	for _, tag := range htmlBlock {
		s = reBlock[tag].ReplaceAllString(s, "\n\n")
	}
	s = reCell.ReplaceAllString(s, " ")
	s = reTag.ReplaceAllString(s, "")
	return ReadPage{Title: title, Content: tidy(DecodeEntities(s))}
}
