package test

import (
	"regexp"
	"strings"
	"testing"
)

func TestReverse(t *testing.T) {
	//regexpPattern := regexp.MustCompile("")
	regexpPattern := regexp.MustCompile(`^(\S+) - - \[([^]]+)] "([^"]*)" (\d+) (\d+) "([^"]*)" "([^"]*)" "([^"]*)"`)
	matches := regexpPattern.FindStringSubmatch("222.67.99.238 - - [04/Jan/2024:11:07:23 +0800] \"GET / HTTP/1.1\" 304 0 \"-\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\" \"-\"")
	strings.Trim(matches[1], "\n")
}
