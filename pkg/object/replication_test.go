package object

import (
	"fmt"
	"os"
	"path"
	"sort"
	"testing"

	assert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type LogFileSuite struct {
	suite.Suite
}

func createLogFile(ent []LogEntry, index uint64, dir string) {
	file, _ := os.Create(path.Join(dir, fmt.Sprintf("%v.log", index)))
	file.WriteString(fmt.Sprintf("%v\n", LogVer))
	f := LogFile{LogEntries{make([]LogEntry, 0)}, file, nil, fmt.Sprintf("%v.log", index), 0, 0}
	for _, entry := range ent {
		f.AppendLog(entry)
	}
	f.Close(false)
}

func (suite *LogFileSuite) TestAppend() {
	entries := make([]LogEntry, 0)
	for i := 0; i < 100; i += 1 {
		entries = append(entries, LogEntry{LogType(i % 2), fmt.Sprintf("key_%v", i)})
	}
	file, _ := os.Create("0.log")
	file.WriteString(fmt.Sprintf("%v\n", LogVer))
	f := LogFile{LogEntries{make([]LogEntry, 0)}, file, nil, "0.log", 0, 0}
	for _, entry := range entries {
		f.AppendLog(entry)
	}
	f.Close(false)
	l := LogManager{}
	f2, _ := l.NewLogFileForRead("0.log", 0)
	f2.ReadAll(false)
	assert.Equal(suite.T(), entries, f2.entries)
}

func (suite *LogFileSuite) TestAppendTrim() {
	entries := make([]LogEntry, 0)
	expected := make([]LogEntry, 0)
	for i := 0; i < 100; i += 1 {
		entries = append(entries, LogEntry{LogType(i % 2), fmt.Sprintf("key_%v", i)})
		if i % 2 == 0 {
			expected = append(expected, entries[i])
		}
	}
	for i := 0; i < len(expected) / 2; i += 1 {
		entries = append(entries, LogEntry{Delete, expected[i].key})
	}
	expected = expected[len(expected) / 2:]
	file, _ := os.Create("0.log")
	file.WriteString(fmt.Sprintf("%v\n", LogVer))
	f := LogFile{LogEntries{make([]LogEntry, 0)}, file, nil, "0.log", 0, 0}
	for _, entry := range entries {
		f.AppendLog(entry)
	}
	f.Close(false)
	l := LogManager{}
	f2, _ := l.NewLogFileForRead("0.log", 0)
	f2.ReadAll(true)
	assert.Equal(suite.T(), expected, f2.entries)
}

func ScanDir(dir string) []string {
	ret := []string{}
	ent, _ := os.ReadDir("test")
	for _, e := range ent {
		ret = append(ret, e.Name())
	}
	return ret
}

func (suite *LogFileSuite) TestMultiAppend() {
	createEnt := func(lower int) ([]LogEntry, []LogEntry) {
		entries := make([]LogEntry, 0)
		expected := make([]LogEntry, 0)
		for i := 0; i < 100; i += 1 {
			entries = append(entries, LogEntry{LogType(i % 2), fmt.Sprintf("key_%v", i + lower)})
			if i % 2 == 0 {
				expected = append(expected, entries[i])
			}
		}
		for i := 0; i < len(expected) / 2; i += 1 {
			entries = append(entries, LogEntry{Delete, expected[i].key})
		}
		expected = expected[len(expected) / 2:]
		return entries, expected
	}
	err := os.RemoveAll("test")
	if err != nil {
		logger.Error(err)
	}
	os.Mkdir("test", os.ModePerm)
	defer os.RemoveAll("test")
	entries := make([][]LogEntry, 0)
	expected := make([]LogEntry, 0)
	for i := 0; i < 3; i += 1 {
		a, b := createEnt(i * 100)
		entries = append(entries, a)
		expected = append(expected, b...)
		createLogFile(a, uint64(i), "test")
	}
	l, _ := NewLogManager("test", 20)
	l.Init()
	for i := 0; i < 60; i += 1 {
		l.Put(fmt.Sprintf("key_%v", i + 400))
		expected = append(expected, LogEntry{Put, fmt.Sprintf("key_%v", i + 400)})
	}

	expectedFn := []string{}
	for i := 0; i <= 6; i += 1 {
		expectedFn = append(expectedFn, fmt.Sprintf("%v.log", i))
	}
	actualFn := ScanDir("test")
	sort.Slice(actualFn, func(i, j int) bool { return actualFn[i] < actualFn[j] })
	sort.Slice(expectedFn, func(i, j int) bool { return expectedFn[i] < expectedFn[j] })
	assert.Equal(suite.T(), expectedFn, actualFn)

	actual := make([]LogEntry, 0)
	for len(l.previous) > 0 {
		t := l.NextFile()
		actual = append(actual, t.Entries()...)
		l.Pop()
	}
	assert.Equal(suite.T(), expected, actual)
	l.Close()
	assert.Equal(suite.T(), []string{"6.log"}, ScanDir("test"))
}

func TestLogFileSuite(t *testing.T) {
	suite.Run(t, new(LogFileSuite))
}