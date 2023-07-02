/*
 * JuiceFS, Copyright 2018 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package object

import (
	// "container/heap"
	// "errors"
	"fmt"
	// "hash/fnv"
	"io"
	"bufio"
	"strings"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

type LogType uint64
const (
	Put LogType = iota
	Delete
)

const (
	MaxWrite uint64 = 100000
)

type LogEntry struct {
	logType LogType
	key string
}

func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

const LogVer = 0

type LogWriter struct {}

func (_* LogWriter) Serialize(entry LogEntry, w io.Writer) (int, error) {
	s := fmt.Sprint(entry.logType) + " " + entry.key + "\n"
	return io.WriteString(w, s)
}

type LogReader struct {}

func (_* LogReader) Deserialize(r *bufio.Reader) ([]LogEntry, error) {
	entries := make([]LogEntry, 0)
	for {
		line, err := r.ReadBytes('\n');
		if err != nil {
			if err != io.EOF {
				return entries, err
			}
			return entries, nil
		}
		s := string(line)
		var t LogType
		var key string
		_, err = fmt.Sscanf(s, "%v %v\n", &t, &key)
		if err != nil {
			logger.Errorf("Failed to deserialize log entry [%v]", s)
			continue
		}
		entries = append(entries, LogEntry{t, key})
	}
}

type LogEntries struct {
	entries []LogEntry
}

func (l *LogEntries) Entries() []LogEntry {
	return l.entries
}

func (_ *LogEntries) Close(_ bool) error { return nil }

func (_ *LogEntries) String() string { return "<current>" }

type LogFile struct {
	LogEntries
	file *os.File
	reader *bufio.Reader
	path string
	version uint64
	index uint64
}

func (f *LogFile) materialize() {
	if f.reader == nil || len(f.entries) > 0 {
		return
	}
	err := f.ReadAll(true)
	if err != nil {
		logger.Error("Failed to read log entry for log file", f.path, "error is", err)
	}
}

func (f *LogFile) Close(remove bool) error {
	f.file.Close()
	f.reader = nil
	if remove {
		return os.Remove(f.path)
	}
	return nil
}

func (f *LogFile) Entries() []LogEntry {
	f.materialize()
	return f.entries
}

func (f *LogFile) String() string { return f.path }

type ReplayTask interface {
	Entries() []LogEntry
	Close(remove bool) error
	String() string
}

type LogManager struct {
	logMaxIdx uint64
	logDir string
	log *LogFile
	written uint64
	previous []ReplayTask
	m *sync.Mutex
	newFile *sync.Cond
	maxWrite uint64
}

func (m *LogManager) FilePath(idx uint64) string {
	return path.Join(m.logDir, fmt.Sprintf("%v.log", idx))
}

func NewLogManager(dir string, maxWrite uint64) (*LogManager, error) {
	m := sync.Mutex{}
	log := &LogManager{0, dir, nil, 0, make([]ReplayTask, 0), &m, sync.NewCond(&m), maxWrite}
	err := log.ScanDir()
	if err != nil {
		return nil, err
	}
	err = log.NewLogFile()
	if err != nil {
		log.Close()
		return nil, err
	}
	return log, nil
}

func (m *LogManager) ScanDir() error {
	names, err := os.ReadDir(m.logDir)
	if err != nil {
		return err
	}
	logFiles := make([]*LogFile, 0)
	for _, name := range names {
		if name.IsDir() {
			continue
		}
		if !strings.HasSuffix(name.Name(), ".log") {
			continue
		}
		var idx uint64 = 0
		_, err := fmt.Sscanf(name.Name(), "%v.log", &idx)
		m.logMaxIdx = max(idx, m.logMaxIdx)
		if err != nil {
			continue
		}
		fp := path.Join(m.logDir, name.Name())
		logFile, err := m.NewLogFileForRead(fp, idx)
		if err != nil {
			logger.Error("Failed to log file", fp, "error is", err)
			continue
		}
		logFiles = append(logFiles, logFile)
	}
	sort.Slice(logFiles, func(i int, j int) bool {
		return logFiles[i].index < logFiles[j].index
	})
	for _, item := range logFiles {
		m.previous = append(m.previous, item)
	}
	return nil
}

func (m *LogManager) NewLogFile() error {
	m.logMaxIdx += 1
	idx := m.logMaxIdx
	path := m.FilePath(idx)
	file, err := os.Create(path)
	failback := func() {
		file.Close()
		os.Remove(path)
	}
	if err != nil {
		failback()
		return err
	}
	_, err = file.WriteString(fmt.Sprintf("%v\n", LogVer))
	if err != nil {
		failback()
		return err
	}
	err = file.Sync()
	if err != nil {
		logger.Warn("Failed to sync file ", path)
	}
	if m.log != nil {
		m.log.Close(false)
		m.pushTask(m.log)
	}
	m.log = &LogFile{LogEntries{make([]LogEntry, 0)}, file, nil, path, LogVer, idx}
	m.written = 0
	return nil
}

func (m *LogManager) NewLogFileForRead(path string, idx uint64) (*LogFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(file)
	line, err := r.ReadBytes('\n')
	if err != nil {
		file.Close()
		return nil, err
	}
	version := uint64(0)
	_, err = fmt.Sscanf(string(line), "%v\n", &version)
	if err != nil {
		file.Close()
		return nil, err
	}
	return &LogFile{LogEntries{make([]LogEntry, 0)}, file, r, path, uint64(version), idx}, nil
}

func (m *LogManager) Put(key string) error {
	return m.AppendLog(LogEntry{Put, key})
}

func (m *LogManager) Delete(key string) error {
	return m.AppendLog(LogEntry{Delete, key})
}

func (m *LogManager) AppendLog(entry LogEntry) error {
	_, err := m.log.AppendLog(entry)
	m.written += 1
	if err != nil {
		return err
	}
	m.pushTask(&LogEntries{[]LogEntry{entry}})
	if m.written >= m.maxWrite {
		// push a empty task to remove file
		m.log.entries = []LogEntry{}
		err = m.NewLogFile()
		if err != nil {
			logger.Error("Failed to create new log file for write")
		}
	}
	return nil
}

func (m *LogManager) NextFile() ReplayTask {
	m.m.Lock()
	defer m.m.Unlock()
	for len(m.previous) == 0 {
		m.newFile.Wait()
	}
	return m.previous[0]
}

func (m *LogManager) pushTask(t ReplayTask) {
	m.m.Lock()
	defer m.m.Unlock()
	m.newFile.Signal()
	m.previous = append(m.previous, t)
}

func (m *LogManager) Pop() {
	m.m.Lock()
	defer m.m.Unlock()
	if len(m.previous) == 0 {
		return
	}
	f := m.previous[0]
	f.Close(true)
	m.previous = m.previous[1:]
}

func (m *LogManager) Close() {
	m.m.Lock()
	defer m.m.Unlock()
	for _, entry := range m.previous {
		entry.Close(false)
	}
	if m.log != nil {
		m.log.Close(false)
	}
}

func (f *LogFile) AppendLog(entry LogEntry) (int, error) {
	w := LogWriter{}
	count, err := w.Serialize(entry, f.file)
	if err != nil {
		return count, err
	}
	return count, f.file.Sync()
}

func (f *LogFile) ReadAll(trim bool) error {
	r := LogReader{}
	entries, err := r.Deserialize(f.reader)
	if err != nil {
		return err
	}
	f.entries = entries
	if trim {
		f.trim()
	}
	f.Close(false)
	return nil
}

func (f *LogFile) trim() {
	m := make(map[string]interface{})
	for _, entry := range f.entries {
		if entry.logType == Put {
			m[entry.key] = nil
		}
		if entry.logType == Delete {
			delete(m, entry.key)
		}
	}

	entries := make([]LogEntry, 0)
	for _, entry := range f.entries {
		if _, ok := m[entry.key]; ok {
			entries = append(entries, entry)
		}
	}
	f.entries = entries
}

type ReplicaManager struct {
	primary ObjectStorage
	slave []ObjectStorage
	log *LogManager
}

func (r *ReplicaManager) run() {
	for {
		f := r.log.NextFile()
		logger.Infof("start replaying log file %v", f.String())
		for _, entry := range f.Entries() {
			switch entry.logType {
			case Put: {
				var reader io.ReadCloser
				var err error
				for {
					reader, err = r.primary.Get(entry.key, 0, -1)
					if err != nil {
						logger.Errorf("Failed to Get key %v in log file %v, retry later", entry.key, f.String())
						time.Sleep(5 * time.Second)
						continue
					}
					break
				}
				for _, slave := range r.slave {
					for {
						err = slave.Put(entry.key, reader)
						if err != nil {
							logger.Errorf("Failed to put key %v in log file %v to slave %v, retry later", entry.key, f.String(), slave.String())
							time.Sleep(5 * time.Second)
							continue
						}
						break
					}
				}
			}
			case Delete: {
				for _, slave := range r.slave {
					for {
						err := slave.Delete(entry.key)
						if err != nil {
							logger.Errorf("Failed to delete key %v in log file %v to slave %v, retry later", entry.key, f.String(), slave.String())
							time.Sleep(5 * time.Second)
							continue
						}
						break
					}
				}
			}
			}
		}
		r.log.Pop()
	}
}

func (r *ReplicaManager) Init() {
	go r.run()
}

type replication struct {
	DefaultObjectStorage
	primary ObjectStorage
	replica ReplicaManager
}

func (s *replication) String() string {
	return fmt.Sprintf("replication%d://%s", len(s.replica.slave), s.primary)
}

func (s *replication) Create() error {
	if err := s.primary.Create(); err != nil {
		return err
	}
	for _, o := range s.replica.slave {
		if err := o.Create(); err != nil {
			return err
		}
	}
	return nil
}

// func (s *replication) pick(key string) ObjectStorage {
// 	h := fnv.New32a()
// 	_, _ = h.Write([]byte(key))
// 	i := h.Sum32() % uint32(len(s.stores))
// 	return s.stores[i]
// }

func (s *replication) Head(key string) (Object, error) {
	return s.primary.Head(key)
}

func (s *replication) Get(key string, off, limit int64) (io.ReadCloser, error) {
	return s.primary.Get(key, off, limit)
}

func (s *replication) Put(key string, body io.Reader) error {
	err := s.primary.Put(key, body)
	if err != nil {
		return err
	}
	return s.replica.log.Put(key)
}

func (s *replication) Delete(key string) error {
	return s.primary.Delete(key)
}

// const maxResults = 10000

// // ListAll on all the keys that starts at marker from object storage.
// func ListAll(store ObjectStorage, prefix, marker string) (<-chan Object, error) {
// 	return nil, notSupported
// 	// if ch, err := store.ListAll(prefix, marker); err == nil {
// 	// 	return ch, nil
// 	// } else if !errors.Is(err, notSupported) {
// 	// 	return nil, err
// 	// }

// 	// startTime := time.Now()
// 	// out := make(chan Object, maxResults)
// 	// logger.Debugf("Listing objects from %s marker %q", store, marker)
// 	// objs, err := store.List(prefix, marker, maxResults)
// 	// if err != nil {
// 	// 	logger.Errorf("Can't list %s: %s", store, err.Error())
// 	// 	return nil, err
// 	// }
// 	// logger.Debugf("Found %d object from %s in %s", len(objs), store, time.Since(startTime))
// 	// go func() {
// 	// 	lastkey := ""
// 	// 	first := true
// 	// END:
// 	// 	for len(objs) > 0 {
// 	// 		for _, obj := range objs {
// 	// 			key := obj.Key()
// 	// 			if !first && key <= lastkey {
// 	// 				logger.Errorf("The keys are out of order: marker %q, last %q current %q", marker, lastkey, key)
// 	// 				out <- nil
// 	// 				return
// 	// 			}
// 	// 			lastkey = key
// 	// 			// logger.Debugf("found key: %s", key)
// 	// 			out <- obj
// 	// 			first = false
// 	// 		}
// 	// 		// Corner case: the func parameter `marker` is an empty string("") and exactly
// 	// 		// one object which key is an empty string("") returned by the List() method.
// 	// 		if lastkey == "" {
// 	// 			break END
// 	// 		}

// 	// 		marker = lastkey
// 	// 		startTime = time.Now()
// 	// 		logger.Debugf("Continue listing objects from %s marker %q", store, marker)
// 	// 		objs, err = store.List(prefix, marker, maxResults)
// 	// 		for err != nil {
// 	// 			logger.Warnf("Fail to list: %s, retry again", err.Error())
// 	// 			// slow down
// 	// 			time.Sleep(time.Millisecond * 100)
// 	// 			objs, err = store.List(prefix, marker, maxResults)
// 	// 		}
// 	// 		logger.Debugf("Found %d object from %s in %s", len(objs), store, time.Since(startTime))
// 	// 	}
// 	// 	close(out)
// 	// }()
// 	// return out, nil
// }

// type nextKey struct {
// 	o  Object
// 	ch <-chan Object
// }

// type nextObjects struct {
// 	os []nextKey
// }

// func (s *nextObjects) Len() int           { return len(s.os) }
// func (s *nextObjects) Less(i, j int) bool { return s.os[i].o.Key() < s.os[j].o.Key() }
// func (s *nextObjects) Swap(i, j int)      { s.os[i], s.os[j] = s.os[j], s.os[i] }
// func (s *nextObjects) Push(o interface{}) { s.os = append(s.os, o.(nextKey)) }
// func (s *nextObjects) Pop() interface{} {
// 	o := s.os[len(s.os)-1]
// 	s.os = s.os[:len(s.os)-1]
// 	return o
// }

func (s *replication) ListAll(prefix, marker string) (<-chan Object, error) {
	// heads := &nextObjects{make([]nextKey, 0)}
	// for i := range s.stores {
	// 	ch, err := ListAll(s.stores[i], prefix, marker)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("list %s: %s", s.stores[i], err)
	// 	}
	// 	first := <-ch
	// 	if first != nil {
	// 		heads.Push(nextKey{first, ch})
	// 	}
	// }
	// heap.Init(heads)

	// out := make(chan Object, 1000)
	// go func() {
	// 	for heads.Len() > 0 {
	// 		n := heap.Pop(heads).(nextKey)
	// 		out <- n.o
	// 		o := <-n.ch
	// 		if o != nil {
	// 			heap.Push(heads, nextKey{o, n.ch})
	// 		}
	// 	}
	// 	close(out)
	// }()
	// return out, nil
	return nil, notSupported
}

func (s *replication) CreateMultipartUpload(key string) (*MultipartUpload, error) {
	//return s.primary.CreateMultipartUpload(key)
	return nil, notSupported
}

func (s *replication) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	// return s.primary.UploadPart(key, uploadID, num, body)
	return nil, notSupported
}

func (s *replication) AbortUpload(key string, uploadID string) {
	// s.primary.AbortUpload(key, uploadID)
	return
}

func (s *replication) CompleteUpload(key string, uploadID string, parts []*Part) error {
	// return s.primary.CompleteUpload(key, uploadID, parts)
	return notSupported
}

func NewReplication(name, endpoint, ak, sk, token []string, logDir string) (ObjectStorage, error) {
	if len(endpoint) == 0 {
		return nil, notSupported
	}
	log, err := NewLogManager(logDir, MaxWrite)
	if err != nil {
		return nil, err
	}
	stores := make([]ObjectStorage, len(endpoint) - 1)
	primary, err := CreateStorage(name[0], endpoint[0], ak[0], sk[0], token[0])
	if err != nil {
		return nil, err
	}

	for i := 1; i < len(endpoint); i += 1 {
		stores[i], err = CreateStorage(name[i], endpoint[i], ak[i], sk[i], token[i])
		if err != nil {
			return nil, err
		}
	}
	replica := ReplicaManager{primary, stores, log}
	replica.Init()
	return &replication{primary: primary, replica: replica}, nil
}
