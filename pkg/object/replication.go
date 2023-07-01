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
	// "strings"
	// "time"
)

type replication struct {
	DefaultObjectStorage
	primary ObjectStorage
	replica ReplicaManager
}

type ReplicaManager struct {
	slave []ObjectStorage
}

func (r *ReplicaManager) Put(key string, body io.Reader) error {
	return nil
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
	return s.replica.Put(key, body)
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

func NewReplication(name, endpoint, ak, sk, token []string) (ObjectStorage, error) {
	if len(endpoint) < 0 {
		return nil, notSupported
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
	replica := ReplicaManager{slave: stores}
	return &replication{primary: primary, replica: replica}, nil
}
