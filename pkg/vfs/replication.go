package vfs

import (
	"syscall"

	"encoding/json"

	"github.com/juicedata/juicefs/pkg/meta"
)

type ReplicateSet = meta.ReplicationSet

const ReplicateXattrName = "__Replicate__"

type ReplicateXattr struct {
	replica []ReplicateSet
}

func (a *ReplicateXattr) Marshal() ([]byte, error) {
	return json.Marshal(a)
}

func (a *ReplicateXattr) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, *a)
}

func ReadReplicateXattr(ctx Context, ino Ino, m meta.Meta) (*ReplicateXattr, syscall.Errno) {
	vbuf := make([]byte, 0)
	err := m.GetXattr(ctx, ino, ReplicateXattrName, &vbuf)
	if err != 0 && err != meta.ENOATTR {
		return nil, err
	}
	var r *ReplicateXattr = nil
	if err == 0 {
		r = new(ReplicateXattr)
		if Err := r.Unmarshal(vbuf); Err != nil {
			r = nil
			logger.Errorf("Cannot marshal inode %v replicate xattr: %v", ino, vbuf)
		}
	}
	return r, 0
}

func WriteReplicateXattr(ctx Context, ino Ino, m meta.Meta, r ReplicateXattr, flags uint32) syscall.Errno {
	vbuf, err := r.Marshal()
	if err != nil {
		panic("233")
	}

	logger.Info("set replica xattr for ino %v %v", ino, vbuf)

	return m.SetXattr(ctx, ino, ReplicateXattrName, vbuf, flags)
}