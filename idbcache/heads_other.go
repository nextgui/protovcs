//go:build !(js && wasm)

package idbcache

import (
	"context"

	protovcs "github.com/nextgui/protovcs"
)

func (h *headMarks) Load(context.Context) (map[string]protovcs.CommitID, error) {
	return nil, errNoBrowser
}

func (h *headMarks) CompareAndSwap(context.Context, string, protovcs.CommitID, protovcs.CommitID) (protovcs.CommitID, error) {
	return protovcs.CommitID{}, errNoBrowser
}
