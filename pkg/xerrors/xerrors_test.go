package xerrors

import (
	"errors"
	iofs "io/fs"
	"os"
	"testing"

	pkgfs "github.com/jacktea/xgfs/pkg/fs"
)

func TestKindOf(t *testing.T) {
	wrapped := Wrap(KindPermission, "op", "", errors.New("boom"))

	testcases := []struct {
		name string
		err  error
		kind Kind
	}{
		{name: "nil", err: nil, kind: KindInvalid},
		{name: "wrapped error", err: wrapped, kind: KindPermission},
		{name: "fs not found", err: pkgfs.ErrNotFound, kind: KindNotFound},
		{name: "fs already exists", err: pkgfs.ErrAlreadyExist, kind: KindAlreadyExists},
		{name: "fs not supported", err: pkgfs.ErrNotSupported, kind: KindNotSupported},
		{name: "iofs permission", err: iofs.ErrPermission, kind: KindPermission},
		{name: "iofs exist", err: iofs.ErrExist, kind: KindAlreadyExists},
		{name: "iofs invalid", err: iofs.ErrInvalid, kind: KindInvalid},
		{name: "os not exist", err: os.ErrNotExist, kind: KindNotFound},
		{name: "unknown error defaults internal", err: errors.New("other"), kind: KindInternal},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := KindOf(tc.err); got != tc.kind {
				t.Fatalf("KindOf() = %v, want %v", got, tc.kind)
			}
		})
	}
}
