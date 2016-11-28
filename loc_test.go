package rigging

import (
	"testing"

	. "gopkg.in/check.v1"
)

func TestLoc(t *testing.T) { TestingT(t) }

type LocSuite struct{}

var _ = Suite(&LocSuite{})

func (s *LocSuite) TestParseRef(c *C) {
	tcs := []struct {
		input string
		ref   Ref
		error bool
	}{
		{
			input: "tx1",
			ref:   Ref{Kind: KindChangeset, Name: "tx1"},
		},
		{
			input: "ds/ds1",
			ref:   Ref{Kind: KindDaemonSet, Name: "ds1"},
		},
		{
			input: "ds ds1",
			ref:   Ref{Kind: KindDaemonSet, Name: "ds1"},
		},
		{
			input: "daemonsets/ds1",
			ref:   Ref{Kind: KindDaemonSet, Name: "ds1"},
		},
	}
	for i, tc := range tcs {
		comment := Commentf("test case %v", i+1)
		ref, err := ParseRef(tc.input)
		if tc.error {
			c.Assert(err, NotNil, comment)
		} else {
			c.Assert(err, IsNil, comment)
			c.Assert(*ref, Equals, tc.ref, comment)
		}
	}
}
