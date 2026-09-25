package backend

import "testing"

func TestVersionCompatible(t *testing.T) {
	cases := []struct {
		req, act string
		want     bool
	}{
		{"1.7.0", "1.7.0", true},
		{"1.7.0", "1.8.0", false},
		{"1.*", "1.7.0", true},
		{"1.*", "2.0.0", false},
	}
	for _, c := range cases {
		if got := VersionCompatible(c.req, c.act); got != c.want {
			t.Errorf("VersionCompatible(%q,%q)=%v want %v", c.req, c.act, got, c.want)
		}
	}
}
