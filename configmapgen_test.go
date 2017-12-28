package rigging

import (
	"io/ioutil"
	"path/filepath"
	"runtime/debug"

	"github.com/davecgh/go-spew/spew"
	"github.com/kylelemons/godebug/diff"
	. "gopkg.in/check.v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ConfigmapGenSuite struct{}

var _ = Suite(&ConfigmapGenSuite{})

func (s *ConfigmapGenSuite) TestConfigMap(c *C) {
	tcs := []struct {
		name      string
		namespace string
		files     []string
		literals  []string
		result    *v1.ConfigMap
		error     bool
	}{
		{
			name:      "cm1",
			namespace: "kube-system",
			literals:  []string{"a=b", "key=val"},
			result: &v1.ConfigMap{
				TypeMeta:   metav1.TypeMeta{Kind: KindConfigMap, APIVersion: V1},
				ObjectMeta: metav1.ObjectMeta{Name: "cm1", Namespace: "kube-system"},
				Data: map[string]string{
					"a":   "b",
					"key": "val",
				},
			},
		},
		{
			name:      "cm1",
			namespace: "kube-system",
			files:     files(c, []file{{name: "file1", value: "val1"}, {name: "text.yaml", value: "a: b"}}),
			result: &v1.ConfigMap{
				TypeMeta:   metav1.TypeMeta{Kind: KindConfigMap, APIVersion: V1},
				ObjectMeta: metav1.ObjectMeta{Name: "cm1", Namespace: "kube-system"},
				Data: map[string]string{
					"file1":     "val1",
					"text.yaml": "a: b",
				},
			},
		},
	}
	for i, tc := range tcs {
		comment := Commentf("test case %v", i+1)
		configMap, err := GenerateConfigMap(tc.name, tc.namespace, tc.files, tc.literals)
		if tc.error {
			c.Assert(err, NotNil, comment)
		} else {
			c.Assert(err, IsNil, comment)
			DeepCompare(c, configMap, tc.result)
		}
	}
}

type file struct {
	name  string
	value string
}

func files(c *C, files []file) []string {
	dir := c.MkDir()
	paths := []string{}
	for _, f := range files {
		err := ioutil.WriteFile(filepath.Join(dir, f.name), []byte(f.value), 0644)
		c.Assert(err, IsNil)
		paths = append(paths, filepath.Join(dir, f.name))
	}
	return paths
}

// DeepCompare uses gocheck DeepEquals but provides nice diff if things are not equal
func DeepCompare(c *C, a, b interface{}) {
	d := &spew.ConfigState{Indent: " ", DisableMethods: true, DisablePointerMethods: true}

	c.Assert(a, DeepEquals, b, Commentf("%v\nStack:\n%v\n", diff.Diff(d.Sdump(a), d.Sdump(b)), string(debug.Stack())))
}
