// Copyright 2016 Gravitational Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rigging

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/gravitational/trace"
)

// CreateSecretFromPath creates a Kubernetes Secret of the supplied name, from the file or directory supplied as an argument.
func CreateSecretFromPath(name string, path string) ([]byte, error) {
	cmd := KubeCommand("create", "secret", "generic", "name", "--from-file="+path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}

// CreateSecretFromMap creates a Kubernetes Secret of the supplied name, with the supplied map of keys and values.
func CreateSecretFromMap(name string, values map[string]string) ([]byte, error) {
	dir, err := ioutil.TempDir("", "rigging")
	if err != nil {
		return nil, trace.Wrap(err)
	}
	defer os.RemoveAll(dir)

	for key, val := range values {
		err := ioutil.WriteFile(path.Join(dir, key), []byte(val), 0644)
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}

	cmd := KubeCommand("create", "secret", "generic", name, "--from-file="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, trace.Wrap(err)
	}
	return out, nil
}
