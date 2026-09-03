// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"fmt"
	"sort"
	"strings"
)

// OverrideFileName is written next to the operator's compose file.
const OverrideFileName = "docker-compose.bloodtrail.yml"

// Override replaces a service's image and adds environment variables.
type Override struct {
	Service     string
	Image       string
	Environment map[string]string
}

// Render produces the override YAML. Keys are sorted for stable output.
func (o Override) Render() string {
	var b strings.Builder
	b.WriteString("# Written by bloodtrail. Remove this file and the COMPOSE_FILE entry in .env to undo.\n")
	b.WriteString("services:\n")
	fmt.Fprintf(&b, "  %s:\n", o.Service)
	fmt.Fprintf(&b, "    image: %s\n", o.Image)
	if len(o.Environment) > 0 {
		b.WriteString("    environment:\n")
		keys := make([]string, 0, len(o.Environment))
		for k := range o.Environment {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "      - %s=%s\n", k, o.Environment[k])
		}
	}
	return b.String()
}
