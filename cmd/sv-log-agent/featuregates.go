// Not generated: ocb leaves this file in place when it regenerates the
// distribution.

package main

import "go.opentelemetry.io/collector/featuregate"

// Gates the agent turns on by default; --feature-gates=-<id> still disables one.
var defaultGates = []string{
	// Without it filelog may save a read offset before its records reach the
	// exporter's persistent queue, so a crash loses them (contrib #35456).
	"stanza.synchronousLogEmitter",
}

func init() {
	for _, id := range defaultGates {
		if err := featuregate.GlobalRegistry().Set(id, true); err != nil {
			panic(err)
		}
	}
}
