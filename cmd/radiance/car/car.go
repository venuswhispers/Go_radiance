//go:build !lite

package car

import (
	"github.com/spf13/cobra"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar"
)

var Cmd = cobra.Command{
	Use:   "car",
	Short: "Manage IPLD Content-addressable ARchives",
	Long:  "https://ipld.io/specs/transport/car/",
}

func init() {
	Cmd.AddCommand(
		&createcar.Cmd,
	)
}
