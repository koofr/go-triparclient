package triparclient_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2/dsl/core"
	. "github.com/onsi/gomega"
)

func TestTripar(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tripar Suite")
}
