package triparclient_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestTripar(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tripar Suite")
}
