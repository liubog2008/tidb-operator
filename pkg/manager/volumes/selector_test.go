package volumes

import (
	"testing"

	. "github.com/onsi/gomega"

	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
)

func TestNewSelector(t *testing.T) {
	cases := []struct {
		desc     string
		instance string
		mt       v1alpha1.MemberType

		expected       string
		expectedHasErr bool
	}{
		{
			desc:     "selector for pd",
			instance: "aaa",
			mt:       v1alpha1.PDMemberType,

			expected: "app.kubernetes.io/component=pd,app.kubernetes.io/instance=aaa,app.kubernetes.io/managed-by=tidb-operator,app.kubernetes.io/name=tidb-cluster",
		},
		{
			desc:     "selector for tidb",
			instance: "aaa",
			mt:       v1alpha1.TiDBMemberType,

			expected: "app.kubernetes.io/component=tidb,app.kubernetes.io/instance=aaa,app.kubernetes.io/managed-by=tidb-operator,app.kubernetes.io/name=tidb-cluster",
		},
		{
			desc:     "selector for tikv",
			instance: "aaa",
			mt:       v1alpha1.TiKVMemberType,

			expected: "app.kubernetes.io/component=tikv,app.kubernetes.io/instance=aaa,app.kubernetes.io/managed-by=tidb-operator,app.kubernetes.io/name=tidb-cluster",
		},
		{
			desc:     "selector for tiflash",
			instance: "aaa",
			mt:       v1alpha1.TiFlashMemberType,

			expected: "app.kubernetes.io/component=tiflash,app.kubernetes.io/instance=aaa,app.kubernetes.io/managed-by=tidb-operator,app.kubernetes.io/name=tidb-cluster",
		},
		{
			desc:     "selector for ticdc",
			instance: "aaa",
			mt:       v1alpha1.TiCDCMemberType,

			expected: "app.kubernetes.io/component=ticdc,app.kubernetes.io/instance=aaa,app.kubernetes.io/managed-by=tidb-operator,app.kubernetes.io/name=tidb-cluster",
		},
		{
			desc:     "selector for pump",
			instance: "aaa",
			mt:       v1alpha1.PumpMemberType,

			expected: "app.kubernetes.io/component=pump,app.kubernetes.io/instance=aaa,app.kubernetes.io/managed-by=tidb-operator,app.kubernetes.io/name=tidb-cluster",
		},
	}

	sf := MustNewSelectorFactory()

	g := NewGomegaWithT(t)
	for _, c := range cases {
		s, err := sf.NewSelector(c.instance, c.mt)
		if c.expectedHasErr {
			g.Expect(err).Should(HaveOccurred())
		} else {
			g.Expect(err).Should(Succeed(), c.desc)
		}
		g.Expect(s.String()).Should(Equal(c.expected), c.desc)
	}
}
