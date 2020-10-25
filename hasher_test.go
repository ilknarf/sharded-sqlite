package sharded

import "testing"

func TestCluster_Hash(t *testing.T) {
	const expected = 4

	c := &Cluster{
		metadata: ClusterMetadata{
			NumShards: 5,
		},
	}

	index, err := c.Hash("hello?")

	if err != nil {
		t.Error(err)
	}

	if index != expected {
		t.Logf("Index is %v, not %v", index, expected)
		t.Fail()
	}
}