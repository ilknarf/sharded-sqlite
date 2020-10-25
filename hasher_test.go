package sharded

import "testing"

// hash string
func TestCluster_Hash(t *testing.T) {
	const v = "hello1@example.com"
	const expected = 0

	c := &Cluster{
		metadata: ClusterMetadata{
			NumShards: 5,
		},
	}

	testHashEquals(t, c, expected, v)
}

// hash int
func TestCluster_Hash2(t *testing.T) {
	const v = 2145133
	const expected = 2

	c := &Cluster{
		metadata: ClusterMetadata{
			NumShards: 5,
		},
	}

	testHashEquals(t, c, expected, v,)
}

func testHashEquals(t *testing.T, c *Cluster, expected int, v interface{}) {
	index, err := c.Hash(v)

	if err != nil {
		t.Fatal(err)
	}

	if index != expected {
		t.Logf("Index is %v, not %v", index, expected)
		t.Fail()
	}
}