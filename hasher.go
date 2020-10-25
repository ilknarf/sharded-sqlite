package sharded

import (
	"github.com/mitchellh/hashstructure"
)

// Hash uses the pleasant hashstructure library to hash arbitrary values
func (c *Cluster) Hash(v interface{}) (int, error) {
	h, err := hashstructure.Hash(v, nil)

	if err != nil {
		return -1, err
	}

	// get cluster index
	ci := int(h % uint64(c.metadata.NumShards))

	return ci, nil
}