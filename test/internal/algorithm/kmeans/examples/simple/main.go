package main

import (
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/algorithm/kmeans"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/algorithm/kmeans/cluster"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	// set up a random two-dimensional data set (float64 values between 0.0 and 1.0)
	var d cluster.Observations
	for x := 0; x < 1024; x++ {
		d = append(d, cluster.Coordinates{
			rand.Float64(),
			rand.Float64(),
		})
	}
	fmt.Printf("%d data points\n", len(d))

	// Partition the data points into 7 clusters
	km := kmeans.New()
	clusters, _ := km.Partition(d, 7)

	for i, c := range clusters {
		fmt.Printf("Cluster: %d\n", i)
		fmt.Printf("Centered at x: %.2f y: %.2f\n", c.Center[0], c.Center[1])
		fmt.Printf("Observations: length=%d %+v\n", len(c.Observations), c.Observations)
	}
}
