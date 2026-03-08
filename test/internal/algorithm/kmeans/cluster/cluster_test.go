package cluster

import (
	"testing"
)

func TestDistance(t *testing.T) {
	p1 := Coordinates{
		ID:     "id-p1",
		Vector: []float64{2, 2},
	}
	p2 := Coordinates{
		ID:     "id-p2",
		Vector: []float64{3, 5},
	}

	d := p1.Distance(p2.Coordinates())
	if d != 10 {
		t.Errorf("Expected distance of 10, got %f", d)
	}
}

func TestCenter(t *testing.T) {
	var o Observations
	p1 := Coordinates{
		ID:     "id-p1",
		Vector: []float64{1, 1},
	}
	p2 := Coordinates{
		ID:     "id-p2",
		Vector: []float64{3, 2},
	}
	p3 := Coordinates{
		ID:     "id-p3",
		Vector: []float64{5, 3},
	}
	o = append(o, p1, p2, p3)

	m, err := o.Center()
	if err != nil {
		t.Errorf("Could not retrieve center: %v", err)
		return
	}

	if m.Vector[0] != 3 || m.Vector[1] != 2 {
		t.Errorf("Expected coordinates [3 2], got %v", m)
	}
}

func TestAverageDistance(t *testing.T) {
	var o Observations
	p1 := Coordinates{
		ID:     "id-p1",
		Vector: []float64{1, 1},
	}
	p2 := Coordinates{
		ID:     "id-p2",
		Vector: []float64{3, 2},
	}
	p3 := Coordinates{
		ID:     "id-p3",
		Vector: []float64{5, 3},
	}
	o = append(o, p1, p2, p3)

	d := AverageDistance(o[0], o[1:])
	if d != 12.5 {
		t.Errorf("Expected average distance of 12.5, got %v", d)
	}

	d = AverageDistance(o[1], Observations{o[1]})
	if d != 0 {
		t.Errorf("Expected average distance of 0, got %v", d)
	}
}

func TestClusters(t *testing.T) {
	var o Observations
	p1 := Coordinates{
		ID:     "id-p1",
		Vector: []float64{1, 1},
	}
	p2 := Coordinates{
		ID:     "id-p2",
		Vector: []float64{3, 2},
	}
	p3 := Coordinates{
		ID:     "id-p3",
		Vector: []float64{5, 3},
	}
	o = append(o, p1, p2, p3)
	c, err := New(2, o)
	if err != nil {
		t.Errorf("Error seeding clusters: %v", err)
		return
	}

	t.Logf("Seeded clusters: %+v", c)
	if len(c) != 2 {
		t.Errorf("Expected 2 clusters, got %d", len(c))
		return
	}

	c[0].Append(o[0])
	c[1].Append(o[1])
	c[1].Append(o[2])
	c.Recenter()

	t.Logf("Seeded clusters: %+v", c)
	if n := c.Nearest(o[1]); n != 1 {
		t.Errorf("Expected nearest cluster 1, got %d", n)
	}

	nc, d := c.Neighbour(o[0], 0)
	if nc != 1 {
		t.Errorf("Expected neighbouring cluster 1, got %d", nc)
	}
	if d != 12.5 {
		t.Errorf("Expected neighbouring cluster distance 12.5, got %f", d)
	}

	if pp := c[1].PointsInDimension(0); pp.Vector[0] != 3 || pp.Vector[1] != 5 {
		t.Errorf("Expected [3 5] as points in dimension 0, got %v", pp)
	}
	if pp := c.CentersInDimension(0); pp.Vector[0] != 1 || pp.Vector[1] != 4 {
		t.Errorf("Expected [1 4] as centers in dimension 0, got %v", pp)
	}

	c.Reset()
	if len(c[0].Observations) > 0 {
		t.Errorf("Expected empty cluster 1, found %d observations", len(c[0].Observations))
	}
}
