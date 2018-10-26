package point

import (
	"math"

	"testing"
)

func TestUniq(t *testing.T) {
	tests := [][2][]Point{
		{
			{ // in
				Point{MetricID: 1, Time: 1478025152, Value: 1},
				Point{MetricID: 1, Time: 1478025152, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Value: 1},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025152, Value: 1},
				Point{MetricID: 1, Time: 1478025155, Value: 1},
			},
		},
		{
			{ // in
				Point{MetricID: 1, Time: 1478025152, Value: 1},
				Point{MetricID: 1, Time: 1478025152, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Value: 1},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025152, Value: 1},
				Point{MetricID: 1, Time: 1478025155, Value: 1},
			},
		},
		{
			{ // in
				Point{MetricID: 1, Time: 1478025152, Value: math.NaN()},
				Point{MetricID: 1, Time: 1478025152, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Value: 1},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025155, Value: 1},
			},
		},
	}

	for _, test := range tests {
		result := Uniq(test[0])
		AssertListEq(t, test[1], result)
	}
}

func TestCleanUp(t *testing.T) {
	tests := [][2][]Point{
		{
			{ // in
				Point{MetricID: 1, Time: 1478025152, Value: 1},
				Point{MetricID: 0, Time: 1478025152, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Value: 1},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025152, Value: 1},
				Point{MetricID: 1, Time: 1478025155, Value: 1},
			},
		},
		{
			{ // in
				Point{MetricID: 0, Time: 1478025152, Value: 1},
				Point{MetricID: 0, Time: 1478025152, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Value: 1},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025155, Value: 1},
			},
		},
		{
			{ // in
				Point{MetricID: 0, Time: 1478025152, Value: 1},
				Point{MetricID: 0, Time: 1478025152, Value: 2},
				Point{MetricID: 0, Time: 1478025155, Value: 1},
			},
			{ // out
			},
		},
		{
			{ // in
				Point{MetricID: 1, Time: 1478025152, Value: math.NaN()},
				Point{MetricID: 1, Time: 1478025152, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Value: math.NaN()},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025152, Value: 2},
			},
		},
	}

	for _, test := range tests {
		result := CleanUp(test[0])
		AssertListEq(t, test[1], result)
	}
}
