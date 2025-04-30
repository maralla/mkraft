package raft

import (
	"fmt"
	"testing"
)

type DummyIface interface {
	Dummay()
}

type DummyImpl struct {
}

func (d *DummyImpl) Dummay() {
	fmt.Println("DummyImpl Dummay called")
}

func TestPrintIface(t *testing.T) {
	var iface DummyIface = &DummyImpl{}
	fmt.Println(iface)
}
func TestCalculateIfMajorityMet(t *testing.T) {
	tests := []struct {
		total               int
		peerVoteAccumulated int
		expected            bool
	}{
		{total: 5, peerVoteAccumulated: 3, expected: true},
		{total: 5, peerVoteAccumulated: 2, expected: false},
		{total: 3, peerVoteAccumulated: 2, expected: true},
		{total: 3, peerVoteAccumulated: 1, expected: false},
	}

	for _, test := range tests {
		result := calculateIfMajorityMet(test.total, test.peerVoteAccumulated)
		if result != test.expected {
			t.Errorf("calculateIfMajorityMet(%d, %d) = %v; want %v", test.total, test.peerVoteAccumulated, result, test.expected)
		}
	}
}

func TestCalculateIfAlreadyFail(t *testing.T) {
	tests := []struct {
		total               int
		peersCount          int
		peerVoteAccumulated int
		voteFailed          int
		expected            bool
	}{
		{total: 5, peersCount: 4, peerVoteAccumulated: 2, voteFailed: 1, expected: false},
		{total: 5, peersCount: 4, peerVoteAccumulated: 1, voteFailed: 2, expected: false},
		{total: 5, peersCount: 4, peerVoteAccumulated: 1, voteFailed: 3, expected: true},

		{total: 3, peersCount: 1, peerVoteAccumulated: 0, voteFailed: 1, expected: true},
		{total: 3, peersCount: 2, peerVoteAccumulated: 1, voteFailed: 0, expected: false},
	}

	for _, test := range tests {
		result := calculateIfAlreadyFail(test.total, test.peersCount, test.peerVoteAccumulated, test.voteFailed)
		if result != test.expected {
			t.Errorf("calculateIfAlreadyFail(%d, %d, %d, %d) = %v; want %v", test.total, test.peersCount, test.peerVoteAccumulated, test.voteFailed, result, test.expected)
		}
	}
}
