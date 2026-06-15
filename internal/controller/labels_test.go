package controller

import "testing"

func TestVolumeLabelID(t *testing.T) {
	got := VolumeLabelID("Games", "Windrose_State")
	if got != "games.windrose_state" {
		t.Fatalf("label id = %q", got)
	}
}

func TestRoleLabel(t *testing.T) {
	got := RoleLabel("games", "windrose-state")
	want := "simple-volume.shipstuff.io/games.windrose-state-role"
	if got != want {
		t.Fatalf("role label = %q, want %q", got, want)
	}
}

func TestCandidateLabel(t *testing.T) {
	got := CandidateLabel("games", "windrose-state")
	want := "simple-volume.shipstuff.io/games.windrose-state-candidate"
	if got != want {
		t.Fatalf("candidate label = %q, want %q", got, want)
	}
}
