package vcraft

type role int

const (
	Follower = iota
	Candidate
	Leader
)

var roleText = map[role]string{
	Follower:  "Follower",
	Candidate: "Candidate",
	Leader:    "Leader",
}

func RoleText(code role) string {
	return roleText[code]
}