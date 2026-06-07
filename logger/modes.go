package logger

type Mode int

const (
	Development Mode = iota
	Production
)

func (mode Mode) String() string {
	switch mode {
	case Production:
		return "Production"
	default:
		return "Development"
	}
}
