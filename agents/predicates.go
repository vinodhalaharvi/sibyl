package agents

import (
	"context"

	"github.com/vinodhalaharvi/sibyl/channels"
)

// SeverityCarrier is anything that has a severity. Domain types implement
// this so SeverityPredicate can decide based on the output's severity field
// without knowing the domain's concrete shape.
type SeverityCarrier interface {
	GetSeverity() channels.Severity
}

// SeverityAtLeast returns a predicate that requires HITL only when the
// output's severity meets or exceeds the threshold.
//
// Example:
//
//	policy := agents.WithPredicate(
//	    agents.AlwaysAskPolicy[Finding]{ RenderFn: renderFinding },
//	    agents.SeverityAtLeast[Finding](channels.SeverityHigh),
//	)
//
// Severity ordering: INFO < LOW < MEDIUM < HIGH < CRITICAL.
func SeverityAtLeast[Resp SeverityCarrier](minimum channels.Severity) PolicyPredicate[Resp] {
	return func(_ context.Context, output Resp) bool {
		return severityRank(output.GetSeverity()) >= severityRank(minimum)
	}
}

// SeverityIn returns a predicate that requires HITL only when the output's
// severity is in the given set. Useful for "ask on CRITICAL and HIGH but
// skip MEDIUM/LOW/INFO" patterns that aren't a simple threshold.
func SeverityIn[Resp SeverityCarrier](severities ...channels.Severity) PolicyPredicate[Resp] {
	set := make(map[channels.Severity]struct{}, len(severities))
	for _, s := range severities {
		set[s] = struct{}{}
	}
	return func(_ context.Context, output Resp) bool {
		_, ok := set[output.GetSeverity()]
		return ok
	}
}

// ConfidenceCarrier is anything that exposes an agent self-reported
// confidence score in [0, 1]. A lower score means the agent is less certain.
type ConfidenceCarrier interface {
	GetConfidence() float64
}

// ConfidenceBelow returns a predicate that requires HITL when the agent's
// own confidence is below the threshold.
//
// Combine with severity for "ask if either critical OR the agent was unsure":
//
//	predicate := agents.AnyOfPredicate(
//	    agents.SeverityAtLeast[Finding](channels.SeverityCritical),
//	    agents.ConfidenceBelow[Finding](0.7),
//	)
func ConfidenceBelow[Resp ConfidenceCarrier](threshold float64) PolicyPredicate[Resp] {
	return func(_ context.Context, output Resp) bool {
		return output.GetConfidence() < threshold
	}
}

// severityRank maps Severity to a numeric rank for ordering comparisons.
// Unknown severities rank below INFO so they never satisfy ">=" thresholds.
func severityRank(s channels.Severity) int {
	switch s {
	case channels.SeverityCritical:
		return 5
	case channels.SeverityHigh:
		return 4
	case channels.SeverityMedium:
		return 3
	case channels.SeverityLow:
		return 2
	case channels.SeverityInfo:
		return 1
	default:
		return 0
	}
}
