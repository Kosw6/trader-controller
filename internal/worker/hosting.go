package worker

// ForHostingMode keeps collection coordination active on shared hosts while
// excluding every actuator/planner that could create independent worker nodes.
func ForHostingMode(definitions []Definition, shared bool) []Definition {
	if !shared {
		return definitions
	}
	result := make([]Definition, 0, len(definitions))
	for _, definition := range definitions {
		switch definition.Name {
		case "worker-scale-planner", "local-docker-actuator", "aws-asg-actuator":
			continue
		}
		result = append(result, definition)
	}
	return result
}
